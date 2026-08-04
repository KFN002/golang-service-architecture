package app

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/KFN002/perfect-go-service/config"
	amqpv1 "github.com/KFN002/perfect-go-service/internal/controller/amqp/v1"
	"github.com/KFN002/perfect-go-service/internal/usecase/worker"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/logger"
	"github.com/KFN002/perfect-go-service/pkg/otel"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/workerpool"
)

// RunAgent boots one agent replica and blocks until shutdown.
func RunAgent(cfg config.Agent) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log, err := logger.New(constants.ServiceAgent, cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer log.Sync() //nolint:errcheck

	shutdownOTel, err := otel.Setup(ctx, otel.Config{
		ServiceName: constants.ServiceAgent,
		Endpoint:    cfg.OTelEndpoint,
		Enabled:     cfg.OTelEnabled,
		SampleRatio: 1,
	})
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer flush(shutdownOTel)

	mq, err := rabbitmq.New(rabbitmq.Config{URL: cfg.RabbitURL}, log)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer mq.Close()
	if err := declareFlows(mq, amqpv1.TasksFlow, amqpv1.ResultsFlow, amqpv1.AuditFlow); err != nil {
		return err
	}
	pub, err := mq.NewPublisher()
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()

	computer := worker.NewComputer(cfg.Latencies, cfg.InstanceID)

	// The auto-scaling pool: every resize is audited and visible live.
	onScale := amqpv1.PublishPoolScaled(pub, cfg.InstanceID, log)
	pool := workerpool.New(workerpool.Config{
		Min:         cfg.PoolMin,
		Max:         cfg.PoolMax,
		QueueSize:   cfg.Prefetch,
		IdleTimeout: cfg.PoolIdle,
	}, func(from, to int32, reason string) {
		log.Info("pool scaled", zap.Int32("from", from), zap.Int32("to", to), zap.String("reason", reason))
		onScale(from, to, reason)
	})

	consumer := amqpv1.NewAgentConsumer(computer, pool, pub, log)
	registerAgentMetrics(cfg.InstanceID, pool)

	// Minimal HTTP surface: liveness + metrics only.
	app := fiber.New(fiber.Config{AppName: constants.ServiceAgent})
	app.Use(recover.New())
	app.Get("/healthz", func(c fiber.Ctx) error { return c.JSON(fiber.Map{"status": "ok"}) })
	app.Get("/readyz", func(c fiber.Ctx) error {
		if err := mq.Ping(); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"status": "degraded"})
		}
		return c.JSON(fiber.Map{"status": "ready"})
	})
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(runPprof(gctx, cfg.PprofEnabled, log))
	g.Go(func() error { return consumer.Run(gctx, mq, cfg.Prefetch) })
	g.Go(func() error {
		log.Info("HTTP listening", zap.Int("port", cfg.HTTPPort))
		return app.Listen(fmt.Sprintf(":%d", cfg.HTTPPort))
	})
	g.Go(func() error {
		<-gctx.Done()
		log.Info("draining worker pool")
		pool.Close() // waits for queued tasks to finish
		_ = app.Shutdown()
		return nil
	})

	err = g.Wait()
	log.Info("agent stopped")
	return err
}
