// Package app wires dependencies and runs service lifecycles. Construction
// happens here and only here (dependency injection by hand — explicit,
// debuggable, no framework magic).
package app

import (
	"context"
	"fmt"
	"net"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/db"
	calcpb "github.com/KFN002/perfect-go-service/gen/calc/v1"
	amqpv1 "github.com/KFN002/perfect-go-service/internal/controller/amqp/v1"
	"github.com/KFN002/perfect-go-service/internal/controller/grpc/interceptors"
	grpcv1 "github.com/KFN002/perfect-go-service/internal/controller/grpc/v1"
	httpv1 "github.com/KFN002/perfect-go-service/internal/controller/http/v1"
	"github.com/KFN002/perfect-go-service/internal/repo/cache"
	"github.com/KFN002/perfect-go-service/internal/repo/persistent"
	"github.com/KFN002/perfect-go-service/internal/usecase/expression"
	"github.com/KFN002/perfect-go-service/internal/usecase/scheduler"
	"github.com/KFN002/perfect-go-service/pkg/circuitbreaker"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/logger"
	"github.com/KFN002/perfect-go-service/pkg/otel"
	"github.com/KFN002/perfect-go-service/pkg/postgres"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/ratelimit"
	"github.com/KFN002/perfect-go-service/pkg/redis"

	"go.uber.org/zap"
)

// RunOrchestrator boots the orchestrator and blocks until shutdown.
func RunOrchestrator(cfg config.Orchestrator) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log, err := logger.New(constants.ServiceOrchestrator, cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer log.Sync() //nolint:errcheck

	shutdownOTel, err := otel.Setup(ctx, otel.Config{
		ServiceName: constants.ServiceOrchestrator,
		Endpoint:    cfg.OTelEndpoint,
		Enabled:     cfg.OTelEnabled,
		SampleRatio: 1,
	})
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer flush(shutdownOTel)

	// ---- infrastructure ----------------------------------------------------
	pool, err := postgres.New(ctx, postgres.Config{DSN: cfg.PGDSN, MaxConns: 16})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	if err := postgres.Migrate(pool, db.MainMigrations, "main/migrations"); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	log.Info("migrations applied")

	rds, err := redis.New(ctx, redis.Config{Addr: cfg.RedisAddr})
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rds.Close()

	mq, err := rabbitmq.New(rabbitmq.Config{URL: cfg.RabbitURL}, log)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer mq.Close()
	for _, flow := range []rabbitmq.Flow{amqpv1.TasksFlow, amqpv1.ResultsFlow, amqpv1.AuditFlow} {
		if err := mq.DeclareFlow(flow); err != nil {
			return fmt.Errorf("declare %s: %w", flow.Queue, err)
		}
	}
	pub, err := mq.NewPublisher()
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()

	// ---- layers ------------------------------------------------------------
	repo := persistent.New(pool)
	notifier := cache.NewNotifier(rds, log)

	breaker := circuitbreaker.New("amqp-publish", circuitbreaker.Config{}, logBreaker(log))
	guarded := amqpv1.NewGuardedPublisher(pub, breaker)

	exprSvc := expression.NewService(repo, notifier, log)
	sched := scheduler.New(scheduler.Config{
		RelayInterval: cfg.RelayInterval,
		RelayBatch:    cfg.RelayBatch,
	}, repo, guarded, notifier, log)

	exprServer := grpcv1.New(exprSvc)
	limiter := ratelimit.New(ratelimit.Config{Rate: cfg.RateRPS, Burst: cfg.RateBurst})
	defer limiter.Close()

	hub := httpv1.NewSSEHub(log)

	app, err := httpv1.NewOrchestratorApp(httpv1.AppConfig{
		ServiceName: constants.ServiceOrchestrator,
		Log:         log,
		Limiter:     limiter,
		Readiness: []httpv1.ReadinessCheck{
			{Name: "postgres", Check: repo.Ping},
			{Name: "redis", Check: rds.Ping},
			{Name: "rabbitmq", Check: func(context.Context) error { return mq.Ping() }},
		},
	}, exprServer, hub, mq, pub)
	if err != nil {
		return err
	}

	grpcSrv := grpc.NewServer(interceptors.Chain(log, limiter))
	calcpb.RegisterExpressionServiceServer(grpcSrv, exprServer)
	healthpb.RegisterHealthServer(grpcSrv, health.NewServer())

	registerOrchestratorMetrics(hub)

	// ---- lifecycles --------------------------------------------------------
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error { return sched.RunOutboxRelay(gctx) })
	g.Go(func() error { return amqpv1.RunResultsConsumer(gctx, mq, pub, sched, cfg.Prefetch) })
	g.Go(func() error { return hub.RunRedisBridge(gctx, rds.Subscribe, constants.EventsChannel) })

	g.Go(func() error {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
		if err != nil {
			return err
		}
		log.Info("gRPC listening", zap.Int("port", cfg.GRPCPort))
		return grpcSrv.Serve(lis)
	})
	g.Go(func() error {
		log.Info("HTTP listening", zap.Int("port", cfg.HTTPPort))
		return app.Listen(fmt.Sprintf(":%d", cfg.HTTPPort))
	})

	// Shutdown choreography: stop intake first, then drain.
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down gracefully")
		shCtx, cancel := context.WithTimeout(context.Background(), constants.DefaultShutdownTimeout)
		defer cancel()
		_ = app.ShutdownWithContext(shCtx)
		grpcSrv.GracefulStop()
		return nil
	})

	err = g.Wait()
	log.Info("orchestrator stopped")
	return err
}

func flush(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdown(ctx)
}

func logBreaker(log *zap.Logger) circuitbreaker.OnChange {
	return func(name string, from, to circuitbreaker.State) {
		log.Warn("circuit breaker transition",
			zap.String("breaker", name),
			zap.String("from", from.String()),
			zap.String("to", to.String()))
	}
}
