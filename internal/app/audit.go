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

	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/config"
	"github.com/KFN002/perfect-go-service/db"
	auditpb "github.com/KFN002/perfect-go-service/gen/audit/v1"
	amqpv1 "github.com/KFN002/perfect-go-service/internal/controller/amqp/v1"
	auditgrpc "github.com/KFN002/perfect-go-service/internal/controller/grpc/auditv1"
	"github.com/KFN002/perfect-go-service/internal/controller/grpc/interceptors"
	httpv1 "github.com/KFN002/perfect-go-service/internal/controller/http/v1"
	"github.com/KFN002/perfect-go-service/internal/repo/auditstore"
	"github.com/KFN002/perfect-go-service/internal/repo/cache"
	"github.com/KFN002/perfect-go-service/internal/usecase/audit"
	"github.com/KFN002/perfect-go-service/pkg/bulkhead"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/logger"
	"github.com/KFN002/perfect-go-service/pkg/otel"
	"github.com/KFN002/perfect-go-service/pkg/postgres"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/ratelimit"
	"github.com/KFN002/perfect-go-service/pkg/redis"
)

// RunAudit boots the audit microservice and blocks until shutdown.
func RunAudit(cfg config.Audit) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log, err := logger.New(constants.ServiceAudit, cfg.Env, cfg.LogLevel)
	if err != nil {
		return err
	}
	defer log.Sync() //nolint:errcheck

	shutdownOTel, err := otel.Setup(ctx, otel.Config{
		ServiceName: constants.ServiceAudit,
		Endpoint:    cfg.OTelEndpoint,
		Enabled:     cfg.OTelEnabled,
		SampleRatio: 1,
	})
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer flush(shutdownOTel)

	// ---- own database, own redis: full isolation ---------------------------
	pool, err := postgres.New(ctx, postgres.Config{DSN: cfg.PGDSN, MaxConns: 16})
	if err != nil {
		return fmt.Errorf("audit postgres: %w", err)
	}
	defer pool.Close()
	if err := postgres.Migrate(pool, db.AuditMigrations, "audit/migrations"); err != nil {
		return fmt.Errorf("audit migrate: %w", err)
	}
	log.Info("audit migrations applied")

	rds, err := redis.New(ctx, redis.Config{Addr: cfg.RedisAddr, CacheTTL: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("audit redis: %w", err)
	}
	defer rds.Close()

	mq, err := rabbitmq.New(rabbitmq.Config{URL: cfg.RabbitURL}, log)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer mq.Close()
	if err := mq.DeclareFlow(amqpv1.AuditFlow); err != nil {
		return fmt.Errorf("declare audit flow: %w", err)
	}
	pub, err := mq.NewPublisher()
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer pub.Close()

	// ---- layers ------------------------------------------------------------
	store := auditstore.New(pool)
	dedup := cache.NewDeduper(rds, log)
	qcache := cache.NewQueryCache(rds, log)

	ingestor := audit.NewIngestor(audit.IngestConfig{
		BatchMaxSize: cfg.BatchMaxSize,
		BatchMaxWait: cfg.BatchMaxWait,
	}, store, dedup, log)
	querySvc := audit.NewQueryService(store, qcache, cfg.QueryTTL)

	server := auditgrpc.New(ingestor, querySvc, cfg.WriteBulk)
	ingestBulkhead := bulkhead.New("amqp-ingest", cfg.IngestBulk)

	limiter := ratelimit.New(ratelimit.Config{Rate: cfg.RateRPS, Burst: cfg.RateBurst})
	defer limiter.Close()

	app, err := httpv1.NewAuditApp(httpv1.AppConfig{
		ServiceName: constants.ServiceAudit,
		Log:         log,
		Limiter:     limiter,
		Readiness: []httpv1.ReadinessCheck{
			{Name: "postgres", Check: store.Ping},
			{Name: "redis", Check: rds.Ping},
			{Name: "rabbitmq", Check: func(context.Context) error { return mq.Ping() }},
		},
	}, server)
	if err != nil {
		return err
	}

	grpcSrv := grpc.NewServer(interceptors.Chain(log, limiter))
	auditpb.RegisterAuditServiceServer(grpcSrv, server)
	healthpb.RegisterHealthServer(grpcSrv, health.NewServer())

	registerAuditMetrics(ingestor, ingestBulkhead)

	// ---- lifecycles --------------------------------------------------------
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return amqpv1.RunAuditConsumer(gctx, mq, pub, ingestor, ingestBulkhead, cfg.Prefetch)
	})
	g.Go(func() error {
		return ingestor.RunPartitionMaintainer(gctx, cfg.PartitionsAhead, 6*time.Hour)
	})
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
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down gracefully: draining batcher")
		shCtx, cancel := context.WithTimeout(context.Background(), constants.DefaultShutdownTimeout)
		defer cancel()
		_ = app.ShutdownWithContext(shCtx)
		grpcSrv.GracefulStop()
		ingestor.Close() // final batch flush
		return nil
	})

	err = g.Wait()
	log.Info("audit stopped")
	return err
}
