//go:build integration

// Package integration hosts container-backed tests: real PostgreSQL 18, real
// Redis 8, real RabbitMQ 4 — the same images the compose stack runs.
package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/db"
	"github.com/KFN002/perfect-go-service/pkg/postgres"
	"github.com/KFN002/perfect-go-service/pkg/rabbitmq"
	"github.com/KFN002/perfect-go-service/pkg/redis"
)

func startPostgres(t *testing.T, ctx context.Context, dbName string) *pgxpool.Pool {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:18-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       dbName,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432")
	dsn := fmt.Sprintf("postgres://test:test@%s/%s?sslmode=disable", net.JoinHostPort(host, port.Port()), dbName)

	pool, err := postgres.New(ctx, postgres.Config{DSN: dsn})
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func startRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "redis:8-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379")
	client, err := redis.New(ctx, redis.Config{Addr: net.JoinHostPort(host, port.Port())})
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func startRabbit(t *testing.T, ctx context.Context) *rabbitmq.Client {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4-alpine",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5672")
	client, err := rabbitmq.New(rabbitmq.Config{
		URL: fmt.Sprintf("amqp://guest:guest@%s/", net.JoinHostPort(host, port.Port())),
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("connect rabbitmq: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func migrateMain(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if err := postgres.Migrate(pool, db.MainMigrations, "main/migrations"); err != nil {
		t.Fatalf("migrate main: %v", err)
	}
}

func migrateAudit(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if err := postgres.Migrate(pool, db.AuditMigrations, "audit/migrations"); err != nil {
		t.Fatalf("migrate audit: %v", err)
	}
}
