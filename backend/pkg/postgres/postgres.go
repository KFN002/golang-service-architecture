// Package postgres owns pgx pool construction and goose migrations.
package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Config for one PostgreSQL pool.
type Config struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	ConnectTimeout  time.Duration
	HealthCheckTime time.Duration
}

// New builds a pgx connection pool and verifies connectivity.
func New(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		pc.MinConns = cfg.MinConns
	}
	if cfg.ConnectTimeout > 0 {
		pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	if cfg.HealthCheckTime > 0 {
		pc.HealthCheckPeriod = cfg.HealthCheckTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// Migrate applies goose migrations from the embedded filesystem.
// Each service migrates its own database on startup; goose's advisory lock
// makes concurrent replicas safe.
func Migrate(pool *pgxpool.Pool, migrations fs.FS, dir string) error {
	goose.SetBaseFS(migrations)
	defer goose.SetBaseFS(nil)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close() //nolint:errcheck // closing the adapter, not the pool
	return goose.Up(db, dir)
}
