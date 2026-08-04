package audit

import (
	"context"
	"time"

	"github.com/KFN002/perfect-go-service/internal/entity"
)

// Store is the audit persistence port (implemented by repo/auditstore).
type Store interface {
	// InsertBatch persists events idempotently (duplicates are skipped) and
	// returns the number of rows actually inserted.
	InsertBatch(ctx context.Context, events []entity.AuditEvent) (int64, error)
	Query(ctx context.Context, f entity.AuditFilter) ([]entity.AuditEvent, error)
	Stats(ctx context.Context) (entity.AuditStats, error)
	// EnsurePartitions pre-creates daily partitions [today .. today+ahead].
	EnsurePartitions(ctx context.Context, ahead int) error
}

// Deduper is the fast-path duplicate filter (Redis SETNX semantics).
type Deduper interface {
	// Seen reports whether id was already recorded; MarkSeen records it.
	Seen(ctx context.Context, id string) (bool, error)
	MarkSeen(ctx context.Context, id string, ttl time.Duration)
}

// QueryCache caches hot query results (rueidis client-side caching under it).
type QueryCache interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration)
}
