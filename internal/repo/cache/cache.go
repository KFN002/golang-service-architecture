// Package cache adapts Redis (rueidis) to the notifier, dedup and query-cache
// ports owned by the usecases.
package cache

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
	"github.com/KFN002/perfect-go-service/pkg/redis"
)

// Notifier publishes dashboard events on Redis pub/sub. Fire-and-forget by
// design: UI events must never fail a business transaction.
type Notifier struct {
	r   *redis.Client
	log *zap.Logger
}

// NewNotifier builds the notifier.
func NewNotifier(r *redis.Client, log *zap.Logger) *Notifier {
	return &Notifier{r: r, log: log}
}

// Notify publishes one event; failures are logged, never propagated.
func (n *Notifier) Notify(ctx context.Context, ev messages.Event) {
	payload, err := jsonx.Marshal(ev)
	if err != nil {
		n.log.Error("event marshal failed", zap.Error(err))
		return
	}
	if err := n.r.Publish(ctx, constants.EventsChannel, string(payload)); err != nil {
		n.log.Warn("event publish failed", zap.Error(err))
	}
}

// Deduper implements audit.Deduper over Redis.
type Deduper struct {
	r   *redis.Client
	log *zap.Logger
}

// NewDeduper builds the dedup adapter.
func NewDeduper(r *redis.Client, log *zap.Logger) *Deduper {
	return &Deduper{r: r, log: log}
}

// Seen reports whether the id was recorded after a durable flush.
func (d *Deduper) Seen(ctx context.Context, id string) (bool, error) {
	_, found, err := d.r.GetCached(ctx, constants.AuditDedupKey+id)
	return found, err
}

// MarkSeen records the id (best effort; PG ON CONFLICT is the backstop).
func (d *Deduper) MarkSeen(ctx context.Context, id string, ttl time.Duration) {
	if err := d.r.Set(ctx, constants.AuditDedupKey+id, "1", ttl); err != nil {
		d.log.Warn("dedup mark failed", zap.String("id", id), zap.Error(err))
	}
}

// QueryCache implements audit.QueryCache over Redis.
type QueryCache struct {
	r   *redis.Client
	log *zap.Logger
}

// NewQueryCache builds the query cache adapter.
func NewQueryCache(r *redis.Client, log *zap.Logger) *QueryCache {
	return &QueryCache{r: r, log: log}
}

// Get returns a cached payload.
func (c *QueryCache) Get(ctx context.Context, key string) ([]byte, bool) {
	val, found, err := c.r.GetCached(ctx, key)
	if err != nil || !found {
		return nil, false
	}
	return []byte(val), true
}

// Set stores a payload with TTL (best effort).
func (c *QueryCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) {
	if err := c.r.Set(ctx, key, string(val), ttl); err != nil {
		c.log.Warn("query cache set failed", zap.Error(err))
	}
}
