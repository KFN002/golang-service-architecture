package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
)

// QueryService is the read side: cached, collapsed, shed-before-writes.
type QueryService struct {
	store Store
	cache QueryCache
	sf    singleflight.Group
	ttl   time.Duration
}

// NewQueryService wires the read side.
func NewQueryService(store Store, cache QueryCache, ttl time.Duration) *QueryService {
	if ttl <= 0 {
		ttl = 3 * time.Second
	}
	return &QueryService{store: store, cache: cache, ttl: ttl}
}

// Query pages the audit log. Identical concurrent queries collapse into one
// database hit (singleflight); results ride the cache for a short TTL —
// dashboards polling in lockstep cost one query total.
func (q *QueryService) Query(ctx context.Context, f entity.AuditFilter) ([]entity.AuditEvent, error) {
	if f.Limit <= 0 || f.Limit > constants.MaxPageSize {
		f.Limit = 50
	}
	key := cacheKey(f)

	if data, ok := q.cache.Get(ctx, key); ok {
		var cached []entity.AuditEvent
		if err := jsonx.Unmarshal(data, &cached); err == nil {
			return cached, nil
		}
	}

	v, err, _ := q.sf.Do(key, func() (any, error) {
		events, err := q.store.Query(ctx, f)
		if err != nil {
			return nil, err
		}
		if data, err := jsonx.Marshal(events); err == nil {
			q.cache.Set(ctx, key, data, q.ttl)
		}
		return events, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]entity.AuditEvent), nil
}

// Stats returns aggregates (cached the same way).
func (q *QueryService) Stats(ctx context.Context) (entity.AuditStats, error) {
	const key = constants.AuditQueryCacheKey + "stats"

	if data, ok := q.cache.Get(ctx, key); ok {
		var cached entity.AuditStats
		if err := jsonx.Unmarshal(data, &cached); err == nil {
			return cached, nil
		}
	}
	v, err, _ := q.sf.Do(key, func() (any, error) {
		stats, err := q.store.Stats(ctx)
		if err != nil {
			return nil, err
		}
		if data, err := jsonx.Marshal(stats); err == nil {
			q.cache.Set(ctx, key, data, q.ttl)
		}
		return stats, nil
	})
	if err != nil {
		return entity.AuditStats{}, err
	}
	return v.(entity.AuditStats), nil
}

func cacheKey(f entity.AuditFilter) string {
	raw, _ := jsonx.Marshal(f)
	sum := sha256.Sum256(raw)
	return constants.AuditQueryCacheKey + hex.EncodeToString(sum[:16])
}
