// Package ratelimit implements a sharded token-bucket rate limiter keyed by
// arbitrary strings (client IP, API key, method).
//
// Buckets refill lazily on access — no background goroutines — and idle
// buckets are evicted by a periodic sweep to bound memory under key churn
// (a DoS vector if unbounded). Shards keep lock contention negligible.
package ratelimit

import (
	"hash/fnv"
	"sync"
	"time"
)

const shardCount = 32

// Config tunes bucket behavior.
type Config struct {
	Rate  float64       // tokens added per second
	Burst float64       // bucket capacity
	TTL   time.Duration // evict buckets untouched for this long
}

func (c *Config) defaults() {
	if c.Rate <= 0 {
		c.Rate = 10
	}
	if c.Burst <= 0 {
		c.Burst = c.Rate
	}
	if c.TTL <= 0 {
		c.TTL = 10 * time.Minute
	}
}

type bucket struct {
	tokens float64
	last   time.Time
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

// Limiter is a keyed token-bucket limiter.
type Limiter struct {
	cfg    Config
	shards [shardCount]*shard
	stop   chan struct{}
	once   sync.Once
}

// New creates a limiter and starts its eviction sweep.
func New(cfg Config) *Limiter {
	cfg.defaults()
	l := &Limiter{cfg: cfg, stop: make(chan struct{})}
	for i := range l.shards {
		l.shards[i] = &shard{buckets: make(map[string]*bucket)}
	}
	go l.sweep()
	return l
}

// Allow consumes one token for key if available.
func (l *Limiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN consumes n tokens for key if available.
func (l *Limiter) AllowN(key string, n float64) bool {
	s := l.shardFor(key)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{tokens: l.cfg.Burst, last: now}
		s.buckets[key] = b
	}
	// Lazy refill.
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * l.cfg.Rate
	if b.tokens > l.cfg.Burst {
		b.tokens = l.cfg.Burst
	}
	b.last = now

	if b.tokens >= n {
		b.tokens -= n
		return true
	}
	return false
}

// Close stops the eviction sweep.
func (l *Limiter) Close() { l.once.Do(func() { close(l.stop) }) }

func (l *Limiter) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return l.shards[h.Sum32()%shardCount]
}

func (l *Limiter) sweep() {
	ticker := time.NewTicker(l.cfg.TTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-l.cfg.TTL)
			for _, s := range l.shards {
				s.mu.Lock()
				for k, b := range s.buckets {
					if b.last.Before(cutoff) {
						delete(s.buckets, k)
					}
				}
				s.mu.Unlock()
			}
		}
	}
}
