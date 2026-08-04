// Package redis wraps rueidis — the fastest Go Redis client (auto-pipelining,
// server-assisted client-side caching) — behind the few operations this
// system needs. Keeping the surface narrow lets usecases depend on small
// interfaces they own (ISP/DIP) while the implementation stays swappable.
package redis

import (
	"context"
	"time"

	"github.com/redis/rueidis"
)

// Config for one Redis connection.
type Config struct {
	Addr     string
	Password string
	// CacheTTL enables server-assisted client-side caching for reads when > 0.
	CacheTTL time.Duration
}

// Client is the thin wrapper over rueidis.
type Client struct {
	r        rueidis.Client
	cacheTTL time.Duration
}

// New connects and pings.
func New(ctx context.Context, cfg Config) (*Client, error) {
	r, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{cfg.Addr},
		Password:    cfg.Password,
	})
	if err != nil {
		return nil, err
	}
	if err := r.Do(ctx, r.B().Ping().Build()).Error(); err != nil {
		r.Close()
		return nil, err
	}
	return &Client{r: r, cacheTTL: cfg.CacheTTL}, nil
}

// Close releases the connection.
func (c *Client) Close() { c.r.Close() }

// Ping verifies liveness (used by /readyz).
func (c *Client) Ping(ctx context.Context) error {
	return c.r.Do(ctx, c.r.B().Ping().Build()).Error()
}

// SetNX sets key→value only if absent; returns true when the key was set.
// This is the audit dedup primitive (idempotent at-least-once consumption).
func (c *Client) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	res := c.r.Do(ctx, c.r.B().Set().Key(key).Value(value).Nx().Ex(ttl).Build())
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil // NX condition failed — duplicate
		}
		return false, err
	}
	return true, nil
}

// GetCached reads key through rueidis client-side caching when enabled:
// repeated hot reads are served from process memory and invalidated by the
// server — the fastest possible cache-aside.
func (c *Client) GetCached(ctx context.Context, key string) (string, bool, error) {
	var res rueidis.RedisResult
	if c.cacheTTL > 0 {
		res = c.r.DoCache(ctx, c.r.B().Get().Key(key).Cache(), c.cacheTTL)
	} else {
		res = c.r.Do(ctx, c.r.B().Get().Key(key).Build())
	}
	s, err := res.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return s, true, nil
}

// Set writes key→value with a TTL.
func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.r.Do(ctx, c.r.B().Set().Key(key).Value(value).Ex(ttl).Build()).Error()
}

// Publish sends a message to a pub/sub channel (feeds SSE fan-out).
func (c *Client) Publish(ctx context.Context, channel, payload string) error {
	return c.r.Do(ctx, c.r.B().Publish().Channel(channel).Message(payload).Build()).Error()
}

// Subscribe consumes a pub/sub channel until ctx is done, invoking fn per
// message on a dedicated connection.
func (c *Client) Subscribe(ctx context.Context, channel string, fn func(payload string)) error {
	return c.r.Receive(ctx, c.r.B().Subscribe().Channel(channel).Build(), func(msg rueidis.PubSubMessage) {
		fn(msg.Message)
	})
}
