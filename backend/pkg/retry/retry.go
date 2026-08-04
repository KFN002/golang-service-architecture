// Package retry implements context-aware retries with exponential backoff and
// full jitter (AWS-style), retrying only errors classified as transient.
package retry

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

// Config tunes the backoff schedule.
type Config struct {
	Attempts int           // total tries including the first
	Base     time.Duration // first backoff ceiling
	Cap      time.Duration // max backoff ceiling
}

func (c *Config) defaults() {
	if c.Attempts <= 0 {
		c.Attempts = 3
	}
	if c.Base <= 0 {
		c.Base = 100 * time.Millisecond
	}
	if c.Cap <= 0 {
		c.Cap = 5 * time.Second
	}
}

// Do runs fn until success, permanent failure, exhausted attempts, or ctx end.
//
// Backoff for attempt n is rand(0, min(Cap, Base·2ⁿ)) — full jitter prevents
// synchronized retry storms across replicas (thundering herd).
func Do(ctx context.Context, cfg Config, fn func(ctx context.Context) error) error {
	cfg.defaults()
	var err error
	for attempt := range cfg.Attempts {
		if err = fn(ctx); err == nil {
			return nil
		}
		if !apperrors.IsRetryable(err) {
			return err // permanent — do not hammer
		}
		if attempt == cfg.Attempts-1 {
			break
		}
		ceiling := min(cfg.Base<<attempt, cfg.Cap)
		// #nosec G404 -- jitter needs speed, not cryptographic randomness.
		delay := time.Duration(rand.Int64N(int64(ceiling) + 1))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return apperrors.Wrap(apperrors.CodeUnavailable, "retry canceled", ctx.Err())
		}
	}
	return err
}

// Backoff exposes the schedule for callers that need the delay value itself
// (e.g. AMQP retry-queue TTLs).
func Backoff(cfg Config, attempt int) time.Duration {
	cfg.defaults()
	ceiling := cfg.Base << attempt
	if ceiling > cfg.Cap || ceiling <= 0 {
		ceiling = cfg.Cap
	}
	return ceiling
}
