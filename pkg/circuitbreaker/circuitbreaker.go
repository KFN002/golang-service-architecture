// Package circuitbreaker implements the classic three-state circuit breaker.
//
//	Closed    — calls flow; failures are counted in a sliding window.
//	Open      — calls fail fast with CodeUnavailable; after Cooldown the
//	            breaker admits probes.
//	Half-open — a bounded number of probe calls decide: success closes the
//	            breaker, failure re-opens it.
//
// State and counters are atomics; the mutex guards only state transitions.
package circuitbreaker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

// State of the breaker.
type State int32

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Config tunes the breaker.
type Config struct {
	FailureThreshold int           // consecutive failures that trip Closed → Open
	Cooldown         time.Duration // Open → HalfOpen delay
	HalfOpenProbes   int           // successes needed to close from HalfOpen
}

func (c *Config) defaults() {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 5
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Second
	}
	if c.HalfOpenProbes <= 0 {
		c.HalfOpenProbes = 2
	}
}

// OnChange observes transitions (for metrics/audit).
type OnChange func(name string, from, to State)

// Breaker guards one downstream dependency.
type Breaker struct {
	name string
	cfg  Config

	state     atomic.Int32
	failures  atomic.Int32
	successes atomic.Int32 // half-open probe successes
	probes    atomic.Int32 // in-flight half-open probes
	openedAt  atomic.Int64 // unix nanos

	mu       sync.Mutex
	onChange OnChange
}

// New creates a breaker in Closed state.
func New(name string, cfg Config, onChange OnChange) *Breaker {
	cfg.defaults()
	return &Breaker{name: name, cfg: cfg, onChange: onChange}
}

// State returns the current state, promoting Open → HalfOpen when cooled down.
func (b *Breaker) State() State {
	s := State(b.state.Load())
	if s == Open && time.Since(time.Unix(0, b.openedAt.Load())) >= b.cfg.Cooldown {
		b.transition(Open, HalfOpen)
		return HalfOpen
	}
	return s
}

// Do executes fn under the breaker's policy.
func (b *Breaker) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	switch b.State() {
	case Open:
		return apperrors.Newf(apperrors.CodeUnavailable, "circuit %q open", b.name)
	case HalfOpen:
		// Admit only a bounded number of concurrent probes.
		if b.probes.Add(1) > int32(b.cfg.HalfOpenProbes) {
			b.probes.Add(-1)
			return apperrors.Newf(apperrors.CodeUnavailable, "circuit %q probing", b.name)
		}
		defer b.probes.Add(-1)
	}

	err := fn(ctx)
	b.record(err)
	return err
}

func (b *Breaker) record(err error) {
	if err == nil {
		switch State(b.state.Load()) {
		case HalfOpen:
			if b.successes.Add(1) >= int32(b.cfg.HalfOpenProbes) {
				b.transition(HalfOpen, Closed)
			}
		default:
			b.failures.Store(0)
		}
		return
	}
	switch State(b.state.Load()) {
	case HalfOpen:
		b.transition(HalfOpen, Open)
	case Closed:
		if b.failures.Add(1) >= int32(b.cfg.FailureThreshold) {
			b.transition(Closed, Open)
		}
	}
}

// transition performs from → to atomically; losers of the race no-op.
func (b *Breaker) transition(from, to State) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.state.CompareAndSwap(int32(from), int32(to)) {
		return
	}
	switch to {
	case Open:
		b.openedAt.Store(time.Now().UnixNano())
	case HalfOpen:
		b.successes.Store(0)
		b.probes.Store(0)
	case Closed:
		b.failures.Store(0)
	}
	if b.onChange != nil {
		b.onChange(b.name, from, to)
	}
}
