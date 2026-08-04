// Package bulkhead isolates resource pools so one overloaded path cannot sink
// the whole service — the ship-compartment pattern. It is a counting semaphore
// with try-acquire semantics and saturation metrics.
package bulkhead

import (
	"context"
	"sync/atomic"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

// Bulkhead bounds concurrent executions of one isolated path.
type Bulkhead struct {
	name     string
	slots    chan struct{}
	inFlight atomic.Int64
	rejected atomic.Int64
}

// New creates a bulkhead with the given concurrency capacity.
func New(name string, capacity int) *Bulkhead {
	if capacity <= 0 {
		capacity = 1
	}
	return &Bulkhead{name: name, slots: make(chan struct{}, capacity)}
}

// Do runs fn if a slot is free, or fails fast with CodeOverloaded — the
// caller sheds load instead of queueing unboundedly.
func (b *Bulkhead) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	select {
	case b.slots <- struct{}{}:
		b.inFlight.Add(1)
		defer func() {
			<-b.slots
			b.inFlight.Add(-1)
		}()
		return fn(ctx)
	default:
		b.rejected.Add(1)
		return apperrors.Newf(apperrors.CodeOverloaded, "bulkhead %q saturated", b.name)
	}
}

// DoWait runs fn, waiting for a slot until ctx expires (for paths where
// queueing briefly beats shedding — e.g. internal consumers).
func (b *Bulkhead) DoWait(ctx context.Context, fn func(ctx context.Context) error) error {
	select {
	case b.slots <- struct{}{}:
		b.inFlight.Add(1)
		defer func() {
			<-b.slots
			b.inFlight.Add(-1)
		}()
		return fn(ctx)
	case <-ctx.Done():
		b.rejected.Add(1)
		return apperrors.Wrap(apperrors.CodeOverloaded, "bulkhead wait canceled", ctx.Err())
	}
}

// Stats reports live occupancy for metrics: in-flight count, then rejected.
func (b *Bulkhead) Stats() (int64, int64) {
	return b.inFlight.Load(), b.rejected.Load()
}
