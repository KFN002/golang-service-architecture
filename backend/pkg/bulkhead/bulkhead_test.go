package bulkhead

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

func TestShedsBeyondCapacity(t *testing.T) {
	t.Parallel()
	b := New("ingest", 2)
	block := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			_ = b.Do(context.Background(), func(context.Context) error {
				<-block
				return nil
			})
		})
	}
	time.Sleep(20 * time.Millisecond) // both slots taken
	err := b.Do(context.Background(), func(context.Context) error { return nil })
	if apperrors.CodeOf(err) != apperrors.CodeOverloaded {
		t.Errorf("err = %v, want OVERLOADED", err)
	}
	inFlight, rejected := b.Stats()
	if inFlight != 2 || rejected != 1 {
		t.Errorf("stats = (%d,%d), want (2,1)", inFlight, rejected)
	}
	close(block)
	wg.Wait()
}

func TestDoWaitQueuesUntilContext(t *testing.T) {
	t.Parallel()
	b := New("ingest", 1)
	block := make(chan struct{})
	go func() {
		_ = b.DoWait(context.Background(), func(context.Context) error {
			<-block
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := b.DoWait(ctx, func(context.Context) error { return nil })
	if apperrors.CodeOf(err) != apperrors.CodeOverloaded {
		t.Errorf("err = %v, want OVERLOADED after wait timeout", err)
	}
	close(block)
}
