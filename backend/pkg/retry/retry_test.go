package retry

import (
	"context"
	"testing"
	"time"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

func TestRetriesTransientUntilSuccess(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Do(context.Background(), Config{Attempts: 5, Base: time.Millisecond}, func(context.Context) error {
		calls++
		if calls < 3 {
			return apperrors.New(apperrors.CodeUnavailable, "transient")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil/3", err, calls)
	}
}

func TestPermanentErrorNotRetried(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Do(context.Background(), Config{Attempts: 5, Base: time.Millisecond}, func(context.Context) error {
		calls++
		return apperrors.New(apperrors.CodeInvalidInput, "permanent")
	})
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on permanent)", calls)
	}
	if apperrors.CodeOf(err) != apperrors.CodeInvalidInput {
		t.Errorf("err = %v", err)
	}
}

func TestExhaustionReturnsLastError(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Do(context.Background(), Config{Attempts: 3, Base: time.Millisecond}, func(context.Context) error {
		calls++
		return apperrors.New(apperrors.CodeUnavailable, "still down")
	})
	if calls != 3 || err == nil {
		t.Fatalf("calls=%d err=%v, want 3 attempts and final error", calls, err)
	}
}

func TestContextCancelStopsRetry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := Do(ctx, Config{Attempts: 10, Base: 50 * time.Millisecond}, func(context.Context) error {
		calls++
		return apperrors.New(apperrors.CodeUnavailable, "transient")
	})
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (canceled before backoff)", calls)
	}
	if err == nil {
		t.Error("want error after cancel")
	}
}

func TestBackoffCapped(t *testing.T) {
	t.Parallel()
	cfg := Config{Base: 100 * time.Millisecond, Cap: 300 * time.Millisecond}
	if d := Backoff(cfg, 0); d != 100*time.Millisecond {
		t.Errorf("attempt 0: %v", d)
	}
	if d := Backoff(cfg, 10); d != 300*time.Millisecond {
		t.Errorf("attempt 10: %v, want capped at 300ms", d)
	}
}
