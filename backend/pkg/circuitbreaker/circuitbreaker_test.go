package circuitbreaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

var errBoom = errors.New("boom")

func fail(context.Context) error { return errBoom }
func ok(context.Context) error   { return nil }

func TestTripsOpenAfterThreshold(t *testing.T) {
	t.Parallel()
	b := New("db", Config{FailureThreshold: 3, Cooldown: time.Hour}, nil)
	ctx := context.Background()
	for range 3 {
		_ = b.Do(ctx, fail)
	}
	if b.State() != Open {
		t.Fatalf("state = %v, want Open", b.State())
	}
	err := b.Do(ctx, ok)
	if apperrors.CodeOf(err) != apperrors.CodeUnavailable {
		t.Errorf("open breaker returned %v, want UNAVAILABLE fast-fail", err)
	}
}

func TestHalfOpenRecovery(t *testing.T) {
	t.Parallel()
	b := New("db", Config{FailureThreshold: 1, Cooldown: 10 * time.Millisecond, HalfOpenProbes: 2}, nil)
	ctx := context.Background()
	_ = b.Do(ctx, fail) // trip
	if b.State() != Open {
		t.Fatal("expected Open")
	}
	time.Sleep(15 * time.Millisecond) // cooldown elapses
	if b.State() != HalfOpen {
		t.Fatalf("state = %v, want HalfOpen", b.State())
	}
	// Two successful probes close it.
	if err := b.Do(ctx, ok); err != nil {
		t.Fatalf("probe 1: %v", err)
	}
	if err := b.Do(ctx, ok); err != nil {
		t.Fatalf("probe 2: %v", err)
	}
	if b.State() != Closed {
		t.Fatalf("state = %v, want Closed after probes", b.State())
	}
}

func TestHalfOpenFailureReopens(t *testing.T) {
	t.Parallel()
	b := New("db", Config{FailureThreshold: 1, Cooldown: 10 * time.Millisecond}, nil)
	ctx := context.Background()
	_ = b.Do(ctx, fail)
	time.Sleep(15 * time.Millisecond)
	_ = b.Do(ctx, fail) // probe fails
	if b.State() != Open {
		t.Fatalf("state = %v, want re-Open", b.State())
	}
}

func TestSuccessResetsFailureStreak(t *testing.T) {
	t.Parallel()
	b := New("db", Config{FailureThreshold: 3, Cooldown: time.Hour}, nil)
	ctx := context.Background()
	_ = b.Do(ctx, fail)
	_ = b.Do(ctx, fail)
	_ = b.Do(ctx, ok) // streak broken
	_ = b.Do(ctx, fail)
	_ = b.Do(ctx, fail)
	if b.State() != Closed {
		t.Fatalf("state = %v, want Closed (streak was reset)", b.State())
	}
}

func TestOnChangeFires(t *testing.T) {
	t.Parallel()
	transitions := make(chan [2]State, 8)
	b := New("db", Config{FailureThreshold: 1, Cooldown: time.Hour},
		func(_ string, from, to State) { transitions <- [2]State{from, to} })
	_ = b.Do(context.Background(), fail)
	select {
	case tr := <-transitions:
		if tr != [2]State{Closed, Open} {
			t.Errorf("transition = %v, want Closed→Open", tr)
		}
	default:
		t.Error("onChange not fired")
	}
}
