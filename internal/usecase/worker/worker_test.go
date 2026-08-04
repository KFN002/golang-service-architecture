package worker

import (
	"context"
	"testing"
	"time"

	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
)

func TestCompute(t *testing.T) {
	t.Parallel()
	c := NewComputer(Latencies{}, "w1")
	ctx := context.Background()

	cases := []struct {
		op   string
		a, b float64
		want float64
	}{
		{"+", 2, 3, 5},
		{"-", 2, 3, -1},
		{"*", 2, 3, 6},
		{"/", 7, 2, 3.5},
	}
	for _, tc := range cases {
		got, err := c.Compute(ctx, messages.TaskMessage{Op: tc.op, Arg1: tc.a, Arg2: tc.b})
		if err != nil || got != tc.want {
			t.Errorf("%v %s %v = (%v, %v), want %v", tc.a, tc.op, tc.b, got, err, tc.want)
		}
	}
}

func TestDivisionByZeroIsPermanent(t *testing.T) {
	t.Parallel()
	c := NewComputer(Latencies{}, "w1")
	_, err := c.Compute(context.Background(), messages.TaskMessage{Op: "/", Arg1: 1, Arg2: 0})
	if apperrors.CodeOf(err) != apperrors.CodeDivisionByZero {
		t.Fatalf("err = %v, want DIVISION_BY_ZERO", err)
	}
	if apperrors.IsRetryable(err) {
		t.Error("division by zero must not be retryable")
	}
}

func TestLatencyRespected(t *testing.T) {
	t.Parallel()
	c := NewComputer(Latencies{Add: 50 * time.Millisecond}, "w1")
	start := time.Now()
	_, err := c.Compute(context.Background(), messages.TaskMessage{Op: "+", Arg1: 1, Arg2: 1})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("returned in %v, want ≥ 50ms demo latency", elapsed)
	}
}

func TestCancelDuringLatency(t *testing.T) {
	t.Parallel()
	c := NewComputer(Latencies{Mul: time.Hour}, "w1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.Compute(ctx, messages.TaskMessage{Op: "*", Arg1: 1, Arg2: 1})
	if apperrors.CodeOf(err) != apperrors.CodeUnavailable {
		t.Fatalf("err = %v, want UNAVAILABLE (retryable cancel)", err)
	}
}
