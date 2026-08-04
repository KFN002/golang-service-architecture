// Package worker is the agent-side usecase: compute one operation with the
// configured demo latency and report the result.
package worker

import (
	"context"
	"time"

	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/constants"
)

// Latencies configures the artificial per-operation delay that makes the
// distributed pipeline visible on the dashboard. Zero values = native speed.
type Latencies struct {
	Add time.Duration
	Sub time.Duration
	Mul time.Duration
	Div time.Duration
}

// For returns the delay for an operation.
func (l Latencies) For(op string) time.Duration {
	switch op {
	case constants.OpAdd:
		return l.Add
	case constants.OpSub:
		return l.Sub
	case constants.OpMul:
		return l.Mul
	case constants.OpDiv:
		return l.Div
	default:
		return 0
	}
}

// Computer evaluates single operations.
type Computer struct {
	lat      Latencies
	workerID string
}

// NewComputer builds a computer identified as workerID in results.
func NewComputer(lat Latencies, workerID string) *Computer {
	return &Computer{lat: lat, workerID: workerID}
}

// WorkerID identifies this agent instance.
func (c *Computer) WorkerID() string { return c.workerID }

// Compute performs the operation, honoring the demo latency and ctx
// cancellation. Division by zero is a permanent, typed failure.
func (c *Computer) Compute(ctx context.Context, task messages.TaskMessage) (float64, error) {
	if delay := c.lat.For(task.Op); delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return 0, apperrors.Wrap(apperrors.CodeUnavailable, "compute canceled", ctx.Err())
		}
	}

	switch task.Op {
	case constants.OpAdd:
		return task.Arg1 + task.Arg2, nil
	case constants.OpSub:
		return task.Arg1 - task.Arg2, nil
	case constants.OpMul:
		return task.Arg1 * task.Arg2, nil
	case constants.OpDiv:
		if task.Arg2 == 0 {
			return 0, apperrors.New(apperrors.CodeDivisionByZero, "division by zero")
		}
		return task.Arg1 / task.Arg2, nil
	default:
		return 0, apperrors.Newf(apperrors.CodeInvalidInput, "unknown operation %q", task.Op)
	}
}
