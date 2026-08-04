package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
)

// OutboxEntry is one claimed, unpublished outbox row.
type OutboxEntry struct {
	ID      int64
	Kind    string // "task" | "audit"
	Payload []byte
}

// Store is the scheduler's persistence port.
type Store interface {
	// RelayOutbox claims up to limit unpublished rows (SKIP LOCKED), invokes
	// publish for the batch, and marks only successfully published rows —
	// all inside one transaction.
	RelayOutbox(ctx context.Context, limit int, publish func(entries []OutboxEntry) []int64) (int, error)
	// ApplyStarted marks a task running (idempotent).
	ApplyStarted(ctx context.Context, taskID uuid.UUID, workerID string, attempt int) error
	// ApplyResult applies a computed result transactionally: completes the
	// task, propagates the value into dependents, enqueues newly-ready task
	// messages + audit rows to the outbox, finalizes the expression if the
	// root finished. Returns the dashboard events to emit. Idempotent:
	// duplicates return (nil, nil).
	ApplyResult(ctx context.Context, res messages.ResultMessage, audit []messages.AuditMessage) ([]messages.Event, error)
	// ApplyFailure fails the task and its expression permanently.
	ApplyFailure(ctx context.Context, res messages.ResultMessage, audit []messages.AuditMessage) ([]messages.Event, error)
	// PruneOutbox deletes old published rows; returns rows removed.
	PruneOutbox(ctx context.Context) (int64, error)
}

// Publisher is the broker port (implemented over pkg/rabbitmq with breaker).
type Publisher interface {
	PublishTask(ctx context.Context, body []byte, traceparent string) error
	PublishAudit(ctx context.Context, body []byte, traceparent string) error
}

// Notifier publishes dashboard events.
type Notifier interface {
	Notify(ctx context.Context, ev messages.Event)
}

// Clock abstracts time for tests.
type Clock interface{ Now() time.Time }

// SystemClock is the production clock.
type SystemClock struct{}

// Now returns the current UTC time.
func (SystemClock) Now() time.Time { return time.Now().UTC() }
