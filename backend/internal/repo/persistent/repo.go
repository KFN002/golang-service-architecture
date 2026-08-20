// Package persistent adapts the main PostgreSQL database (via sqlc-generated
// queries) to the persistence ports owned by the usecases.
package persistent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/repo/persistent/sqlcgen"
	"github.com/KFN002/perfect-go-service/internal/usecase/expression"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/internal/usecase/scheduler"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
	"github.com/KFN002/perfect-go-service/pkg/otel"
)

const eventKindTaskUpdated = "task.updated"

// Repo implements expression.Store and scheduler.Store over pgx.
type Repo struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

// New builds the adapter.
func New(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool, q: sqlcgen.New(pool)}
}

// Compile-time port checks.
var (
	_ expression.Store = (*Repo)(nil)
	_ scheduler.Store  = (*Repo)(nil)
)

// ---- expression.Store ------------------------------------------------------

// CreateExpression persists expression + DAG + outbox rows in one transaction.
func (r *Repo) CreateExpression(ctx context.Context, expr *entity.Expression, tasks []*entity.Task, audit []messages.AuditMessage) error {
	return r.inTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.InsertExpression(ctx, sqlcgen.InsertExpressionParams{
			ID: expr.ID, Raw: expr.Raw, TraceID: expr.TraceID,
		}); err != nil {
			return fmt.Errorf("insert expression: %w", err)
		}
		for _, t := range tasks {
			if err := q.InsertTask(ctx, sqlcgen.InsertTaskParams{
				ID:           t.ID,
				ExpressionID: t.ExpressionID,
				Op:           t.Op,
				Arg1Value:    t.Arg1Value,
				Arg1TaskID:   t.Arg1TaskID,
				Arg2Value:    t.Arg2Value,
				Arg2TaskID:   t.Arg2TaskID,
				UnmetDeps:    int32(min(t.UnmetDeps, 2)), // #nosec G115 -- binary ops have ≤2 deps
				Status:       sqlcgen.TaskStatusPending,
				IsRoot:       t.IsRoot,
			}); err != nil {
				return fmt.Errorf("insert task: %w", err)
			}
		}
		// Initially-ready tasks go straight to the outbox.
		for _, t := range tasks {
			if t.Ready() {
				if err := enqueueTask(ctx, q, t); err != nil {
					return err
				}
				if err := q.MarkTaskReady(ctx, t.ID); err != nil {
					return err
				}
			}
		}
		return enqueueAudit(ctx, q, audit)
	})
}

// FinalizeImmediate stores a taskless expression as done.
func (r *Repo) FinalizeImmediate(ctx context.Context, expr *entity.Expression, result float64, audit []messages.AuditMessage) error {
	return r.inTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.InsertExpression(ctx, sqlcgen.InsertExpressionParams{
			ID: expr.ID, Raw: expr.Raw, TraceID: expr.TraceID,
		}); err != nil {
			return err
		}
		if err := q.FinalizeExpressionDone(ctx, sqlcgen.FinalizeExpressionDoneParams{
			ID: expr.ID, Result: &result,
		}); err != nil {
			return err
		}
		return enqueueAudit(ctx, q, audit)
	})
}

// GetExpression loads one expression.
func (r *Repo) GetExpression(ctx context.Context, id uuid.UUID) (*entity.Expression, error) {
	row, err := r.q.GetExpression(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperrors.ErrNotFound
		}
		return nil, err
	}
	e := toExpression(row)
	return &e, nil
}

// ListExpressions pages newest-first.
func (r *Repo) ListExpressions(ctx context.Context, limit, offset int) ([]*entity.Expression, int64, error) {
	rows, err := r.q.ListExpressions(ctx, sqlcgen.ListExpressionsParams{
		Limit: int32(min(limit, 1000)), Offset: int32(min(offset, 1<<30)), // #nosec G115 -- min-clamped
	})
	if err != nil {
		return nil, 0, err
	}
	total, err := r.q.CountExpressions(ctx)
	if err != nil {
		return nil, 0, err
	}
	out := make([]*entity.Expression, len(rows))
	for i, row := range rows {
		e := toExpression(row)
		out[i] = &e
	}
	return out, total, nil
}

// GetTasks returns an expression's DAG.
func (r *Repo) GetTasks(ctx context.Context, exprID uuid.UUID) ([]*entity.Task, error) {
	rows, err := r.q.GetTasksByExpression(ctx, exprID)
	if err != nil {
		return nil, err
	}
	out := make([]*entity.Task, len(rows))
	for i, row := range rows {
		t := toTask(row)
		out[i] = &t
	}
	return out, nil
}

// GetProgress counts DAG completion.
func (r *Repo) GetProgress(ctx context.Context, exprID uuid.UUID) (entity.Progress, error) {
	row, err := r.q.ExpressionProgress(ctx, exprID)
	if err != nil {
		return entity.Progress{}, err
	}
	return entity.Progress{Total: int(row.Total), Done: int(row.Done)}, nil
}

// ---- scheduler.Store -------------------------------------------------------

// RelayOutbox claims, publishes and marks outbox rows in one transaction.
func (r *Repo) RelayOutbox(ctx context.Context, limit int, publish func([]scheduler.OutboxEntry) []int64) (int, error) {
	var relayed int
	err := r.inTx(ctx, func(q *sqlcgen.Queries) error {
		rows, err := q.SelectOutboxBatch(ctx, int32(min(limit, 4096))) //nolint:gosec // min-clamped
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		entries := make([]scheduler.OutboxEntry, len(rows))
		for i, row := range rows {
			entries[i] = scheduler.OutboxEntry{ID: row.ID, Kind: row.Kind, Payload: row.Payload}
		}
		published := publish(entries)
		relayed = len(entries)
		if len(published) == 0 {
			return nil
		}
		return q.MarkOutboxPublished(ctx, published)
	})
	return relayed, err
}

// ApplyStarted marks a task running.
func (r *Repo) ApplyStarted(ctx context.Context, taskID uuid.UUID, workerID string, attempt int) error {
	return r.q.MarkTaskRunning(ctx, sqlcgen.MarkTaskRunningParams{
		ID: taskID, WorkerID: workerID,
		Attempt: int32(min(attempt, 1<<20)), //nolint:gosec // min-clamped
	})
}

// ApplyResult is the fan-in transaction (see scheduler.Store contract).
func (r *Repo) ApplyResult(ctx context.Context, res messages.ResultMessage, audit []messages.AuditMessage) ([]messages.Event, error) {
	var events []messages.Event
	err := r.inTx(ctx, func(q *sqlcgen.Queries) error {
		done, err := q.CompleteTask(ctx, sqlcgen.CompleteTaskParams{ID: res.TaskID, Result: &res.Result})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // duplicate result — idempotent no-op
			}
			return err
		}

		if err := q.MarkExpressionInProgress(ctx, res.ExpressionID); err != nil {
			return err
		}

		now := nowUTC()
		events = append(events, messages.Event{
			Kind: eventKindTaskUpdated, ExpressionID: res.ExpressionID, TaskID: &res.TaskID,
			Status: string(entity.TaskDone), Result: &res.Result, WorkerID: res.WorkerID, At: now,
		})

		// Fan-in: propagate the value into both argument slots of dependents.
		dep1, err := q.FillArg1FromResult(ctx, sqlcgen.FillArg1FromResultParams{
			Arg1TaskID: &res.TaskID, Arg1Value: &res.Result,
		})
		if err != nil {
			return err
		}
		dep2, err := q.FillArg2FromResult(ctx, sqlcgen.FillArg2FromResultParams{
			Arg2TaskID: &res.TaskID, Arg2Value: &res.Result,
		})
		if err != nil {
			return err
		}

		// Newly-ready dependents → outbox + ready status.
		readyEvents, err := unlockReadyDependents(ctx, q, res.ExpressionID,
			append(depRows(dep1), depRows(dep2)...), now)
		if err != nil {
			return err
		}
		events = append(events, readyEvents...)

		// Root finished → the expression is done.
		if done.IsRoot {
			if err := q.FinalizeExpressionDone(ctx, sqlcgen.FinalizeExpressionDoneParams{
				ID: res.ExpressionID, Result: &res.Result,
			}); err != nil {
				return err
			}
			events = append(events, messages.Event{
				Kind: "expression.updated", ExpressionID: res.ExpressionID,
				Status: string(entity.ExpressionDone), Result: &res.Result, At: now,
			})
			doneAudit := messages.NewAudit(entity.AuditExpressionDone, "orchestrator",
				"expression", res.ExpressionID.String(), "", map[string]any{"result": res.Result})
			audit = append(audit, doneAudit)
		}
		return enqueueAudit(ctx, q, audit)
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// ApplyFailure fails the task and finalizes the expression as failed.
func (r *Repo) ApplyFailure(ctx context.Context, res messages.ResultMessage, audit []messages.AuditMessage) ([]messages.Event, error) {
	var events []messages.Event
	err := r.inTx(ctx, func(q *sqlcgen.Queries) error {
		if _, err := q.FailTask(ctx, res.TaskID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // already settled
			}
			return err
		}
		if err := q.FinalizeExpressionFailed(ctx, sqlcgen.FinalizeExpressionFailedParams{
			ID: res.ExpressionID, Error: &res.Error,
		}); err != nil {
			return err
		}
		now := nowUTC()
		events = append(events,
			messages.Event{
				Kind: eventKindTaskUpdated, ExpressionID: res.ExpressionID, TaskID: &res.TaskID,
				Status: string(entity.TaskFailed), Error: res.Error, WorkerID: res.WorkerID, At: now,
			},
			messages.Event{
				Kind: "expression.updated", ExpressionID: res.ExpressionID,
				Status: string(entity.ExpressionFailed), Error: res.Error, At: now,
			})
		return enqueueAudit(ctx, q, audit)
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// PruneOutbox removes old published rows.
func (r *Repo) PruneOutbox(ctx context.Context) (int64, error) {
	return r.q.PruneOutbox(ctx)
}

// Ping verifies the pool (readiness checks).
func (r *Repo) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

// ---- helpers ---------------------------------------------------------------

func (r *Repo) inTx(ctx context.Context, fn func(q *sqlcgen.Queries) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	if err := fn(r.q.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// enqueueTask writes a task message to the outbox.
func enqueueTask(ctx context.Context, q *sqlcgen.Queries, t *entity.Task) error {
	if t.Arg1Value == nil || t.Arg2Value == nil {
		return fmt.Errorf("task %s enqueued with unresolved args", t.ID)
	}
	payload, err := jsonx.Marshal(taskPayload{
		TaskMessage: messages.TaskMessage{
			TaskID: t.ID, ExpressionID: t.ExpressionID, Op: t.Op,
			Arg1: *t.Arg1Value, Arg2: *t.Arg2Value,
		},
		// Embedded so the relay can stamp the AMQP header and the agent's
		// span joins the submitting request's trace.
		Traceparent: otel.InjectTraceparent(ctx),
	})
	if err != nil {
		return err
	}
	return q.InsertOutbox(ctx, sqlcgen.InsertOutboxParams{Kind: "task", Payload: payload})
}

// taskPayload lets the relay recover trace context embedded at enqueue time.
type taskPayload struct {
	messages.TaskMessage
	Traceparent string `json:"traceparent,omitempty"`
}

// enqueueAudit writes audit messages to the outbox.
func enqueueAudit(ctx context.Context, q *sqlcgen.Queries, audit []messages.AuditMessage) error {
	for _, a := range audit {
		payload, err := jsonx.Marshal(a)
		if err != nil {
			return err
		}
		if err := q.InsertOutbox(ctx, sqlcgen.InsertOutboxParams{Kind: "audit", Payload: payload}); err != nil {
			return err
		}
	}
	return nil
}

type depRow struct {
	id         uuid.UUID
	op         string
	arg1, arg2 *float64
	unmet      int32
}

func depRows[T interface {
	sqlcgen.FillArg1FromResultRow | sqlcgen.FillArg2FromResultRow
}](rows []T) []depRow {
	out := make([]depRow, 0, len(rows))
	for _, r := range rows {
		switch v := any(r).(type) {
		case sqlcgen.FillArg1FromResultRow:
			out = append(out, depRow{v.ID, v.Op, v.Arg1Value, v.Arg2Value, v.UnmetDeps})
		case sqlcgen.FillArg2FromResultRow:
			out = append(out, depRow{v.ID, v.Op, v.Arg1Value, v.Arg2Value, v.UnmetDeps})
		}
	}
	return out
}

// unlockReadyDependents publishes tasks whose last dependency just resolved.
func unlockReadyDependents(ctx context.Context, q *sqlcgen.Queries,
	exprID uuid.UUID, deps []depRow, now time.Time) ([]messages.Event, error) {
	events := make([]messages.Event, 0, len(deps))
	for _, d := range deps {
		if d.unmet != 0 {
			continue
		}
		t := &entity.Task{
			ID: d.id, ExpressionID: exprID, Op: d.op,
			Arg1Value: d.arg1, Arg2Value: d.arg2,
		}
		if err := enqueueTask(ctx, q, t); err != nil {
			return nil, err
		}
		if err := q.MarkTaskReady(ctx, d.id); err != nil {
			return nil, err
		}
		events = append(events, messages.Event{
			Kind: eventKindTaskUpdated, ExpressionID: exprID, TaskID: &t.ID,
			Status: string(entity.TaskReady), At: now,
		})
	}
	return events, nil
}
