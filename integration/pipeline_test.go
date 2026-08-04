//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/repo/persistent"
	"github.com/KFN002/perfect-go-service/internal/usecase/expression"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/internal/usecase/scheduler"
	"github.com/KFN002/perfect-go-service/pkg/jsonx"
)

type nopNotifier struct{}

func (nopNotifier) Notify(context.Context, messages.Event) {}

// TestExpressionPipelineAgainstRealPG drives the orchestrator's full DB
// choreography — plan → outbox fan-out → fan-in → dependent unlock →
// finalization — against a real PostgreSQL 18, with the broker replaced by a
// capture function (the broker path has its own test).
func TestExpressionPipelineAgainstRealPG(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := startPostgres(t, ctx, "calc")
	migrateMain(t, pool)

	log := zap.NewNop()
	repo := persistent.New(pool)
	exprSvc := expression.NewService(repo, nopNotifier{}, log)

	// Submit "2 + 2 * 3": two tasks, mul ready first.
	expr, err := exprSvc.Submit(ctx, "2 + 2 * 3")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// The outbox must now hold the ready task (mul) + submit audit.
	var published []messages.TaskMessage
	drainOutbox := func() {
		for {
			n, err := repo.RelayOutbox(ctx, 64, func(entries []scheduler.OutboxEntry) []int64 {
				done := make([]int64, 0, len(entries))
				for _, e := range entries {
					if e.Kind == "task" {
						var tm messages.TaskMessage
						if err := jsonx.Unmarshal(e.Payload, &tm); err != nil {
							t.Fatalf("bad task payload: %v", err)
						}
						published = append(published, tm)
					}
					done = append(done, e.ID)
				}
				return done
			})
			if err != nil {
				t.Fatalf("relay: %v", err)
			}
			if n == 0 {
				return
			}
		}
	}

	drainOutbox()
	if len(published) != 1 || published[0].Op != "*" {
		t.Fatalf("first fan-out = %+v, want exactly the '*' task", published)
	}
	mul := published[0]

	// Fan-in the mul result: 2*3=6 → add task unlocks with args (2, 6).
	res := messages.ResultMessage{
		Kind: messages.ResultOK, TaskID: mul.TaskID,
		ExpressionID: mul.ExpressionID, Result: 6, WorkerID: "itest",
	}
	audit := []messages.AuditMessage{
		messages.NewAudit(entity.AuditTaskDone, "itest", "task", mul.TaskID.String(), "", nil),
	}
	events, err := repo.ApplyResult(ctx, res, audit)
	if err != nil {
		t.Fatalf("apply mul result: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("events = %d, want task.done + dependent ready", len(events))
	}

	published = published[:0]
	drainOutbox()
	if len(published) != 1 || published[0].Op != "+" {
		t.Fatalf("second fan-out = %+v, want the unlocked '+' task", published)
	}
	add := published[0]
	if add.Arg1 != 2 || add.Arg2 != 6 {
		t.Fatalf("add args = (%v, %v), want (2, 6) — fan-in propagation broken", add.Arg1, add.Arg2)
	}

	// Idempotency: replaying the same result must be a no-op.
	replay, err := repo.ApplyResult(ctx, res, nil)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay != nil {
		t.Fatalf("replayed result produced events %v, want nil (idempotent)", replay)
	}

	// Fan-in the root: 2+6=8 → expression done.
	if _, err := repo.ApplyResult(ctx, messages.ResultMessage{
		Kind: messages.ResultOK, TaskID: add.TaskID,
		ExpressionID: add.ExpressionID, Result: 8, WorkerID: "itest",
	}, nil); err != nil {
		t.Fatalf("apply root result: %v", err)
	}

	final, err := exprSvc.Get(ctx, expr.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if final.Status != entity.ExpressionDone || final.Result == nil || *final.Result != 8 {
		t.Fatalf("final = %+v, want done with result 8", final)
	}

	// The full audit trail must have been enqueued through the outbox:
	// submitted + 2× task.done (mul replayed once, not duplicated) + expression.done.
	rows, err := pool.Query(ctx, `SELECT payload->>'type' FROM outbox WHERE kind = 'audit' ORDER BY id`)
	if err != nil {
		t.Fatalf("query audit outbox: %v", err)
	}
	defer rows.Close()
	var types []string
	for rows.Next() {
		var typ string
		_ = rows.Scan(&typ)
		types = append(types, typ)
	}
	want := map[string]int{"expression.submitted": 1, "task.done": 1, "expression.done": 1}
	got := map[string]int{}
	for _, typ := range types {
		got[typ]++
	}
	for typ, n := range want {
		if got[typ] < n {
			t.Errorf("audit trail missing %s (got %v)", typ, got)
		}
	}
}

// TestImmediateExpression checks the single-literal fast path.
func TestImmediateExpression(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := startPostgres(t, ctx, "calc")
	migrateMain(t, pool)

	exprSvc := expression.NewService(persistent.New(pool), nopNotifier{}, zap.NewNop())
	expr, err := exprSvc.Submit(ctx, "42")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if expr.Status != entity.ExpressionDone || expr.Result == nil || *expr.Result != 42 {
		t.Fatalf("immediate = %+v, want done/42", expr)
	}
}
