package expression

import (
	"context"

	"github.com/google/uuid"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
)

// Store is the persistence port this usecase owns (DIP: the interface lives
// with its consumer; internal/repo/persistent implements it).
type Store interface {
	// CreateExpression persists the expression, its task DAG and outbox rows
	// for initially-ready tasks in ONE transaction (with audit outbox rows).
	CreateExpression(ctx context.Context, expr *entity.Expression, tasks []*entity.Task, audit []messages.AuditMessage) error
	// FinalizeImmediate persists an expression that needed no tasks.
	FinalizeImmediate(ctx context.Context, expr *entity.Expression, result float64, audit []messages.AuditMessage) error
	GetExpression(ctx context.Context, id uuid.UUID) (*entity.Expression, error)
	ListExpressions(ctx context.Context, limit, offset int) ([]*entity.Expression, int64, error)
	GetTasks(ctx context.Context, exprID uuid.UUID) ([]*entity.Task, error)
	GetProgress(ctx context.Context, exprID uuid.UUID) (entity.Progress, error)
}

// Notifier publishes dashboard events (Redis pub/sub → SSE).
type Notifier interface {
	Notify(ctx context.Context, ev messages.Event)
}
