package expression

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/messages"
	"github.com/KFN002/perfect-go-service/pkg/apperrors"
	"github.com/KFN002/perfect-go-service/pkg/constants"
	"github.com/KFN002/perfect-go-service/pkg/otel"
	"github.com/KFN002/perfect-go-service/pkg/validator"
)

// Service implements the expression usecases: submit, read, list, graph.
type Service struct {
	store    Store
	notifier Notifier
	log      *zap.Logger
}

// NewService wires the usecase.
func NewService(store Store, notifier Notifier, log *zap.Logger) *Service {
	return &Service{store: store, notifier: notifier, log: log}
}

// Submit validates, parses, plans and persists an expression.
// Ready tasks reach RabbitMQ via the transactional outbox — this method never
// talks to the broker directly, so DB state and queue can not diverge.
func (s *Service) Submit(ctx context.Context, raw string) (*entity.Expression, error) {
	ctx, span := otel.Tracer("usecase.expression").Start(ctx, "Submit")
	defer span.End()

	if err := validator.ValidateExpression(raw); err != nil {
		return nil, err
	}
	ast, err := Parse(raw)
	if err != nil {
		return nil, err
	}

	expr := &entity.Expression{
		ID:        uuid.New(),
		Raw:       raw,
		Status:    entity.ExpressionPending,
		TraceID:   otel.TraceIDFrom(ctx),
		CreatedAt: time.Now().UTC(),
	}

	tasks, immediate, err := Plan(expr.ID, ast)
	if err != nil {
		return nil, err
	}

	submittedAudit := messages.NewAudit(entity.AuditExpressionSubmitted,
		constants.ServiceOrchestrator, "expression", expr.ID.String(), expr.TraceID,
		map[string]any{"raw": raw, "tasks": len(tasks)})

	if immediate != nil {
		doneAudit := messages.NewAudit(entity.AuditExpressionDone,
			constants.ServiceOrchestrator, "expression", expr.ID.String(), expr.TraceID,
			map[string]any{"result": *immediate})
		if err := s.store.FinalizeImmediate(ctx, expr, *immediate,
			[]messages.AuditMessage{submittedAudit, doneAudit}); err != nil {
			return nil, err
		}
		expr.Status = entity.ExpressionDone
		expr.Result = immediate
		s.notifier.Notify(ctx, messages.Event{
			Kind: "expression.updated", ExpressionID: expr.ID,
			Status: string(expr.Status), Result: immediate, At: time.Now().UTC(),
		})
		return expr, nil
	}

	if err := s.store.CreateExpression(ctx, expr, tasks, []messages.AuditMessage{submittedAudit}); err != nil {
		return nil, err
	}
	s.notifier.Notify(ctx, messages.Event{
		Kind: "expression.updated", ExpressionID: expr.ID,
		Status: string(entity.ExpressionPending), At: time.Now().UTC(),
	})
	s.log.Info("expression submitted",
		zap.String("id", expr.ID.String()), zap.Int("tasks", len(tasks)))
	return expr, nil
}

// Get returns one expression.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*entity.Expression, error) {
	return s.store.GetExpression(ctx, id)
}

// List pages expressions newest-first.
func (s *Service) List(ctx context.Context, pageSize, page int) ([]*entity.Expression, int64, error) {
	if pageSize <= 0 || pageSize > constants.MaxPageSize {
		pageSize = 20
	}
	if page < 0 {
		page = 0
	}
	return s.store.ListExpressions(ctx, pageSize, page*pageSize)
}

// Graph returns the task DAG for visualization.
func (s *Service) Graph(ctx context.Context, exprID uuid.UUID) ([]*entity.Task, error) {
	tasks, err := s.store.GetTasks(ctx, exprID)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		// Distinguish "no tasks" from "no expression".
		if _, err := s.store.GetExpression(ctx, exprID); err != nil {
			return nil, err
		}
	}
	return tasks, nil
}

// Progress reports DAG completion.
func (s *Service) Progress(ctx context.Context, exprID uuid.UUID) (entity.Progress, error) {
	return s.store.GetProgress(ctx, exprID)
}

// ParseID converts an external id string, guarding against garbage input.
func ParseID(id string) (uuid.UUID, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, apperrors.New(apperrors.CodeInvalidInput, "id must be a UUID")
	}
	return u, nil
}
