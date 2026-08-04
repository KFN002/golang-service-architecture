// Package v1 implements the ExpressionService gRPC server.
package v1

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	calcv1 "github.com/KFN002/perfect-go-service/gen/calc/v1"
	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/usecase/expression"
)

// Server serves ExpressionService on top of the expression usecase.
type Server struct {
	calcv1.UnimplementedExpressionServiceServer
	svc *expression.Service
}

// New builds the server.
func New(svc *expression.Service) *Server { return &Server{svc: svc} }

// SubmitExpression handles POST /api/v1/expressions.
func (s *Server) SubmitExpression(ctx context.Context, req *calcv1.SubmitExpressionRequest) (*calcv1.Expression, error) {
	expr, err := s.svc.Submit(ctx, req.GetRaw())
	if err != nil {
		return nil, err
	}
	return toProto(expr, entity.Progress{}), nil
}

// GetExpression handles GET /api/v1/expressions/{id}.
func (s *Server) GetExpression(ctx context.Context, req *calcv1.GetExpressionRequest) (*calcv1.Expression, error) {
	id, err := expression.ParseID(req.GetId())
	if err != nil {
		return nil, err
	}
	expr, err := s.svc.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	progress, _ := s.svc.Progress(ctx, id)
	return toProto(expr, progress), nil
}

// ListExpressions handles GET /api/v1/expressions.
func (s *Server) ListExpressions(ctx context.Context, req *calcv1.ListExpressionsRequest) (*calcv1.ListExpressionsResponse, error) {
	exprs, total, err := s.svc.List(ctx, int(req.GetPageSize()), int(req.GetPage()))
	if err != nil {
		return nil, err
	}
	out := &calcv1.ListExpressionsResponse{Total: total}
	for _, e := range exprs {
		out.Expressions = append(out.Expressions, toProto(e, entity.Progress{}))
	}
	return out, nil
}

// GetTaskGraph handles GET /api/v1/expressions/{id}/graph.
func (s *Server) GetTaskGraph(ctx context.Context, req *calcv1.GetExpressionRequest) (*calcv1.TaskGraph, error) {
	id, err := expression.ParseID(req.GetId())
	if err != nil {
		return nil, err
	}
	tasks, err := s.svc.Graph(ctx, id)
	if err != nil {
		return nil, err
	}
	graph := &calcv1.TaskGraph{ExpressionId: id.String()}
	for _, t := range tasks {
		graph.Tasks = append(graph.Tasks, taskToProto(t))
	}
	return graph, nil
}

// WatchExpression streams state changes until the expression settles.
func (s *Server) WatchExpression(req *calcv1.GetExpressionRequest, stream calcv1.ExpressionService_WatchExpressionServer) error {
	id, err := expression.ParseID(req.GetId())
	if err != nil {
		return err
	}
	ctx := stream.Context()
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	var lastStatus entity.ExpressionStatus
	var lastDone int
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			expr, err := s.svc.Get(ctx, id)
			if err != nil {
				return err
			}
			progress, _ := s.svc.Progress(ctx, id)
			if expr.Status != lastStatus || progress.Done != lastDone {
				lastStatus, lastDone = expr.Status, progress.Done
				if err := stream.Send(toProto(expr, progress)); err != nil {
					return err
				}
			}
			if expr.Status == entity.ExpressionDone || expr.Status == entity.ExpressionFailed {
				return nil
			}
		}
	}
}

// ---- mapping ---------------------------------------------------------------

// i32 clamps an int into int32 range (values here are counts ≤ MaxTasksPerExpr,
// but gosec rightly demands the conversion be provably safe).
func i32(n int) int32 {
	if n > 1<<31-1 {
		return 1<<31 - 1
	}
	if n < -(1 << 31) {
		return -(1 << 31)
	}
	return int32(n)
}

func toProto(e *entity.Expression, p entity.Progress) *calcv1.Expression {
	out := &calcv1.Expression{
		Id:        e.ID.String(),
		Raw:       e.Raw,
		Status:    statusToProto(e.Status),
		Error:     e.Error,
		TraceId:   e.TraceID,
		CreatedAt: timestamppb.New(e.CreatedAt),
		Progress:  &calcv1.Progress{Total: i32(p.Total), Done: i32(p.Done)},
	}
	if e.Result != nil {
		out.HasResult = true
		out.Result = *e.Result
	}
	if e.DoneAt != nil {
		out.DoneAt = timestamppb.New(*e.DoneAt)
	}
	return out
}

func statusToProto(s entity.ExpressionStatus) calcv1.ExpressionStatus {
	switch s {
	case entity.ExpressionPending:
		return calcv1.ExpressionStatus_EXPRESSION_STATUS_PENDING
	case entity.ExpressionInProgress:
		return calcv1.ExpressionStatus_EXPRESSION_STATUS_IN_PROGRESS
	case entity.ExpressionDone:
		return calcv1.ExpressionStatus_EXPRESSION_STATUS_DONE
	case entity.ExpressionFailed:
		return calcv1.ExpressionStatus_EXPRESSION_STATUS_FAILED
	default:
		return calcv1.ExpressionStatus_EXPRESSION_STATUS_UNSPECIFIED
	}
}

func taskStatusToProto(s entity.TaskStatus) calcv1.TaskStatus {
	switch s {
	case entity.TaskPending:
		return calcv1.TaskStatus_TASK_STATUS_PENDING
	case entity.TaskReady:
		return calcv1.TaskStatus_TASK_STATUS_READY
	case entity.TaskRunning:
		return calcv1.TaskStatus_TASK_STATUS_RUNNING
	case entity.TaskDone:
		return calcv1.TaskStatus_TASK_STATUS_DONE
	case entity.TaskFailed:
		return calcv1.TaskStatus_TASK_STATUS_FAILED
	default:
		return calcv1.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

func taskToProto(t *entity.Task) *calcv1.TaskNode {
	out := &calcv1.TaskNode{
		Id:       t.ID.String(),
		Op:       t.Op,
		Status:   taskStatusToProto(t.Status),
		WorkerId: t.WorkerID,
		Attempt:  i32(t.Attempt),
		IsRoot:   t.IsRoot,
	}
	if t.Arg1Value != nil {
		out.HasArg1Value, out.Arg1Value = true, *t.Arg1Value
	}
	if t.Arg1TaskID != nil {
		out.Arg1TaskId = t.Arg1TaskID.String()
	}
	if t.Arg2Value != nil {
		out.HasArg2Value, out.Arg2Value = true, *t.Arg2Value
	}
	if t.Arg2TaskID != nil {
		out.Arg2TaskId = t.Arg2TaskID.String()
	}
	if t.Result != nil {
		out.HasResult, out.Result = true, *t.Result
	}
	if t.QueuedAt != nil {
		out.QueuedAt = timestamppb.New(*t.QueuedAt)
	}
	if t.StartedAt != nil {
		out.StartedAt = timestamppb.New(*t.StartedAt)
	}
	if t.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*t.FinishedAt)
	}
	return out
}
