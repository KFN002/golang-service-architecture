package persistent

import (
	"time"

	"github.com/KFN002/perfect-go-service/internal/entity"
	"github.com/KFN002/perfect-go-service/internal/repo/persistent/sqlcgen"
)

func nowUTC() time.Time { return time.Now().UTC() }

func toExpression(row sqlcgen.Expression) entity.Expression {
	e := entity.Expression{
		ID:        row.ID,
		Raw:       row.Raw,
		Status:    entity.ExpressionStatus(row.Status),
		Result:    row.Result,
		TraceID:   row.TraceID,
		CreatedAt: row.CreatedAt,
		DoneAt:    row.DoneAt,
	}
	if row.Error != nil {
		e.Error = *row.Error
	}
	return e
}

func toTask(row sqlcgen.Task) entity.Task {
	return entity.Task{
		ID:           row.ID,
		ExpressionID: row.ExpressionID,
		Op:           row.Op,
		Arg1Value:    row.Arg1Value,
		Arg1TaskID:   row.Arg1TaskID,
		Arg2Value:    row.Arg2Value,
		Arg2TaskID:   row.Arg2TaskID,
		UnmetDeps:    int(row.UnmetDeps),
		Status:       entity.TaskStatus(row.Status),
		Result:       row.Result,
		Attempt:      int(row.Attempt),
		WorkerID:     row.WorkerID,
		IsRoot:       row.IsRoot,
		QueuedAt:     row.QueuedAt,
		StartedAt:    row.StartedAt,
		FinishedAt:   row.FinishedAt,
	}
}
