// Package entity holds the pure domain model. It depends only on the standard
// library: no transport, storage or framework types may appear here.
package entity

import (
	"time"

	"github.com/google/uuid"
)

// ExpressionStatus is the lifecycle state of a submitted expression.
type ExpressionStatus string

const (
	ExpressionPending    ExpressionStatus = "pending"
	ExpressionInProgress ExpressionStatus = "in_progress"
	ExpressionDone       ExpressionStatus = "done"
	ExpressionFailed     ExpressionStatus = "failed"
)

// Expression is a submitted mathematical expression and its computed outcome.
type Expression struct {
	ID        uuid.UUID
	Raw       string
	Status    ExpressionStatus
	Result    *float64
	Error     string
	TraceID   string
	CreatedAt time.Time
	DoneAt    *time.Time
}

// Progress summarizes DAG completion for dashboards.
type Progress struct {
	Total int
	Done  int
}
