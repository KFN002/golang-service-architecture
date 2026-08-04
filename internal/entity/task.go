package entity

import (
	"time"

	"github.com/google/uuid"
)

// TaskStatus is the lifecycle state of one operation inside an expression DAG.
type TaskStatus string

const (
	TaskPending TaskStatus = "pending" // waiting for dependencies
	TaskReady   TaskStatus = "ready"   // published, waiting for a worker
	TaskRunning TaskStatus = "running" // claimed by a worker
	TaskDone    TaskStatus = "done"
	TaskFailed  TaskStatus = "failed"
)

// Task is a single binary operation node of an expression DAG.
//
// Exactly one of ArgNValue / ArgNTaskID is set per argument: a literal value,
// or a dependency on another task's future result.
type Task struct {
	ID           uuid.UUID
	ExpressionID uuid.UUID
	Op           string
	Arg1Value    *float64
	Arg1TaskID   *uuid.UUID
	Arg2Value    *float64
	Arg2TaskID   *uuid.UUID
	UnmetDeps    int
	Status       TaskStatus
	Result       *float64
	Attempt      int
	WorkerID     string
	QueuedAt     *time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
	IsRoot       bool
}

// Ready reports whether all argument values are resolved.
func (t *Task) Ready() bool { return t.UnmetDeps == 0 }
