package entity

import (
	"time"

	"github.com/google/uuid"
)

// AuditEventType enumerates every event kind the system records.
// Adding a type is a pure extension: no ingester or store code changes (OCP).
type AuditEventType string

const (
	AuditExpressionSubmitted AuditEventType = "expression.submitted"
	AuditExpressionDone      AuditEventType = "expression.done"
	AuditExpressionFailed    AuditEventType = "expression.failed"
	AuditTaskReady           AuditEventType = "task.ready"
	AuditTaskStarted         AuditEventType = "task.started"
	AuditTaskDone            AuditEventType = "task.done"
	AuditTaskFailed          AuditEventType = "task.failed"
	AuditTaskRetried         AuditEventType = "task.retried"
	AuditTaskDeadLettered    AuditEventType = "task.dead_lettered"
	AuditPoolScaled          AuditEventType = "pool.scaled"
	AuditAPIAccess           AuditEventType = "api.access"
)

// AuditEvent is one immutable, append-only record of something that happened.
type AuditEvent struct {
	ID         uuid.UUID
	OccurredAt time.Time
	Type       AuditEventType
	Service    string
	EntityType string
	EntityID   string
	TraceID    string
	Actor      string
	Payload    map[string]any
}

// AuditFilter narrows an audit query. Zero values mean "no constraint".
type AuditFilter struct {
	From       time.Time
	To         time.Time
	Type       AuditEventType
	EntityType string
	EntityID   string
	TraceID    string
	// Keyset cursor: return events strictly older than (CursorTime, CursorID).
	CursorTime time.Time
	CursorID   uuid.UUID
	Limit      int
}

// AuditStats is an aggregate view for dashboards.
type AuditStats struct {
	Total   int64
	ByType  map[string]int64
	Ingest1m int64
}
