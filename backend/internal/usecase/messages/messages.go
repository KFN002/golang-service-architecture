// Package messages defines the wire contracts that cross service boundaries
// over RabbitMQ and Redis pub/sub. They are DTOs — deliberately separate from
// domain entities so the wire format can evolve independently (SRP).
package messages

import (
	"time"

	"github.com/google/uuid"

	"github.com/KFN002/perfect-go-service/internal/entity"
)

// TaskMessage is one ready-to-compute operation, published by the
// orchestrator's outbox relay and consumed by agents.
type TaskMessage struct {
	TaskID       uuid.UUID `json:"task_id"`
	ExpressionID uuid.UUID `json:"expression_id"`
	Op           string    `json:"op"`
	Arg1         float64   `json:"arg1"`
	Arg2         float64   `json:"arg2"`
	Attempt      int       `json:"attempt"`
}

// ResultKind distinguishes lifecycle notifications on the results flow.
type ResultKind string

// Result kinds emitted by agents.
const (
	ResultStarted ResultKind = "started" // agent claimed the task
	ResultOK      ResultKind = "ok"      // computation succeeded
	ResultError   ResultKind = "error"   // permanent computation failure
)

// ResultMessage is the agent's reply, consumed by the orchestrator (fan-in).
type ResultMessage struct {
	Kind         ResultKind `json:"kind"`
	TaskID       uuid.UUID  `json:"task_id"`
	ExpressionID uuid.UUID  `json:"expression_id"`
	Result       float64    `json:"result,omitempty"`
	Error        string     `json:"error,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	WorkerID     string     `json:"worker_id"`
	Attempt      int        `json:"attempt"`
	ComputeMs    int64      `json:"compute_ms,omitempty"`
}

// Event is the SSE/dashboard notification published on Redis pub/sub.
type Event struct {
	Kind         string     `json:"kind"` // expression.updated | task.updated
	ExpressionID uuid.UUID  `json:"expression_id"`
	TaskID       *uuid.UUID `json:"task_id,omitempty"`
	Status       string     `json:"status"`
	Result       *float64   `json:"result,omitempty"`
	Error        string     `json:"error,omitempty"`
	WorkerID     string     `json:"worker_id,omitempty"`
	At           time.Time  `json:"at"`
}

// AuditMessage is the wire form of an audit event on the audit flow.
type AuditMessage struct {
	ID         uuid.UUID      `json:"id"`
	OccurredAt time.Time      `json:"occurred_at"`
	Type       string         `json:"type"`
	Service    string         `json:"service"`
	EntityType string         `json:"entity_type"`
	EntityID   string         `json:"entity_id"`
	TraceID    string         `json:"trace_id"`
	Actor      string         `json:"actor"`
	Payload    map[string]any `json:"payload,omitempty"`
}

// NewAudit builds a wire audit message with a fresh identity.
func NewAudit(typ entity.AuditEventType, service, entityType, entityID, traceID string, payload map[string]any) AuditMessage {
	return AuditMessage{
		ID:         uuid.New(),
		OccurredAt: time.Now().UTC(),
		Type:       string(typ),
		Service:    service,
		EntityType: entityType,
		EntityID:   entityID,
		TraceID:    traceID,
		Payload:    payload,
	}
}

// ToEntity converts the wire form to the domain form.
func (m AuditMessage) ToEntity() entity.AuditEvent {
	return entity.AuditEvent{
		ID:         m.ID,
		OccurredAt: m.OccurredAt,
		Type:       entity.AuditEventType(m.Type),
		Service:    m.Service,
		EntityType: m.EntityType,
		EntityID:   m.EntityID,
		TraceID:    m.TraceID,
		Actor:      m.Actor,
		Payload:    m.Payload,
	}
}
