// Package constants centralizes every shared literal of the system so that no
// magic strings or numbers leak into business logic. One concern per block.
package constants

import "time"

// Service names — used for telemetry resources, logging tags and audit records.
const (
	ServiceOrchestrator = "orchestrator"
	ServiceAgent        = "agent"
	ServiceAudit        = "audit"
	ServiceWeb          = "web"
)

// Supported operations of the calculator.
const (
	OpAdd = "+"
	OpSub = "-"
	OpMul = "*"
	OpDiv = "/"
)

// AMQP topology names. Declared once in pkg/rabbitmq, referenced everywhere.
const (
	TasksExchange   = "calc.tasks.ex"
	TasksQueue      = "calc.tasks.q"
	TasksRoutingKey = "task"

	ResultsExchange   = "calc.results.ex"
	ResultsQueue      = "calc.results.q"
	ResultsRoutingKey = "result"

	AuditExchange   = "audit.ex"
	AuditQueue      = "audit.q"
	AuditRoutingKey = "event"

	RetrySuffix = ".retry"
	DLQSuffix   = ".dlq"

	HeaderAttempt     = "x-attempt"
	HeaderTraceparent = "traceparent"
	HeaderEventID     = "x-event-id"
)

// Redis channels and key prefixes.
const (
	EventsChannel      = "calc:events"
	ExpressionCacheKey = "calc:expr:"   // + expression id
	AuditDedupKey      = "audit:dedup:" // + event id
	AuditQueryCacheKey = "audit:query:" // + query hash
)

// Limits and defaults that guard the public surface (security hardening).
const (
	MaxExpressionLength = 512
	MaxParenDepth       = 64
	MaxTasksPerExpr     = 256
	MaxBodyBytes        = 64 << 10 // 64 KiB request body cap
	MaxAuditBatch       = 1000     // events per WriteEvents call
	MaxPageSize         = 200

	DefaultHTTPReadTimeout  = 10 * time.Second
	DefaultHTTPWriteTimeout = 30 * time.Second
	DefaultHTTPIdleTimeout  = 90 * time.Second
	DefaultShutdownTimeout  = 20 * time.Second

	AuditDedupTTL = 24 * time.Hour
)
