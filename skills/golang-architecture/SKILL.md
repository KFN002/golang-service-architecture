---
name: golang-architecture
description: Use when designing or reviewing the structure of a Go service — layering, package boundaries, interface placement, dependency injection, error taxonomy, and configuration. Grounded in this repo's three-service system.
---

# Go Clean Architecture — as actually practiced in this repo

This skill distills how `perfect-go-service` structures three production-shaped services
(orchestrator, agent, audit) so that business logic stays testable, transports stay swappable,
and no layer can reach where it shouldn't. Everything here is enforced by the compiler, not by
convention documents nobody reads.

## Overview

Clean architecture in Go is one rule plus discipline: **dependencies point inward**. Entities
know nothing. Usecases know entities. Adapters (repos, controllers) know usecases. The app
layer knows everything — and is the *only* place that does.

```
entity  ←  usecase  ←  { repo, controller }  ←  app  ←  cmd
(pure)     (logic)       (adapters)            (wiring)  (main)
```

The payoff you're buying: swap RabbitMQ for Kafka, Fiber for stdlib, PG for anything —
without touching a line of business logic. The pipeline test proves it: the whole DAG
choreography runs against a real PG with the broker replaced by a capture function.

## The rules

1. **Entities import nothing but stdlib (+uuid).** If `internal/entity` imports a framework,
   your domain is coupled to it forever. WHY: entities outlive every framework choice.
2. **Usecases own their interfaces (ports).** The interface lives next to its *consumer*,
   not its implementor. WHY: this is the Dependency Inversion Principle mechanically — the
   usecase defines what it needs; adapters conform.
3. **One concern per package. Name the package after the concern.** `retry` ≠ `circuitbreaker`
   ≠ `workerpool` ≠ `batcher`. WHY: small packages are reviewable, reusable, and honest about
   their scope; a `utils` package is a landfill.
4. **Transport mapping happens in exactly one place.** Error-code→gRPC-status lives in one
   function. WHY: two mapping sites *will* drift; then REST and gRPC disagree about the same error.
5. **Mains are thin.** `cmd/*/main.go` parses config and calls `app.Run*`. WHY: everything a
   main does is untestable; keep it to three lines.
6. **DI by hand, in `internal/app`.** No framework, no reflection, no magic. WHY: a 150-line
   wiring function you can read beats a container you have to debug.
7. **Generated code is quarantined and drift-checked.** `gen/`, `sqlcgen/` are excluded from
   lint, committed, and CI fails if regeneration changes them. WHY: generated code is an
   artifact — review the *inputs* (proto, SQL), verify the outputs are current.
8. **Config is env-first with typed defaults, loaded once.** WHY: 12-factor containers, and a
   single `Load*()` function documents every knob a service accepts.

## Layer by layer

### Entities: the still core

The problem: business types accrete framework tags, DB annotations, transport enums — until
"domain" means "everything".

The solution — `backend/internal/entity/task.go`:

```go
// Task is a single binary operation node of an expression DAG.
type Task struct {
	ID           uuid.UUID
	ExpressionID uuid.UUID
	Op           string
	Arg1Value    *float64
	Arg1TaskID   *uuid.UUID
	// ...
	UnmetDeps    int
	Status       TaskStatus
}

// Ready reports whether all argument values are resolved.
func (t *Task) Ready() bool { return t.UnmetDeps == 0 }
```

No JSON tags, no `gorm:`, no proto. Wire shapes live in `usecase/messages` (DTOs) and
`gen/` (proto); DB shapes live in `sqlcgen`. Three representations, three owners, explicit
conversion at each boundary. That conversion code *is* the architecture — don't resent it.

| ❌ Wrong | ✅ Right |
|---|---|
| One struct with `json:"..." db:"..." proto:"..."` tags serving three masters | Entity + DTO + generated row, mapped explicitly |
| Entity methods that call repositories | Entities compute over their own fields only |
| `entity` importing `pgtype` for nullable columns | Pointers in the entity; `pgtype` stays in the adapter |

### Ports: interfaces live with their consumers

The problem: interfaces defined next to implementations (`repo.Repository`) invert nothing —
usecases still import the repo package.

The solution — `backend/internal/usecase/expression/ports.go`:

```go
// Store is the persistence port this usecase owns (DIP: the interface lives
// with its consumer; internal/repo/persistent implements it).
type Store interface {
	CreateExpression(ctx context.Context, expr *entity.Expression,
		tasks []*entity.Task, audit []messages.AuditMessage) error
	GetExpression(ctx context.Context, id uuid.UUID) (*entity.Expression, error)
	// ...
}

// Notifier publishes dashboard events (Redis pub/sub → SSE).
type Notifier interface {
	Notify(ctx context.Context, ev messages.Event)
}
```

And the adapter *proves* conformance at compile time — `backend/internal/repo/persistent/repo.go`:

```go
// Compile-time port checks.
var (
	_ expression.Store = (*Repo)(nil)
	_ scheduler.Store  = (*Repo)(nil)
)
```

Note the interface granularity: `Notifier` has ONE method. `TaskPublisher` (scheduler's port)
can publish but not consume. That's Interface Segregation doing real work — a fake for tests
is three lines, and no consumer can reach capabilities it shouldn't have.

**How to verify:** `integration/pipeline_test.go` drives the full fan-out/fan-in choreography
through these ports with `nopNotifier{}` and a closure instead of a broker. If your ports are
right, that test is easy to write. If it's hard, your ports leak implementation.

### Usecases: where the verbs live

`backend/internal/usecase/expression/service.go` — note what Submit does and doesn't know:

```go
// Submit validates, parses, plans and persists an expression.
// Ready tasks reach RabbitMQ via the transactional outbox — this method never
// talks to the broker directly, so DB state and queue can not diverge.
func (s *Service) Submit(ctx context.Context, raw string) (*entity.Expression, error) {
	if err := validator.ValidateExpression(raw); err != nil {
		return nil, err
	}
	ast, err := Parse(raw)
	...
	tasks, immediate, err := Plan(expr.ID, ast)
	...
	if err := s.store.CreateExpression(ctx, expr, tasks, []messages.AuditMessage{submittedAudit}); err != nil {
		return nil, err
	}
```

Submit doesn't know RabbitMQ exists. It writes intent (outbox rows) through its port; a
different usecase (the scheduler's relay) turns intent into publishes. Single Responsibility
at the usecase level: *submitting* and *dispatching* are different jobs with different
failure modes.

### Adapters: dumb on purpose

Repos convert types and execute queries. Controllers convert wire formats and call usecases.
Neither *decides* anything. The moment an adapter contains an `if` about business state,
stop — that branch belongs in a usecase.

### App: the composition root

`backend/internal/app/orchestrator.go` builds the object graph in dependency order, visibly:

```go
	repo := persistent.New(pool)
	notifier := cache.NewNotifier(rds, log)

	breaker := circuitbreaker.New("amqp-publish", circuitbreaker.Config{}, logBreaker(log))
	guarded := amqpv1.NewGuardedPublisher(pub, breaker)

	exprSvc := expression.NewService(repo, notifier, log)
	sched := scheduler.New(scheduler.Config{...}, repo, guarded, notifier, log)
```

Read top to bottom: infra → adapters → usecases → controllers → lifecycles. When wiring gets
long, extract *named helpers* (`declareFlows`, `readiness`) — never a DI framework.

## The error taxonomy

The problem: `errors.New` everywhere means transports guess at status codes and retry logic
string-matches messages.

The solution — one typed error, stable codes, classification helpers.
`backend/pkg/apperrors/apperrors.go`:

```go
type Code string

// The stable error classes of the system.
const (
	CodeInvalidInput   Code = "INVALID_INPUT"
	CodeNotFound       Code = "NOT_FOUND"
	CodeUnavailable    Code = "UNAVAILABLE"
	CodeRateLimited    Code = "RATE_LIMITED"
	CodeOverloaded     Code = "OVERLOADED"
	CodeDivisionByZero Code = "DIVISION_BY_ZERO"
	CodeInternal       Code = "INTERNAL"
)

// IsRetryable reports whether the error class is transient — safe to retry.
func IsRetryable(err error) bool {
	switch CodeOf(err) {
	case CodeUnavailable, CodeRateLimited, CodeOverloaded:
		return true
	default:
		return false
	}
}
```

`IsRetryable` is the load-bearing function: `pkg/retry` uses it to stop hammering permanent
failures, and the AMQP dispatcher uses it to choose retry-queue vs DLQ. Error *classification*
is a domain decision, made once, at error creation.

And the single mapping site — `backend/internal/controller/grpc/interceptors/interceptors.go`:

```go
// StatusFromError converts a typed error into a gRPC status — the single
// place the taxonomy meets the transport. Both the interceptor chain (network
// gRPC) and the in-process gateway servers (which bypass interceptors) call
// this, so REST and gRPC can never disagree on status mapping.
func StatusFromError(err error) error {
```

**War story:** the in-process grpc-gateway bypasses interceptors. Before the E2E smoke test,
invalid input returned HTTP **500** instead of 400 — the typed error never became a status.
The fix was calling `StatusFromError` in the server methods too. Lesson: *know which paths
skip your middleware*, and keep the mapping callable from anywhere.

## Configuration

`backend/config/config.go` — env-first, defaults inline, one struct per service:

```go
func LoadOrchestrator() Orchestrator {
	return Orchestrator{
		Common:        common("orchestrator"),
		HTTPPort:      envInt("HTTP_PORT", 8080),
		PGDSN:         envStr("PG_DSN", "postgres://calc:calc@localhost:5432/calc?sslmode=disable"),
		RelayInterval: envDur("RELAY_INTERVAL", 100*time.Millisecond),
		RateRPS:       envFloat("RATE_RPS", 50),
	}
}
```

Rules that keep this sane:
- Every knob has a working local default — `make run-orchestrator` works with zero setup.
- `.env.example` documents everything; a real `.env` is gitignored.
- Config structs are plain data. No methods that fetch, no lazy loading, no globals.
- Constants that are *invariants* (queue names, limits) live in `pkg/constants`, not config —
  if changing it should require a code review, it's a constant.

## Decision table: where does this code go?

| You're writing… | It goes in… | Because |
|---|---|---|
| A type the business talks about | `internal/entity` | Pure domain |
| A verb the business performs | `internal/usecase/<area>` | Logic layer |
| An interface a usecase needs | The usecase's `ports.go` | Consumer owns the port |
| SQL / Redis / AMQP specifics | `internal/repo` or `internal/controller` | Adapter |
| A reusable, domain-free mechanism | `pkg/<mechanism>` | Standalone library |
| Wiring, lifecycles, shutdown | `internal/app` | Composition root |
| A wire DTO crossing services | `internal/usecase/messages` | Contract, versioned separately from entities |
| A magic number two packages share | `pkg/constants` | One source of truth |

## Anti-patterns seen in the wild

**The god repository.** One `Repository` interface with 40 methods that every usecase imports.
Every fake implements 40 methods; every change ripples everywhere.

```go
// ❌
type Repository interface {
	CreateUser(...); GetUser(...); CreateOrder(...); GetOrder(...)
	// ...36 more
}
```

Fix: per-usecase ports (this repo's `expression.Store` vs `scheduler.Store` — same concrete
`*Repo` implements both, but consumers only see their slice).

**Interface returned, struct needed.** Returning interfaces from constructors
(`func New() Storer`) hides the concrete type's extra methods and breaks compile-time checks.
Accept interfaces, return structs. This repo's constructors all return `*Repo`, `*Service`,
`*Scheduler`; ports are satisfied implicitly.

**The layered lasagna with tunnels.** Layers exist but a controller "just this once" queries
the DB directly. One tunnel becomes ten. In this repo the compiler forbids it: controllers
have no DB handle to borrow — only ports.

**Package by layer, not by feature, inside usecase.** A single `usecase` package with 30
files. This repo: `usecase/expression`, `usecase/scheduler`, `usecase/worker`, `usecase/audit`
— each independently understandable, each owning its ports.

## PR review checklist

- [ ] No import from `entity` → anything, `usecase` → `repo|controller|app`
- [ ] New usecase dependencies are interfaces defined in the usecase package
- [ ] Adapter has a `var _ port = (*Impl)(nil)` compile-time check
- [ ] Errors created via `apperrors` with a deliberate code; no naked `fmt.Errorf` crossing layers
- [ ] Transport status mapping only via `StatusFromError` — grep for `codes.` outside interceptors
- [ ] New config knob: default + `.env.example` entry; new shared literal: `pkg/constants`
- [ ] `main.go` still ≤ ~15 lines
- [ ] Generated code untouched by hand (`make generate-check` passes)
- [ ] New `pkg/` package: no imports from `internal/` (domain-free), has its own test file

## How to verify the whole architecture

```bash
cd backend
go build ./...                                  # layering violations = import cycles = build errors
grep -rn "internal/repo" internal/usecase/      # must output nothing
grep -rn "internal/controller" internal/usecase/ # must output nothing
make test && make test-integration              # ports honest enough to fake AND to run real
```

The strongest signal: `integration/pipeline_test.go` exercises submit → outbox → fan-in →
unlock → finalize with zero broker and zero HTTP. Architecture that can do that is doing
its job.
