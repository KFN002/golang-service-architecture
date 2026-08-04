# Distributed Expression Calculator — Design Spec

**Date:** 2026-08-04
**Status:** Approved
**Template basis:** [evrone/go-clean-template](https://github.com/evrone/go-clean-template) (clean architecture layering)

## 1. Overview

A showcase-grade, highload-ready distributed system that evaluates mathematical
expressions asynchronously. An **orchestrator** parses each expression into a DAG of
single-operation tasks, fans them out through RabbitMQ to auto-scaling **agent**
worker pools, fans results back in, and streams live progress to a **Next.js 16**
dark dashboard that visualizes the DAG, the worker fleet, and the full distributed
trace of every expression — browser to worker and back.

The project doubles as a teaching artifact: every pattern (clean architecture,
fan-in/fan-out, worker pool, circuit breaker, retry queue, outbox, tracing) is
implemented explicitly, in its own well-named package, with a guided tour on the
frontend and a beautiful README.

### Goals

- Correct async evaluation of arithmetic expressions (`+ - * /`, parentheses,
  unary minus, decimals) with per-operation distributed execution.
- Horizontal scalability: N stateless orchestrator replicas behind nginx,
  M agent replicas, each with an auto-scaling goroutine pool.
- Fault tolerance: at-least-once delivery, idempotent handling, retry with
  backoff, retry queue + DLQ, circuit breakers, transactional outbox,
  graceful shutdown.
- Full-stack observability: one OpenTelemetry trace per expression spanning
  browser → nginx → gateway → gRPC → RabbitMQ → agent → fan-in; Prometheus
  metrics; Grafana dashboards; correlated zap logs.
- Educational clarity: SOLID throughout, one concern per package/file,
  frontend tour, README with diagrams.

### Non-goals

- Authentication/authorization (out of scope for the showcase).
- Kubernetes manifests (docker-compose only; layout is k8s-friendly).
- Functions/variables in expressions; arbitrary-precision arithmetic.

## 2. Topology

```
Next.js 16 (shadcn dark) ──HTTP/SSE──▶ nginx (least_conn) ──▶ N× orchestrator
        │ OTLP/HTTP (browser spans)                             │  gRPC:50051 + gateway HTTP:8080
        ╰────────────────▶ otel-collector ◀── OTLP ─────────────┤
                               │                                ▼
                            Jaeger v2              PG18 (sqlc) ◀── usecase ──▶ Redis 8 (cache + pub/sub → SSE)
                                                                │
                                                     RabbitMQ 4 (quorum queues)
                                                     tasks ─▶ [retry-queue TTL→requeue] ─▶ DLQ
                                                                │
                                                                ▼
                                                          M× agent (auto-scaling worker pool)
                                                                │ results queue
                                                                ╰──▶ orchestrator fan-in → unlock dependents / finalize
```

Two Go services (Approach A, approved):

- **orchestrator** — gRPC server with grpc-gateway mounted in-process (one binary,
  two ports). Parses, plans, persists, publishes, consumes results, streams SSE.
  Stateless; replicas coordinate only through PG (`FOR UPDATE SKIP LOCKED`) and
  RabbitMQ.
- **agent** — RabbitMQ consumer feeding an auto-scaling worker pool; computes
  single operations with configurable artificial latency; publishes results.

## 3. Expression lifecycle (data flow)

1. `POST /api/v1/expressions` (browser span is trace root; `traceparent` propagates).
2. Validation in `pkg/validator` (syntax, balanced parens, division-by-zero
   literals, length limits) → typed errors from `pkg/apperrors`.
3. Parser (shunting-yard → AST) in `internal/usecase/expression`.
4. Planner flattens AST to a task DAG: leaf operations reference literal args;
   inner operations reference child task IDs.
5. One sqlc transaction: insert expression (with `trace_id`), tasks, and outbox
   rows for ready tasks (no unmet deps).
6. Outbox relay publishes ready tasks to `tasks.exchange` (publisher confirms) —
   fan-out.
7. Agents consume; pool autoscaler sizes goroutines by queue depth between
   `min..max`; each op sleeps its configured demo latency, computes, publishes
   to `results.queue`.
8. Orchestrator result consumer (fan-in): idempotency check by task UUID +
   attempt, record result, decrement dependency counters, enqueue newly-ready
   tasks via outbox, publish state-change to Redis pub/sub.
9. SSE handler relays Redis events to subscribed dashboards.
10. Root task completion finalizes the expression (status, result, `done_at`).
11. Failures: transient → retry queue (TTL + DLX loop, exponential backoff
    headers, max attempts); permanent (e.g. division by zero) → task and
    expression marked failed with typed error; poisoned messages → DLQ,
    inspectable via API.

## 4. Repository layout

```
├── cmd/
│   ├── orchestrator/main.go
│   └── agent/main.go
├── config/                      # env-first config loading (orchestrator.yml, agent.yml defaults)
├── internal/
│   ├── entity/                  # Expression, Task, DAG, statuses — pure domain, thread-safe
│   ├── usecase/
│   │   ├── expression/          # submit, get, list, progress; parser + planner
│   │   ├── scheduler/           # fan-out, fan-in, dependency unlock, finalize
│   │   └── worker/              # agent-side: pool orchestration, compute, autoscale policy
│   ├── repo/
│   │   ├── persistent/          # sqlc-generated code + adapters implementing usecase interfaces
│   │   └── cache/               # Redis cache + event publisher
│   ├── controller/
│   │   ├── grpc/v1/             # ExpressionService implementation
│   │   ├── http/v1/             # gateway mux, SSE handler, health, DLQ inspection
│   │   └── amqp/v1/             # task consumer (agent), result consumer (orchestrator)
│   └── app/                     # DI wiring, lifecycle (errgroup), graceful shutdown
├── pkg/
│   ├── apperrors/               # typed error taxonomy (separate package for errors)
│   ├── constants/               # shared constants (separate package)
│   ├── logger/                  # zap: colored/emoji dev encoder, JSON prod, trace correlation
│   ├── validator/               # expression validation utils
│   ├── postgres/ redis/ rabbitmq/   # infra clients: pooling, confirms, topology declaration
│   ├── circuitbreaker/          # three-state breaker, half-open probes, atomics
│   ├── retry/                   # backoff + jitter, context-aware
│   ├── workerpool/              # generic auto-scaling pool (atomics; used by agent)
│   └── otel/                    # tracer/meter provider setup, AMQP header propagation
├── proto/v1/expression.proto    # + google.api.http annotations
├── gen/                         # buf-generated Go + gateway + OpenAPI (committed, drift-checked)
├── db/
│   ├── migrations/              # goose
│   └── queries/                 # sqlc sources; sqlc.yaml
├── web/                         # Next.js 16, shadcn/ui, dark; app router
├── deploy/
│   ├── docker-compose.yml
│   ├── nginx/  prometheus/  grafana/  otel/   # configs + provisioned dashboards
│   └── docker/                  # Dockerfiles (orchestrator, agent, web) — multi-stage alpine
├── .github/workflows/ci.yml
├── Makefile   README.md   task.md   docs/
```

Dependency rule: `entity ← usecase ← {repo, controller}`; `usecase` depends only
on interfaces it declares (`TaskPublisher`, `ExpressionRepo`, `EventPublisher`,
`Clock`…). `pkg/` libraries are standalone and domain-free.

## 5. Data model (PG18, goose + sqlc)

- `expressions(id uuid pk, raw text, status expr_status, result numeric null,
  error text null, trace_id text, created_at, done_at null)`
- `tasks(id uuid pk, expression_id fk, op char, arg1_value numeric null,
  arg1_task_id uuid null, arg2_value numeric null, arg2_task_id uuid null,
  unmet_deps int, status task_status, result numeric null, attempt int,
  worker_id text null, queued_at, started_at, finished_at)`
- `outbox(id bigserial pk, kind text, payload jsonb, created_at, published_at null)`

Concurrency: replicas claim outbox rows and results with
`FOR UPDATE SKIP LOCKED`; dependency decrement is a single atomic UPDATE
returning readiness.

## 6. API surface

`ExpressionService` (proto v1): `SubmitExpression`, `GetExpression`,
`ListExpressions` (paginated), `GetTaskGraph`, `WatchExpression`
(server-stream; gRPC parity with SSE). grpc-gateway → REST under `/api/v1/*`;
OpenAPI generated in CI. Extra HTTP-only endpoints: `GET /api/v1/events`
(SSE), `GET /healthz`, `GET /readyz`, `GET /metrics`, DLQ list/requeue.

## 7. Async & concurrency patterns

Fan-out (scheduler → queue), fan-in (results merge), auto-scaling worker pool
(atomic size counter, scale-up on backlog, idle-timeout scale-down), pipeline
(parse→plan→persist→publish), errgroup lifecycles, semaphore-bounded DB writes,
context cancellation end-to-end, `atomic.Int64`/`RWMutex` on all shared state
(pool stats, breaker state, SSE registry, in-memory DAG views).

## 8. Fault tolerance

Publisher confirms + mandatory flag; manual acks; idempotent consumers (task
UUID + attempt dedup); retry queue via per-message TTL + dead-letter-exchange
cycle with exponential backoff and `x-attempt` headers, capped → DLQ;
`pkg/retry` (jittered exponential backoff) around transient infra calls;
`pkg/circuitbreaker` around PG, Redis, and publishing; transactional outbox so
DB and broker never diverge; graceful shutdown (stop intake → drain pool →
nack unfinished → close confirms → flush telemetry).

## 9. Observability

- **Tracing:** OTel SDK in orchestrator, agent, Next.js server
  (`instrumentation.ts` + `@vercel/otel`), and browser (Web SDK: fetch
  auto-instrumentation + manual `ui.submit_expression` root span). Context via
  W3C `traceparent` including AMQP headers. Export OTLP → collector → Jaeger v2.
  Browser exports OTLP/HTTP through nginx route (`/v1/traces`), collector stays
  private.
- **Trace persistence:** `expressions.trace_id` stamped at submit, returned by
  API — every expression permanently linked to its trace.
- **Dashboard trace view:** Expression detail → Trace tab renders a custom
  SVG waterfall (queue-wait vs compute visually distinct, service colors,
  durations) from Jaeger query API proxied via nginx; deep links to Jaeger UI.
- **Metrics:** Prometheus on both services: pool size gauge, per-op duration
  histograms, publish/consume/retry/DLQ counters, breaker state, SSE clients,
  expression throughput + latency. Grafana pre-provisioned dashboards (JSON in
  repo).
- **Logs:** zap custom dev encoder (colored, aligned, emoji tags), JSON in
  prod, `trace_id`/`span_id` on every line.
- **Health:** `/healthz`, `/readyz` (deps checked), gRPC health protocol;
  wired into compose healthchecks and nginx upstream checks. pprof on private
  port, config-gated.

## 10. Frontend (web/, Next.js 16, shadcn/ui, dark theme)

- **Dashboard `/`** — submit box (client-side validation mirroring
  `pkg/validator` rules), live expression cards (status, progress bar, result).
- **Expression `/expressions/[id]`** — animated DAG (custom SVG: amber
  in-flight, green done, red failed; edges light on unlock) + **Trace tab**
  (waterfall, above).
- **Workers `/workers`** — live per-agent pool-size chart (Recharts), autoscale
  event feed.
- **Tour `/tour`** — guided walkthrough: architecture diagram, each pattern
  explained, "follow one addition through six services" trace story, links to
  Jaeger/Grafana/RabbitMQ UIs.
- SSE client with auto-reconnect; server components where possible; Recharts
  for charts; strict dark aesthetic.

## 11. Deployment

docker-compose: nginx:alpine (`least_conn` over orchestrators; routes: `/api` →
orchestrators, `/v1/traces` → collector, `/jaeger-api` → Jaeger query, `/` →
web), 3× orchestrator, 2× agent (`--scale` friendly), postgres:18-alpine,
redis:8-alpine, rabbitmq:4-management-alpine, otel-collector, jaeger:2,
prometheus, grafana, web. Multi-stage Dockerfiles: `golang:1.26-alpine` →
static binary → `scratch` (+ca-certs) non-root; `node:24-alpine` standalone
Next build. Resource limits + healthchecks on every service.

## 12. CI (GitHub Actions)

Jobs: (1) generate — buf lint + `buf generate`, `sqlc generate`, fail on git
diff (drift check); (2) lint — golangci-lint, `next lint`; (3) test —
`go test -race ./...` + web tests; (4) build — docker build matrix for all
three images. Makefile mirrors every CI step locally (`make generate lint
test up down`).

## 13. Testing

- Unit (table-driven, `-race`): parser, planner/DAG, validator, workerpool
  autoscaling, circuitbreaker transitions, retry backoff, logger encoder.
- Integration (testcontainers): persistent repo vs real PG18 (migrations
  applied), outbox relay vs real RabbitMQ, idempotent fan-in.
- E2E: compose up → submit expressions (happy, failing, deep-nesting) → assert
  results, SSE events, and trace existence via Jaeger API.

## 14. Configuration

`config/` loads YAML defaults overridden by env (`APP_` prefix). Key knobs:
pool `min/max/idle-timeout`, per-op demo latency (`OP_LATENCY_ADD=1s` … `0` for
benchmarks), retry/breaker tuning, ports, DSNs, telemetry endpoints, pprof
toggle. `.env.example` documents everything.

## 15. Versions (pinned at design time)

Go 1.26 · PG `18-alpine` · Redis `8-alpine` · RabbitMQ `4-management-alpine` ·
Next.js 16.3 · Node 24 · Jaeger v2 · grpc-gateway v2.29 · latest stable sqlc,
goose, buf, zap, otel-go at implementation time.

## 16. SOLID mapping (explicit, for the tour/README)

- **S**: one package per concern (`retry` ≠ `circuitbreaker` ≠ `workerpool`).
- **O**: new operations = registry entry + latency config; no scheduler edits.
- **L**: all `usecase` deps are interfaces; fake/real repos interchangeable in tests.
- **I**: narrow interfaces (`TaskPublisher` publishes; it cannot consume).
- **D**: usecases own their interfaces; `internal/app` injects implementations.
