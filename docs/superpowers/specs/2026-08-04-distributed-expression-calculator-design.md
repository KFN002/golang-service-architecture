# Distributed Expression Calculator — Design Spec

**Date:** 2026-08-04 (amended: audit service, Fiber, fastest-stack sweep)
**Status:** Approved
**Template basis:** [evrone/go-clean-template](https://github.com/evrone/go-clean-template) (clean architecture layering)

## 1. Overview

A showcase-grade, highload-ready distributed system that evaluates mathematical
expressions asynchronously. An **orchestrator** parses each expression into a DAG of
single-operation tasks, fans them out through RabbitMQ to auto-scaling **agent**
worker pools, fans results back in, and streams live progress to a **Next.js 16**
dark dashboard that visualizes the DAG, the worker fleet, and the full distributed
trace of every expression — browser to worker and back. A third microservice,
**audit**, ingests every system event into an append-only store at high volume,
exercising the write-side highload patterns (micro-batching, write-behind,
backpressure, bulkheads, load shedding) the calculator flow doesn't.

The project doubles as a teaching artifact: every pattern (clean architecture,
fan-in/fan-out, worker pool, circuit breaker, retry queue, outbox, tracing,
append-only design) is implemented explicitly, in its own well-named package,
with a guided tour on the frontend and a beautiful README.

### Goals

- Correct async evaluation of arithmetic expressions (`+ - * /`, parentheses,
  unary minus, decimals) with per-operation distributed execution.
- Horizontal scalability: N stateless orchestrator replicas behind nginx,
  M agent replicas (auto-scaling goroutine pools), K audit replicas.
- Fault tolerance: at-least-once delivery, idempotent handling, retry with
  backoff, retry queue + DLQ, circuit breakers, transactional outbox, bulkheads,
  rate limiting, load shedding, backpressure, graceful shutdown.
- Full-stack observability: one OpenTelemetry trace per expression spanning
  browser → nginx → gateway → gRPC → RabbitMQ → agent → fan-in (audit spans
  join the same trace); Prometheus metrics; Grafana dashboards; correlated zap logs.
- Every HTTP surface on Fiber v3; every tech choice is the fastest production
  option available (see §16).
- Educational clarity: SOLID throughout, one concern per package/file,
  frontend tour, README with diagrams.

### Non-goals

- Authentication/authorization (out of scope for the showcase).
- Kubernetes manifests (docker-compose only; layout is k8s-friendly).
- Functions/variables in expressions; arbitrary-precision arithmetic.

## 2. Topology

```
Next.js 16 (shadcn dark) ──HTTP/SSE──▶ nginx (least_conn) ──┬─▶ N× orchestrator (Fiber HTTP:8080 + gRPC:50051)
        │ OTLP/HTTP (browser spans)                         ╰─▶ K× audit (Fiber HTTP:8081 + gRPC:50052)
        ╰────────────▶ otel-collector ◀── OTLP ── all services         │
                            │                                          ├─ audit-PG18 (append-only, partitioned)
                         Jaeger v2                                     ╰─ audit-Redis 8 (rueidis: dedup, cache)
                                                    │
             PG18 (sqlc) ◀── orchestrator ──▶ Redis 8 (cache + pub/sub → SSE)
                                │
                     RabbitMQ 4 (quorum queues)
          tasks.exchange ─▶ [retry TTL→requeue] ─▶ DLQ        audit.exchange ─▶ audit.queue ─▶ audit ingesters
                                │
                                ▼
                          M× agent (auto-scaling worker pool)
                                │ results queue
                                ╰──▶ orchestrator fan-in → unlock dependents / finalize
```

Three Go services:

- **orchestrator** — gRPC server with grpc-gateway mounted in-process on a
  Fiber v3 HTTP server (v3's native net/http adapter hosts the gateway mux;
  SSE/health/metrics/DLQ endpoints are native Fiber handlers). Parses, plans,
  persists, publishes, consumes results, streams SSE. Stateless; replicas
  coordinate only through PG (`FOR UPDATE SKIP LOCKED`) and RabbitMQ.
- **agent** — RabbitMQ consumer feeding an auto-scaling worker pool; computes
  single operations with configurable artificial latency; publishes results
  and audit events.
- **audit** — highload append-only event store. Ingests from `audit.exchange`
  (primary, async) and a `WriteEvents` gRPC batch endpoint (secondary, sync);
  serves queries over gRPC with its own grpc-gateway in its own Fiber server.
  Owns a **separate PG18 instance** and a **separate Redis instance**.

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
12. **Audit stream (parallel to all of the above):** orchestrator and agent
    publish audit events (expression submitted/finalized, task state
    transitions, retries, DLQ arrivals, API accesses, autoscale events) to
    `audit.exchange` — fire-and-forget with confirms, carrying `traceparent`.

## 4. Audit service (append-only, highload)

### Write path

`audit.queue` consumers (bulkheaded pool per ingestion path) → decode →
validate → dedup (event UUID `SETNX`+TTL in audit-Redis) → bounded ingress
channel (**backpressure**) → **micro-batcher** (flush at N events or T ms,
whichever first; double-buffer swap so ingestion never blocks on flush) →
flusher **worker pool** performs `pgx CopyFrom` (PostgreSQL `COPY`) group
commits into the current time partition. Flush failure → nack → RabbitMQ retry
queue (the broker is the durable overflow buffer — no bespoke WAL). Graceful
shutdown drains the batcher before exit. `WriteEvents` gRPC path feeds the same
pipeline behind its own bulkhead with token-bucket **rate limiting** and
**load shedding** (`RESOURCE_EXHAUSTED` + retry-after when saturated).

### Append-only design

Separate PG18 instance. `audit_events` is INSERT-only: declarative range
partitioning by day (a partition-maintainer goroutine pre-creates ahead and
detaches/archives old ones), BRIN index on `occurred_at`, B-tree on
`(entity_type, entity_id)`, no FKs, no UPDATE/DELETE grants + trigger guard
enforcing immutability. Cursor (keyset) pagination for queries.

### Read path

gRPC `QueryEvents` (filters: time range, type, entity, trace_id) and
`GetStats`; hot queries cached in audit-Redis via rueidis server-assisted
client-side caching, collapsed with **singleflight**; reads are shed before
writes under pressure (writes are the contract).

### Patterns exercised here (beyond the calculator flow)

Micro-batching / group commit, write-behind double-buffering, bounded-channel
backpressure, bulkhead isolation, token-bucket rate limiting, load shedding,
singleflight, keyset pagination, partition lifecycle management — plus the
shared set: circuit breaker on PG, jittered retry, idempotent consumers,
graceful drain.

## 5. Repository layout

```
├── cmd/
│   ├── orchestrator/main.go
│   ├── agent/main.go
│   └── audit/main.go
├── config/                      # env-first config loading (per-service YAML defaults)
├── internal/
│   ├── entity/                  # Expression, Task, DAG, AuditEvent — pure domain, thread-safe
│   ├── usecase/
│   │   ├── expression/          # submit, get, list, progress; parser + planner
│   │   ├── scheduler/           # fan-out, fan-in, dependency unlock, finalize
│   │   ├── worker/              # agent-side: pool orchestration, compute, autoscale policy
│   │   └── audit/               # ingest pipeline, batcher policy, query, stats
│   ├── repo/
│   │   ├── persistent/          # sqlc-generated + adapters (main PG)
│   │   ├── auditstore/          # sqlc-generated + CopyFrom adapters (audit PG)
│   │   └── cache/               # rueidis cache + pub/sub (main + audit clients)
│   ├── controller/
│   │   ├── grpc/v1/             # ExpressionService impl
│   │   ├── grpc/auditv1/        # AuditService impl
│   │   ├── http/v1/             # Fiber apps: gateway mounts, SSE, health, DLQ
│   │   └── amqp/v1/             # task consumer (agent), result consumer (orch), audit ingester
│   └── app/                     # DI wiring, lifecycle (errgroup), graceful shutdown
├── pkg/
│   ├── apperrors/ constants/    # typed error taxonomy; shared constants
│   ├── logger/                  # zap: colored/emoji dev encoder, JSON prod, trace correlation
│   ├── validator/               # expression validation utils
│   ├── postgres/ redis/ rabbitmq/   # pgxpool, rueidis, amqp topology+confirms
│   ├── circuitbreaker/ retry/   # three-state breaker; jittered backoff
│   ├── workerpool/              # generic auto-scaling pool (atomics)
│   ├── batcher/                 # generic micro-batcher (size/interval, double-buffer)
│   ├── bulkhead/ ratelimit/     # isolation pools; token bucket
│   └── otel/                    # tracer/meter providers, AMQP header propagation
├── proto/v1/                    # expression.proto, audit.proto (+ http annotations)
├── gen/                         # buf-generated Go + vtprotobuf + gateway + OpenAPI (drift-checked)
├── db/
│   ├── main/{migrations,queries}/    # goose + sqlc (orchestrator PG)
│   └── audit/{migrations,queries}/   # goose + sqlc (audit PG, partition DDL)
├── web/                         # Next.js 16, shadcn/ui, dark; app router
├── deploy/
│   ├── docker-compose.yml
│   ├── nginx/ prometheus/ grafana/ otel/
│   └── docker/                  # Dockerfiles (orchestrator, agent, audit, web)
├── .github/workflows/ci.yml
├── Makefile   README.md   task.md   docs/
```

Dependency rule: `entity ← usecase ← {repo, controller}`; `usecase` depends only
on interfaces it declares (`TaskPublisher`, `ExpressionRepo`, `EventPublisher`,
`AuditSink`, `Clock`…). `pkg/` libraries are standalone and domain-free.

## 6. Data model

**Main PG18** (goose + sqlc):

- `expressions(id uuid pk, raw text, status expr_status, result numeric null,
  error text null, trace_id text, created_at, done_at null)`
- `tasks(id uuid pk, expression_id fk, op char, arg1_value numeric null,
  arg1_task_id uuid null, arg2_value numeric null, arg2_task_id uuid null,
  unmet_deps int, status task_status, result numeric null, attempt int,
  worker_id text null, queued_at, started_at, finished_at)`
- `outbox(id bigserial pk, kind text, payload jsonb, created_at, published_at null)`

Replicas claim outbox rows and results with `FOR UPDATE SKIP LOCKED`;
dependency decrement is a single atomic UPDATE returning readiness.

**Audit PG18** (separate instance, goose + sqlc):

- `audit_events(id uuid, occurred_at timestamptz, event_type text,
  service text, entity_type text, entity_id text, trace_id text,
  actor text, payload jsonb) PARTITION BY RANGE (occurred_at)` — daily
  partitions, BRIN(`occurred_at`), B-tree(`entity_type, entity_id`),
  INSERT-only (grants + trigger guard).

## 7. API surface

- `ExpressionService` (proto v1): `SubmitExpression`, `GetExpression`,
  `ListExpressions`, `GetTaskGraph`, `WatchExpression` (server-stream).
- `AuditService` (proto auditv1): `WriteEvents` (batch, rate-limited),
  `QueryEvents` (filters + keyset cursor), `GetStats`.
- grpc-gateway → REST under `/api/v1/*` (orchestrator) and `/api/v1/audit/*`
  (audit), each mounted in its own Fiber v3 server; OpenAPI generated in CI.
- HTTP-only: `GET /api/v1/events` (SSE), `/healthz`, `/readyz`, `/metrics`,
  DLQ list/requeue — native Fiber handlers.

## 8. Async & concurrency patterns (full inventory)

Fan-out (scheduler → queue), fan-in (results merge; multi-consumer audit
ingest), auto-scaling worker pool (atomic size counter, backlog scale-up,
idle-timeout scale-down), pipeline (parse→plan→persist→publish;
decode→validate→dedup→batch→flush), micro-batching with double-buffer
write-behind, bounded-channel backpressure, semaphore-bounded DB writes,
errgroup lifecycles, singleflight query collapse, context cancellation
end-to-end, `atomic.Int64`/`RWMutex` on all shared state (pool stats, breaker
state, SSE registry, batcher buffers, rate-limit buckets).

## 9. Fault tolerance (full inventory)

Publisher confirms + mandatory flag; manual acks; idempotent consumers (UUID
dedup — tasks in-DB, audit via Redis SETNX); retry queue via per-message TTL +
DLX cycle with exponential backoff and `x-attempt` headers, capped → DLQ;
`pkg/retry` (jittered exponential backoff) around transient infra calls;
`pkg/circuitbreaker` around PG, Redis, publishing; transactional outbox;
bulkhead isolation per ingestion path; token-bucket rate limiting; load
shedding (reads shed before writes, `RESOURCE_EXHAUSTED` + retry-after);
graceful shutdown (stop intake → drain pools/batchers → nack unfinished →
close confirms → flush telemetry).

## 10. Observability

- **Tracing:** OTel SDK in all three Go services, Next.js server
  (`instrumentation.ts` + `@vercel/otel`), and browser (Web SDK: fetch
  auto-instrumentation + manual `ui.submit_expression` root span). W3C
  `traceparent` everywhere including AMQP headers — audit ingestion spans join
  the originating expression trace. Export OTLP → collector → Jaeger v2.
  Browser exports OTLP/HTTP through nginx (`/v1/traces`); collector stays private.
- **Trace persistence:** `expressions.trace_id` stamped at submit; audit events
  carry `trace_id` too — queryable by trace.
- **Dashboard trace view:** Expression detail → Trace tab renders a custom SVG
  waterfall (queue-wait vs compute visually distinct, service colors,
  durations) from Jaeger query API proxied via nginx; deep links to Jaeger UI.
- **Metrics:** Prometheus on all services: pool/batcher gauges, per-op duration
  histograms, batch size/flush latency histograms, publish/consume/retry/DLQ
  counters, breaker + shed + rate-limit counters, SSE clients, throughput.
  Grafana pre-provisioned dashboards (JSON in repo).
- **Logs:** zap custom dev encoder (colored, aligned, emoji tags), JSON +
  sampling in prod, `trace_id`/`span_id` on every line.
- **Health:** `/healthz`, `/readyz` (deps checked), gRPC health protocol;
  compose healthchecks + nginx upstream checks. pprof on private port, config-gated.

## 11. Frontend (web/, Next.js 16, shadcn/ui, dark theme)

- **Dashboard `/`** — submit box (client-side validation mirroring
  `pkg/validator`), live expression cards (status, progress, result).
- **Expression `/expressions/[id]`** — animated DAG (amber in-flight, green
  done, red failed; edges light on unlock) + **Trace tab** (waterfall).
- **Workers `/workers`** — live per-agent pool-size chart (Recharts), autoscale
  event feed.
- **Audit `/audit`** — virtualized, filterable event stream (time/type/entity/
  trace filters via audit gateway), ingest-rate + batch-size sparklines.
- **Tour `/tour`** — architecture walkthrough, each pattern explained, "follow
  one addition through six services" trace story, links to Jaeger/Grafana/
  RabbitMQ UIs.
- SSE client with auto-reconnect; server components where possible; Recharts.

## 12. Deployment

docker-compose: nginx:alpine (`least_conn`; routes: `/api/v1/audit` → audits,
`/api` → orchestrators, `/v1/traces` → collector, `/jaeger-api` → Jaeger query,
`/` → web), 3× orchestrator, 2× agent, 2× audit (`--scale` friendly),
postgres:18-alpine ×2 (main + audit), redis:8-alpine ×2 (main + audit),
rabbitmq:4-management-alpine, otel-collector, jaeger:2, prometheus, grafana,
web. Multi-stage Dockerfiles: `golang:1.26-alpine` → static binary → `scratch`
(+ca-certs), non-root; `node:24-alpine` standalone Next build. Resource limits
+ healthchecks everywhere.

## 13. CI (GitHub Actions)

(1) generate — buf lint + `buf generate` (incl. vtprotobuf), `sqlc generate`
(both DBs), fail on git diff (drift check); (2) lint — golangci-lint,
`next lint`; (3) test — `go test -race ./...` + web tests; (4) build — docker
build matrix for all four images. Makefile mirrors every step locally.

## 13a. Makefile (developer entrypoint)

Self-documenting (`make help` via `##` comments), tool-pinning via
`go run tool@version` so no global installs are needed. Targets:

- **generate**: `proto-gen` (buf + vtprotobuf + gateway + OpenAPI),
  `sqlc-gen` (both DBs), `generate` (all), `generate-check` (drift check used by CI)
- **db**: `migrate-up` / `migrate-down` / `migrate-status` (goose, main),
  `audit-migrate-*` (goose, audit), `db-reset`
- **quality**: `lint` (golangci-lint), `fmt` (gofumpt + goimports), `vet`,
  `test` (`-race` + coverage), `test-integration` (testcontainers), `test-e2e`
- **run**: `up` / `down` / `logs` (compose), `up-infra` (deps only, for local
  binary debugging), `run-orchestrator` / `run-agent` / `run-audit`,
  `scale-agents N=4`, `web-dev`
- **misc**: `bench`, `pprof-orchestrator`, `clean`, `help` (default)

CI calls the exact same targets — one source of truth for every workflow step.

## 14. Testing

- Unit (table-driven, `-race`): parser, planner/DAG, validator, workerpool,
  batcher (size/interval/drain), bulkhead, ratelimit, circuitbreaker, retry,
  logger encoder.
- Integration (testcontainers): persistent repo vs PG18 (migrations applied),
  auditstore CopyFrom + partition DDL vs PG18, outbox relay vs RabbitMQ,
  idempotent fan-in, dedup vs Redis.
- E2E: compose up → submit expressions (happy, failing, deep-nesting) → assert
  results, SSE events, audit trail completeness, and trace existence via
  Jaeger API.

## 15. Configuration

`config/` loads YAML defaults overridden by env (`APP_` prefix). Key knobs:
pool `min/max/idle-timeout`, per-op demo latency (`OP_LATENCY_ADD=1s`… `0` for
benchmarks), batcher `max-size/max-wait`, rate limits, shed thresholds,
retry/breaker tuning, ports, DSNs, telemetry endpoints, pprof toggle.
`.env.example` documents everything.

## 16. Fastest-stack choices (and why)

| Concern | Choice | Why fastest |
|---|---|---|
| HTTP router/server | **Fiber v3** (fasthttp) | fasthttp core; v3 native net/http adapter hosts grpc-gateway mux without a second server |
| JSON | **bytedance/sonic** (Fiber custom codec) | JIT/SIMD codec, amd64+arm64; goccy/go-json fallback |
| Redis client | **rueidis** | auto-pipelining (~14× go-redis throughput), server-assisted client-side caching |
| PG driver | **pgx/v5** + sqlc `pgx` mode | binary protocol, statement caching, `CopyFrom` for audit batch inserts |
| Protobuf | **vtprotobuf** buf plugin | pooled, allocation-free marshal/unmarshal on hot paths |
| Logging | **zap** (required) | sampling + `zapcore` custom encoder; near-zero-alloc |
| Broker | RabbitMQ 4 quorum queues, confirm batching | durable + fast enough; streams unnecessary here |
| LB | nginx `least_conn`, keepalive upstreams | connection reuse; smarter than round-robin |
| Frontend | Next.js 16, Turbopack, standalone output, RSC | smallest runtime image, fastest builds |

## 17. Versions (pinned at design time)

Go 1.26 · Fiber v3.4 · PG `18-alpine` ×2 · Redis `8-alpine` ×2 (rueidis client) ·
RabbitMQ `4-management-alpine` · Next.js 16.3 · Node 24 · Jaeger v2 ·
grpc-gateway v2.29 · latest stable sqlc, goose, buf, zap, otel-go, sonic,
vtprotobuf at implementation time.

## 18. SOLID mapping (explicit, for the tour/README)

- **S**: one package per concern (`retry` ≠ `circuitbreaker` ≠ `workerpool` ≠ `batcher` ≠ `bulkhead`).
- **O**: new operations = registry entry + latency config; new audit event types = enum entry — no scheduler/ingester edits.
- **L**: all `usecase` deps are interfaces; fake/real repos interchangeable in tests.
- **I**: narrow interfaces (`TaskPublisher` publishes; it cannot consume; `AuditSink` writes; it cannot query).
- **D**: usecases own their interfaces; `internal/app` injects implementations.
