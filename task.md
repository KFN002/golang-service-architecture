# Task

## Original prompt (verbatim)

> Write a perfect scalable backend service on golang using clean architecture template from: https://github.com/usestrix/strix
>
> Use zap.logger with custom cool design, goose golang migrations, latest redis, latest pg18, latest next.js, use fan-in, fan-out, worker pool and other async patterns to write an orchestator to solve mathematical expressions (orchestrator gives async tasks to goroutines and they do it, they amount scales automatically, all visible on next.js shadcn/ui simple dark frontend) with validation in utils, seperate package for errors, constants, config. Add docker-compose, dockerfiles, optimize it all, use alpine versions. Use sqlc for db talking, grpc and a http gateway, place it all into ci pipeline, so it generates automatically. Add rabbitmq, fault tolerance patterns, like retry, curcuit breaker, retry queue, like rabbitmq or kafka. Make all strcutures and models aync with atomics or new mutexes. Make it super highload scalable solution with nginx and several server instances and round-robin or more modern scheduling and task distribution. Make it a pinnacle of golang backend developemnt, add a beautiful README, proper structure on next.js site, visualizations of work, a little instructions/tour round the project, so people understand how to do such things. All stuff must be seperate (files, models, etc), have proper names, work perfectly, follow all SOLID principles 100% and other principles. Use brainstorming and other skills and plugins to suggest the plan, save the prompt in the task.md, then add the plan there, after the prompt.

### Follow-up amendments

1. `usestrix/strix` turned out to be a Python pentesting tool, not a Go template —
   confirmed replacement: **https://github.com/evrone/go-clean-template.git**.
2. Add tracing / OpenTelemetry and other visibility items to the plan.
3. Wire tracing into the Next.js app so the full trace is visible on the dashboard itself.
4. Add an **audit-log microservice** — gRPC, its own Redis, its own DB, highload,
   append-only design, all possible async + fault-tolerance/highload patterns.
5. Use **Fiber** for router and HTTP; choose the **fastest option** for every
   technology in the project.
6. Add an explicit **Makefile** (developer entrypoint; CI reuses its targets).

### Approved decisions

- Per-operation DAG decomposition (each binary op = one distributed task).
- Separate `agent` service with auto-scaling goroutine worker pool.
- SSE (fed by Redis pub/sub) for live dashboard updates.
- Configurable artificial per-operation latency (demo visibility; `0` for benchmarks).
- Topology "Approach A", extended: three Go services — orchestrator (Fiber +
  grpc-gateway in-process), agent, audit (own PG18 + own Redis) — nginx
  `least_conn` over replicas, RabbitMQ as the distribution backbone.
- Fastest stack: Fiber v3 (fasthttp), sonic JSON, rueidis, pgx/v5 + sqlc,
  vtprotobuf, zap (sampled prod), nginx keepalive `least_conn`, Next.js 16
  Turbopack/standalone.

## The plan

Full design spec: [docs/superpowers/specs/2026-08-04-distributed-expression-calculator-design.md](docs/superpowers/specs/2026-08-04-distributed-expression-calculator-design.md)

### System in one paragraph

nginx (`least_conn`) load-balances N stateless **orchestrator** replicas (gRPC +
grpc-gateway mounted in a Fiber v3 server). Submitting an expression validates
it (`pkg/validator`), parses it to an AST, flattens it to a task DAG, and
persists expression + tasks + outbox rows in one sqlc/PG18 transaction. An
outbox relay publishes ready tasks to RabbitMQ (fan-out, publisher confirms,
quorum queues). M **agent** replicas consume into an auto-scaling goroutine
worker pool (atomics-driven, scales on queue depth), compute single operations
with configurable demo latency, and publish results. The orchestrator's result
consumer (fan-in) records results idempotently, unlocks dependent tasks, and
publishes every state change to Redis pub/sub, which feeds SSE to a Next.js 16
shadcn/ui dark dashboard that animates the DAG, the worker fleet, and — via
OpenTelemetry from the browser through nginx, gateway, gRPC, RabbitMQ, and the
worker pool to Jaeger — the complete distributed trace of every expression,
rendered as a waterfall right in the UI. In parallel, every system event flows
to the **audit** microservice (own PG18, own Redis/rueidis): bulkheaded AMQP
ingesters dedup, backpressure through bounded channels into a double-buffered
micro-batcher, and group-commit via `COPY` into daily-partitioned, INSERT-only
tables — with token-bucket rate limiting, load shedding, singleflight-cached
keyset-paginated gRPC queries, and its own gateway for the dashboard's `/audit`
page. Failures ride retry queues (TTL + DLX exponential backoff) into a DLQ;
circuit breakers and jittered retries guard all infra calls; everything ships
as Alpine multi-stage images in docker-compose with Prometheus + Grafana +
otel-collector + Jaeger v2, and CI drives the same Makefile targets developers
use: regenerate buf/sqlc code with drift checks, lint, race the tests, build
all images.

### Build phases

1. **Foundations** — repo scaffold (evrone layout), `pkg/logger` (zap custom
   design), `pkg/apperrors`, `pkg/constants`, `config/`, Makefile skeleton
   (`help`, tool pinning, target layout).
2. **Domain** — entities, validator, parser (shunting-yard → AST), DAG planner;
   full unit coverage.
3. **Persistence** — goose migrations + sqlc queries for both DBs (main +
   audit partition DDL), repo adapters, outbox, CopyFrom audit store.
4. **Messaging** — `pkg/rabbitmq` (topology, confirms, consumers), retry
   queue + DLQ wiring, `pkg/retry`, `pkg/circuitbreaker`.
5. **Concurrency kit** — `pkg/workerpool` (auto-scaling), `pkg/batcher`
   (double-buffer micro-batching), `pkg/bulkhead`, `pkg/ratelimit`.
6. **Services** — agent (pool + compute); orchestrator scheduler usecases +
   fan-in; audit ingest pipeline + query side; `internal/app` lifecycles and
   graceful shutdown for all three.
7. **API** — protos (expression + audit), buf + vtprotobuf, grpc-gateway in
   Fiber v3 (sonic codec), SSE, health; rueidis cache + pub/sub.
8. **Observability** — `pkg/otel`, Prometheus metrics, Grafana dashboards,
   trace_id persistence, log correlation, browser/Next.js OTel.
9. **Frontend** — Next.js 16 + shadcn/ui dark: dashboard, DAG view, trace
   waterfall, workers page, audit explorer, tour.
10. **Deployment** — Dockerfiles (alpine, multi-stage, non-root), compose
    stack (2× PG, 2× Redis, full telemetry), nginx LB + routes.
11. **CI & polish** — GitHub Actions reusing Makefile targets (generate/drift,
    lint, test -race, build ×4), integration + E2E tests, README with
    diagrams, final tour content.

### Status — implemented (2026-08-04)

All phases delivered and verified: unit + race suite green, all-linters
golangci-lint clean, container-backed integration suite (incl. the full audit
pipeline test) passing, full compose stack booted, and the E2E smoke test
confirmed: correct distributed results across both agents, complete audit
trail, 400/404 typed error mapping, and one browser-rooted OpenTelemetry trace
spanning orchestrator → RabbitMQ → agent with the fan-in visible.

Follow-up layout amendments applied: `backend/` (Go module + `build/` tool
configs), `frontend/` (Next.js), `deploy/` (compose + `config/` infra confs).
