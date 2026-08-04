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

### Approved decisions

- Per-operation DAG decomposition (each binary op = one distributed task).
- Separate `agent` service with auto-scaling goroutine worker pool.
- SSE (fed by Redis pub/sub) for live dashboard updates.
- Configurable artificial per-operation latency (demo visibility; `0` for benchmarks).
- Topology "Approach A": two Go services, grpc-gateway in-process, nginx `least_conn`
  over N orchestrator replicas, RabbitMQ as the task distribution backbone.

## The plan

Full design spec: [docs/superpowers/specs/2026-08-04-distributed-expression-calculator-design.md](docs/superpowers/specs/2026-08-04-distributed-expression-calculator-design.md)

### System in one paragraph

nginx (`least_conn`) load-balances N stateless **orchestrator** replicas (gRPC +
in-process grpc-gateway HTTP). Submitting an expression validates it
(`pkg/validator`), parses it to an AST, flattens it to a task DAG, and persists
expression + tasks + outbox rows in one sqlc/PG18 transaction. An outbox relay
publishes ready tasks to RabbitMQ (fan-out, publisher confirms, quorum queues).
M **agent** replicas consume into an auto-scaling goroutine worker pool
(atomics-driven, scales on queue depth), compute single operations with
configurable demo latency, and publish results. The orchestrator's result
consumer (fan-in) records results idempotently, unlocks dependent tasks, and
publishes every state change to Redis pub/sub, which feeds SSE to a Next.js 16
shadcn/ui dark dashboard that animates the DAG, the worker fleet, and — via
OpenTelemetry from the browser through nginx, gateway, gRPC, RabbitMQ, and the
worker pool to Jaeger — the complete distributed trace of every expression,
rendered as a waterfall right in the UI. Failures ride retry queues
(TTL + DLX exponential backoff) into a DLQ; circuit breakers and jittered
retries guard all infra calls; everything ships as Alpine multi-stage images in
docker-compose with Prometheus + Grafana + otel-collector + Jaeger v2, and CI
regenerates buf/sqlc code with drift checks, lints, races the tests, and builds
all images.

### Build phases

1. **Foundations** — repo scaffold (evrone layout), `pkg/logger` (zap custom
   design), `pkg/apperrors`, `pkg/constants`, `config/`, Makefile.
2. **Domain** — entities, validator, parser (shunting-yard → AST), DAG planner;
   full unit coverage.
3. **Persistence** — goose migrations, sqlc queries, PG repo adapters, outbox.
4. **Messaging** — `pkg/rabbitmq` (topology, confirms, consumers), retry
   queue + DLQ wiring, `pkg/retry`, `pkg/circuitbreaker`.
5. **Services** — `pkg/workerpool` + agent; scheduler usecases + fan-in;
   `internal/app` lifecycles and graceful shutdown.
6. **API** — proto, buf, grpc-gateway, SSE, health; Redis cache + pub/sub.
7. **Observability** — `pkg/otel`, Prometheus metrics, Grafana dashboards,
   trace_id persistence, log correlation.
8. **Frontend** — Next.js 16 + shadcn/ui dark: dashboard, DAG view, trace
   waterfall, workers page, tour; browser + server OTel.
9. **Deployment** — Dockerfiles (alpine, multi-stage, non-root), compose stack,
   nginx LB + routes.
10. **CI & polish** — GitHub Actions (generate/drift, lint, test -race, build),
    integration + E2E tests, README with diagrams, final tour content.

Detailed step-by-step implementation plan: `docs/superpowers/plans/` (next step).
