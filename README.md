# Σ distributed.calc — a pinnacle-grade Go backend showcase

A distributed calculator that treats `(2 + 2) * 3` the way a real highload
system treats real work: parsed into a DAG, fanned out over RabbitMQ to
auto-scaling worker fleets, fanned back in through a transactional outbox,
audited immutably in a separate microservice, and traced — **from the click in
your browser to the goroutine that adds the numbers** — in one OpenTelemetry
trace you can watch on the dashboard.

Built on the clean-architecture layering of
[evrone/go-clean-template](https://github.com/evrone/go-clean-template).

```
browser ──▶ nginx (least_conn, limit_req) ──▶ orchestrator ×3 ─▶ PostgreSQL 18 (DAG + outbox)
   │              │                                │
   │ OTLP/HTTP    │                                ├─▶ RabbitMQ 4 ─▶ agent ×N (auto-scaling pools)
   ▼              ▼                                │      │ results        │
Jaeger ◀── otel-collector ◀── every service ───────┘      ◀────────────────┘
                                                   │
                   audit ×2 ◀── audit.exchange ────┘   (own PG18 + own Redis, append-only)
                   Redis 8 ── pub/sub ─▶ SSE ─▶ Next.js 16 dashboard (dark, shadcn-style)
```

## Quickstart

```bash
cp .env.example .env      # defaults work out of the box
make up                   # builds 4 images, boots 17 containers
open http://localhost     # the dashboard
```

Submit `((1+2)*(3+4))/7`, click the expression, and watch:

- **Task graph** — DAG nodes light amber while computing, green when done,
  edges draw in as dependencies resolve.
- **Distributed trace** — the same expression as a waterfall across
  `web-browser → orchestrator → rabbitmq → agent`, queue-wait visible,
  deep-linked to Jaeger.
- **/workers** — the goroutine pools autoscaling with load.
- **/audit** — every event, immutable, keyset-paginated.
- **/tour** — a guided walkthrough of how it all works, step by step.

Backstage: [Jaeger](http://localhost:16686) ·
[Grafana](http://localhost:3001) (pre-provisioned dashboard) ·
[RabbitMQ](http://localhost:15672) · Prometheus (internal).

## Why this exists

Every pattern people name-drop in system-design interviews is here, real,
small enough to read, and tested:

| Pattern | Where | Proven by |
|---|---|---|
| Clean architecture (entities → usecases → adapters) | `internal/` | dependency rule: inner layers import nothing outer |
| Fan-out / fan-in | `internal/usecase/scheduler` | `integration/pipeline_test.go` |
| Auto-scaling worker pool (atomics) | `pkg/workerpool` | race-detector unit tests |
| Transactional outbox | `db/main` + `internal/repo/persistent` | pipeline test: DB and broker never diverge |
| Retry queue (TTL + DLX backoff) | `pkg/rabbitmq` | full audit test: transient failures retried |
| Dead-letter queue + operator redrive | `pkg/rabbitmq`, `/api/v1/dlq/*` | poison message lands in DLQ, requeue endpoint |
| Circuit breaker (3-state, half-open probes) | `pkg/circuitbreaker` | state-machine unit tests |
| Jittered exponential retry | `pkg/retry` | unit tests incl. permanent-error short-circuit |
| Micro-batching / group commit (double-buffered) | `pkg/batcher` | `TestAuditFullPipeline`: 400 msgs → 200 rows |
| Bulkhead isolation | `pkg/bulkhead` | saturation sheds with `OVERLOADED` |
| Token-bucket rate limiting (sharded) | `pkg/ratelimit` + nginx `limit_req` + gRPC interceptor | three-ring defense in depth |
| Load shedding | audit gRPC write path | `RESOURCE_EXHAUSTED` + `Retry-After` |
| Idempotent consumers (exactly-once effect) | Redis SETNX fast path + PG `ON CONFLICT` backstop | dedup survives Redis loss |
| Append-only store (partitioned, immutable) | `db/audit` | trigger rejects UPDATE/DELETE in tests |
| Singleflight query collapse | `internal/usecase/audit/query.go` | concurrent identical queries → one DB hit |
| Keyset pagination | audit queries | pagination walk test: no gaps, no overlaps |
| Graceful shutdown (drain, nack, flush) | `internal/app/*` | pool drains queued tasks before exit |
| End-to-end tracing incl. browser + AMQP hops | `pkg/otel`, `web/lib/otel-client.ts` | the Trace tab |

## The fastest stack, deliberately

| Concern | Choice | Why |
|---|---|---|
| HTTP | **Fiber v3** (fasthttp) | fastest Go HTTP; native net/http adapter hosts grpc-gateway in-process |
| JSON | **bytedance/sonic** | JIT/SIMD codec wired into Fiber and all wire DTOs |
| Redis | **rueidis** | auto-pipelining (~14× go-redis), server-assisted client-side caching |
| PostgreSQL | **pgx/v5 + sqlc** | binary protocol, typed queries, `unnest` group commits |
| Protobuf | **vtprotobuf** | pooled zero-reflection marshaling |
| Logging | **zap** | custom colored/emoji dev encoder; sampled JSON in prod; `trace_id` on every line |
| Broker | RabbitMQ 4 quorum queues | publisher confirms; competing consumers = smarter-than-round-robin distribution |
| LB | nginx `least_conn` + keepalive | smarter than round-robin, connection reuse |
| Frontend | Next.js 16 (Turbopack, standalone) | smallest runtime image |

## Repository map

```
cmd/{orchestrator,agent,audit}/  thin mains
config/                          env-first configuration
internal/entity/                 pure domain (imports nothing)
internal/usecase/                business logic; owns its ports (interfaces)
internal/repo/                   PG/Redis adapters (sqlc-generated + hand-written)
internal/controller/             gRPC, AMQP, Fiber HTTP transports
internal/app/                    DI wiring + lifecycles + graceful shutdown
pkg/                             standalone, domain-free libraries — each one pattern
proto/  gen/                     buf-managed contracts + generated code (drift-checked in CI)
db/{main,audit}/                 goose migrations + sqlc queries, embedded in binaries
web/                             Next.js 16 dashboard
deploy/                          compose, nginx, prometheus, grafana, otel
integration/                     container-backed tests (the full audit test lives here)
```

## Development

```bash
make help                # every target, documented
make up-infra            # just PG×2, Redis×2, RabbitMQ, telemetry
make run-orchestrator    # then run services natively for debugging
make run-agent
make run-audit
make web-dev             # Next dev server, proxies API to localhost

make generate            # buf + sqlc codegen (CI fails if you forget)
make lint                # golangci-lint incl. gosec
make vuln                # govulncheck
make test                # unit, -race
make test-integration    # real PG18/Redis/RabbitMQ via testcontainers
```

### Try the failure modes

```bash
# Division by zero: permanent typed failure, no retry, expression fails fast.
curl -s -X POST localhost/api/v1/expressions -d '{"raw":"1/0"}' | jq

# Kill an agent mid-flight: its unacked tasks redeliver to the other replica.
docker kill perfect-go-service-agent-1-1

# Inspect and redrive the dead-letter queue:
curl -s localhost/api/v1/dlq/tasks | jq
curl -s -X POST localhost/api/v1/dlq/tasks/requeue | jq

# Prove the audit log is immutable:
docker exec -it perfect-go-service-postgres-audit-1 \
  psql -U audit -c "UPDATE audit_events SET actor='tamper'"
# ERROR:  audit_events is append-only: UPDATE rejected
```

## Security posture

- **Three throttle rings**: nginx `limit_req`/`limit_conn` → per-IP token
  buckets in Fiber → per-client gRPC interceptor.
- Strict allow-list input validation, UUID validation, 64 KiB body caps,
  full timeout coverage (read/write/idle/shutdown).
- Security headers on every response; SSE and OTLP endpoints specifically
  hardened; browser trace headers never leak cross-origin.
- Containers: scratch images, non-root UID, `no-new-privileges`, read-only
  rootfs, segmented networks (DBs unreachable from the edge), localhost-only
  ops UIs, resource limits on every service.
- CI: gosec on every lint run + govulncheck against the Go vuln DB.
- SQL injection impossible by construction: 100% sqlc-generated parameterized
  queries; the audit store physically cannot UPDATE or DELETE.

## License

Apache-2.0 — see [LICENSE](LICENSE).
