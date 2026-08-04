---
name: highload
description: Use when a Go system needs throughput — stack selection, batching/group commit, caching with singleflight, keyset pagination, partitioning, rate limiting, load shedding, LB strategy, and container-aware runtime tuning. Grounded in this repo's measured choices.
---

# Highload Engineering — throughput as a design property

Throughput isn't a tuning pass at the end; it's a series of structural choices. This skill
walks the choices `perfect-go-service` made, why each one is the fast option, and — just as
important — where each stops being worth it.

## Overview

The load-bearing ideas, in order of leverage:

1. **Do less per request** (in-process gateway, no loopback hops, zero-copy-ish JSON)
2. **Amortize** (batching, group commit, pipelining, connection keepalive)
3. **Don't repeat work** (cache + singleflight collapse)
4. **Make data structures match access patterns** (keyset pagination, time partitions, BRIN)
5. **Refuse excess load early and cheaply** (rate limits at three rings, shedding)
6. **Let the runtime see its true limits** (automaxprocs, GOMEMLIMIT)

## The rules

1. **Amortize per-item costs into per-batch costs wherever latency budget allows.** One
   round-trip per 500 items beats 500 round-trips, always.
2. **Collapse duplicate concurrent work.** N identical queries in flight should cost one
   execution.
3. **OFFSET pagination is O(n) per page — forbidden on hot paths.** Keyset or nothing.
4. **Unbounded anything (map, queue, buffer) is a memory-DoS.** Everything has a cap and an
   eviction story.
5. **Reject early: the cheapest work is work refused at the outermost ring.** nginx > app
   middleware > business logic — in that order.
6. **Prefer smarter distribution over more replicas.** Competing consumers and `least_conn`
   both out-schedule round-robin for free.
7. **Fast libraries are chosen once, behind a choke point.** Swapping JSON codecs must be a
   one-file change.

## The fastest-stack choices, with reasons you can defend

| Layer | Pick | The actual reason |
|---|---|---|
| HTTP | Fiber v3 (fasthttp) | avoids net/http allocation patterns; and v3's native adapter lets grpc-gateway mount **in-process** — REST→gRPC without a loopback dial |
| JSON | sonic | JIT/SIMD codec; behind `pkg/jsonx` so it's swappable in one file |
| Redis | rueidis | **auto-pipelining**: concurrent commands share round-trips transparently (~14× go-redis in contended benchmarks); server-assisted client-side caching |
| PG driver | pgx/v5 + sqlc | binary protocol, statement cache, `CopyFrom`, and compile-time-typed queries |
| Protobuf | vtprotobuf | generated pooled marshal/unmarshal — no reflection on hot paths |
| Broker | RabbitMQ quorum + confirms | competing consumers = work-stealing distribution with zero scheduler code |
| LB | nginx `least_conn` + keepalive | connection reuse kills TCP handshake overhead; least_conn absorbs uneven request costs |

The choke-point idiom (`backend/pkg/jsonx/jsonx.go`) — the entire codebase's JSON runs
through two functions:

```go
var cfg = sonic.ConfigDefault // std-compatible semantics

func Marshal(v any) ([]byte, error)   { return cfg.Marshal(v) }
func Unmarshal(data []byte, v any) error { return cfg.Unmarshal(data, v) }
```

Fiber is configured with these (`JSONEncoder: jsonx.Marshal`), DTOs use them, the wire uses
them. Want to benchmark a different codec? Change one file. **Every "fastest X" decision needs this shape
or it calcifies.**

## Group commit: the audit write path

**Problem:** 10k events/sec × one INSERT each = 10k WAL flushes = your ceiling is the disk's
fsync rate.

**Solution:** micro-batch (size/interval double-buffered batcher — see the concurrency
skill), then ONE statement per batch, idempotent so retries are free
(`backend/db/audit/queries/events.sql`):

```sql
-- unnest-based multi-row insert with conflict skip: near-COPY throughput AND
-- exactly-once effect under at-least-once delivery (retried batches no-op).
INSERT INTO audit_events (
    id, occurred_at, event_type, service,
    entity_type, entity_id, trace_id, actor, payload
)
SELECT unnest($1::uuid[]), unnest($2::timestamptz[]), unnest($3::text[]),
       unnest($4::text[]), unnest($5::text[]), unnest($6::text[]),
       unnest($7::text[]), unnest($8::text[]), unnest($9::jsonb[])
ON CONFLICT DO NOTHING;
```

Why unnest over `CopyFrom`: COPY is marginally faster but can't do `ON CONFLICT` — and
idempotency under redelivery is worth more than the margin. Why not multi-VALUES: unnest
keeps one prepared statement regardless of batch size (VALUES lists explode the statement
cache with per-size variants).

Decision table:

| Write pattern | Use |
|---|---|
| Must dedupe on retry (at-least-once source) | unnest + ON CONFLICT ← this repo |
| Bulk backfill, source is exactly-once | `pgx.CopyFrom` |
| < ~50 rows, no dedup need | multi-row VALUES via sqlc `:batchexec` |
| Single row, transactional with other writes | plain INSERT in the tx |

**Verify:** `TestAuditFullPipeline` — 400 publishes (each event twice) land as *exactly* 200
rows, throughput riding batches the whole way.

## Caching: singleflight + short TTL + server-assisted invalidation

**Problem:** a dashboard polling every second from 50 open tabs = 50 identical queries/sec
against your biggest table.

**Solution** (`backend/internal/usecase/audit/query.go`) — three layers, cheapest first:

```go
	if data, ok := q.cache.Get(ctx, key); ok { ... return cached ... }

	v, err, _ := q.sf.Do(key, func() (any, error) {
		events, err := q.store.Query(ctx, f)
		...
		q.cache.Set(ctx, key, data, q.ttl)
		return events, nil
	})
```

- **rueidis client-side caching** (`DoCache`): repeat hot reads are served from *process
  memory*, invalidated by the Redis server push — no polling, no staleness guessing.
- **singleflight**: while one query executes, identical concurrent callers wait for *its*
  result instead of dogpiling the DB. The cache-miss stampede is the thing that kills you
  at load; this is the 10-line fix.
- **short TTL (3s)**: dashboards tolerate 3 seconds of staleness; your DB tolerates 50× less
  load. Pick TTLs from *product* staleness budgets, not folklore.

Cache keys are hashes of the *whole filter struct* (`sha256(jsonx.Marshal(filter))`) — never
hand-concatenated strings that miss a field and serve wrong data.

## Data layout: keyset pagination + time partitioning + BRIN

**OFFSET is a lie.** `LIMIT 50 OFFSET 100000` reads and discards 100k rows. Keyset reads 50
(`backend/db/audit/queries/events.sql`):

```sql
  AND (sqlc.narg('cursor_ts')::timestamptz IS NULL
       OR (occurred_at, id) < (sqlc.narg('cursor_ts')::timestamptz, sqlc.narg('cursor_id')::uuid))
ORDER BY occurred_at DESC, id DESC
LIMIT sqlc.arg('page_size');
```

The compound cursor `(occurred_at, id)` breaks timestamp ties — without the `id` tiebreaker,
two events in the same microsecond make pages skip or repeat rows. The integration test
walks the full set asserting *no gaps, no overlaps* — write that test for every keyset
implementation; the tie bug is invisible in manual testing.

**Partitioning** (`backend/db/audit/migrations/00001_init.sql`): daily range partitions,
pre-created ahead by a maintainer goroutine so midnight never blocks an insert; BRIN on the
time column:

```sql
-- BRIN suits append-only time-ordered data: tiny index, fast range scans.
CREATE INDEX idx_audit_occurred_brin ON audit_events USING brin (occurred_at);
```

BRIN vs B-tree on append-only time series: the BRIN is ~1000× smaller and stays hot in
cache, because physically-ordered data lets min/max-per-block ranges do the work. Retention
becomes `DETACH PARTITION` (instant, no vacuum debt) instead of `DELETE WHERE` (a fire).

## Refusing load: three rings + shedding

Ring 1 — nginx, before your process spends anything (`deploy/config/nginx.conf`):

```nginx
    limit_req_zone  $binary_remote_addr zone=api:10m   rate=50r/s;
    limit_conn_zone $binary_remote_addr zone=perip:10m;
    ...
    location /api/ {
        limit_req zone=api burst=100 nodelay;
        limit_conn perip 50;
        proxy_pass http://orchestrators;
    }
```

Ring 2 — per-IP token buckets in the app (`backend/pkg/ratelimit/ratelimit.go`). The
interesting engineering is *bounding the limiter itself* — lazy refill (no background
goroutine per bucket) and eviction sweeps (a keyed limiter without eviction is a memory-DoS
via key churn):

```go
	// Lazy refill.
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * l.cfg.Rate
	if b.tokens > l.cfg.Burst {
		b.tokens = l.cfg.Burst
	}
```

32-way sharded map so contention never serializes the hot path. Ring 3 — the gRPC
interceptor, same limiter, catching direct-gRPC callers.

**Shedding order is a product decision encoded in code:** the audit service sheds *reads
before writes* (writes are the durability contract; a dashboard can wait). `Bulkhead.Do`'s
fail-fast + `RESOURCE_EXHAUSTED` + `Retry-After` tells well-behaved clients exactly what to
do. Degrade *predictably*, never collapse.

## Distribution: stop writing schedulers

Two "free" schedulers this repo leans on:

- **Competing consumers**: N agents consume one quorum queue; prefetch bounds per-agent
  in-flight; a fast agent naturally takes more. This is work-stealing with zero code —
  the E2E test showed tasks of one expression split across both agents on the first try.
- **`least_conn` + keepalive upstreams** at nginx: connection counts proxy for in-flight
  cost, absorbing uneven request weights that round-robin ignores.

Add stateless replicas + `FOR UPDATE SKIP LOCKED` for DB-side work claiming and you have
horizontal scaling with no coordinator, no leader election, no gossip. Boring wins.

## Runtime: tell Go the truth about its container

`backend/cmd/*/main.go`:

```go
	// Sets GOMAXPROCS from the container CPU quota — without it a limited
	// container schedules against the host core count and thrashes.
	_ "go.uber.org/automaxprocs"
```

`deploy/docker-compose.yml`:

```yaml
  environment: &go-env
    # Soft memory ceiling below the container limit: the GC works with the
    # quota instead of discovering it via OOM-kill.
    GOMEMLIMIT: 224MiB
  deploy:
    resources:
      limits: { cpus: "1.0", memory: 256M }
```

Without these: GOMAXPROCS=16 in a 1-CPU container means 16 OS threads fighting for one core
(scheduler thrash, latency spikes); and the GC targeting 2× live heap walks straight into
the OOM killer. Two lines, real tail-latency impact. Keep `GOMEMLIMIT` ~10-15% under the
hard limit so spikes have headroom.

Also config-gated: pprof on a localhost-only port (`internal/app/runtime.go`) — profiling
in prod is how you find the *actual* hot path instead of the one you guessed.

## Anti-patterns seen in the wild

**The benchmark-free "optimization".**
```go
// ❌ "strings.Builder is faster" — inside a handler that then does 3 DB queries
```
Profile first (`make` exposes pprof). The wins in this repo are structural (batching,
collapsing, refusing) — micro-optimizations without a flame graph are cosplay.

**Cache without collapse.** TTL cache alone still dogpiles: when the hot key expires, every
concurrent request misses *together* and stampedes the DB. Singleflight is not optional at
load; it's the other half of caching.

**The unbounded keyed map.** Rate limiter / session store / dedup cache keyed by client
input with no eviction. Attackers rotate keys; you OOM. Every keyed structure here has TTL
sweeps (`ratelimit`), server-driven eviction (rueidis), or PG as the bounded owner.

**Round-robin everything.** Round-robin over replicas with 100:1 request cost variance
starves randomly. Prefer least_conn (edge), competing consumers (queues), SKIP LOCKED (DB).

**Scaling before profiling.** 3× replicas to fix a hot loop that singleflight would have
erased. Replicas multiply *cost*; they only multiply *throughput* when the bottleneck is
actually CPU-per-replica.

## PR review checklist

- [ ] New hot-path query: keyset-paginated, compound cursor with tiebreaker, walk-test exists
- [ ] New high-volume write: batched (unnest/COPY decision recorded), idempotent under retry
- [ ] New cache: TTL justified by product staleness budget; singleflight on misses; key covers the full input
- [ ] New keyed in-memory structure: bounded (TTL/LRU/sweep) — state the bound in a comment
- [ ] New endpoint: covered by all three throttle rings (nginx zone, app limiter, gRPC if applicable)
- [ ] Shedding behavior explicit: what gets refused first, what status/header the client sees
- [ ] Compose limits + `GOMEMLIMIT` set for any new service
- [ ] "Faster X" claims: flame graph or benchmark in the PR description

## How to verify

```bash
# saturate and watch the rings + autoscaling do their jobs
for i in $(seq 1 200); do
  curl -s -o /dev/null -X POST localhost/api/v1/expressions \
    -H 'Content-Type: application/json' -d '{"raw":"(1+2)*(3+4)"}' &
done; wait
open http://localhost/workers        # pools scale up, then retire
open http://localhost:3001           # Grafana: batch sizes, shed counts, throughput
```

Expected: some 429s (the rings working), pool growth to Max, audit ingest riding large
batches, zero errors in the DLQ. If you see none of the 429s, your load test is too polite
to learn anything from.
