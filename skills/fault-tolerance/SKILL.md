---
name: fault-tolerance
description: Use when adding resilience to a distributed Go system — retries, circuit breakers, bulkheads, retry queues, DLQs, transactional outbox, idempotency, and graceful shutdown. Grounded in this repo's working implementations and the tests that prove them.
---

# Fault Tolerance — the full pattern stack, wired and tested

Every resilience pattern in this skill is running in `perfect-go-service` right now, guarding
real traffic between three services, and each has a test that demonstrates it working. The
core insight threading them together: **failures are not exceptional — they're an input**,
and each pattern answers one specific question about that input.

| Question | Pattern |
|---|---|
| Should I try again? | Retry with jitter + error classification |
| Should I even try? | Circuit breaker |
| Who else gets hurt if I try? | Bulkhead |
| Where does work wait while a dependency heals? | Retry queue (TTL + DLX) |
| Where does poison go? | DLQ + operator redrive |
| How do DB and broker stay in agreement? | Transactional outbox |
| What happens when delivery repeats? | Idempotent consumers |
| How do we stop without losing work? | Graceful shutdown choreography |

## The rules

1. **Classify errors at creation, not at handling.** Transient vs permanent is a domain
   decision. WHY: retry loops that string-match messages break on the first reworded error.
2. **Never retry permanent failures.** Division by zero doesn't heal. WHY: retrying it is
   pure load with zero possible benefit — you're DoSing yourself.
3. **All backoff is jittered.** WHY: synchronized retries from N replicas are a self-inflicted
   thundering herd; full jitter spreads them.
4. **Durable buffering beats in-memory buffering for must-not-lose data.** The broker *is*
   the write-ahead log. WHY: your process dies; the quorum queue doesn't.
5. **At-least-once delivery + idempotent consumer = exactly-once effect.** Chase this, not
   "exactly-once delivery" (which doesn't exist across failures).
6. **Every fault-tolerance decision must be observable.** Breaker transitions log; retries
   log with attempt counts; shed requests count in metrics. WHY: silent resilience is
   indistinguishable from silent data loss.
7. **Shutdown is a choreography with an order, not a `defer` pile.** Stop intake → drain →
   nack what's left → flush telemetry.

## Retry: jittered, classified, context-aware

`backend/pkg/retry/retry.go`:

```go
// Backoff for attempt n is rand(0, min(Cap, Base·2ⁿ)) — full jitter prevents
// synchronized retry storms across replicas (thundering herd).
func Do(ctx context.Context, cfg Config, fn func(ctx context.Context) error) error {
	cfg.defaults()
	var err error
	for attempt := range cfg.Attempts {
		if err = fn(ctx); err == nil {
			return nil
		}
		if !apperrors.IsRetryable(err) {
			return err // permanent — do not hammer
		}
		...
		ceiling := min(cfg.Base<<attempt, cfg.Cap)
		// #nosec G404 -- jitter needs speed, not cryptographic randomness.
		delay := time.Duration(rand.Int64N(int64(ceiling) + 1))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return apperrors.Wrap(apperrors.CodeUnavailable, "retry canceled", ctx.Err())
		}
```

Three things reviewers should demand in *any* retry helper:
- the permanent-error short-circuit (`IsRetryable`)
- the `ctx.Done()` arm inside the sleep (a canceled request must not keep retrying)
- **full jitter** — `rand(0, ceiling)`, not `ceiling ± ε`

`Backoff(cfg, attempt)` is exported separately: the AMQP layer reuses the same schedule for
retry-queue TTLs. One backoff policy, two enforcement points, zero drift.

**Verify:** `pkg/retry/retry_test.go` — permanent errors get exactly 1 call; cancellation
stops between attempts; caps hold.

## Circuit breaker: three states, atomics, bounded probes

**Problem:** when a dependency dies, every caller burns a timeout discovering it —
capacity evaporates into waiting.

`backend/pkg/circuitbreaker/circuitbreaker.go` — state is an atomic, transitions are CAS'd,
half-open admits a *bounded* number of concurrent probes:

```go
	case HalfOpen:
		// Admit only a bounded number of concurrent probes.
		if b.probes.Add(1) > int32(b.cfg.HalfOpenProbes) {
			b.probes.Add(-1)
			return apperrors.Newf(apperrors.CodeUnavailable, "circuit %q probing", b.name)
		}
		defer b.probes.Add(-1)
```

```go
// transition performs from → to atomically; losers of the race no-op.
func (b *Breaker) transition(from, to State) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.state.CompareAndSwap(int32(from), int32(to)) {
		return
	}
```

The CAS inside the mutex is the subtle part: many goroutines observe the trip condition
simultaneously; exactly one performs the transition (and fires the `OnChange` callback —
which feeds a log line and could feed metrics/audit). The probe *quota* matters too: classic
naive half-open lets a stampede through the moment cooldown ends and re-kills the recovering
dependency.

Where it sits: wrapped around AMQP publishing via `GuardedPublisher`
(`internal/controller/amqp/v1/publisher.go`) — retry *inside* breaker *outside*:

```go
	return retry.Do(ctx, g.retry, func(ctx context.Context) error {
		return g.breaker.Do(ctx, func(ctx context.Context) error {
			return g.pub.Publish(ctx, flow.Exchange, flow.RoutingKey, ...)
		})
	})
```

Order matters: breaker-inside-retry means an open breaker fails fast on every retry attempt
(cheap), while retry-inside-breaker would count retries as separate breaker samples and trip
it on one bad burst.

**Verify:** `circuitbreaker_test.go` covers trip threshold, cooldown → half-open promotion,
probe success closing, probe failure re-opening, and streak reset on success.

## Bulkhead: fail fast at the compartment door

`backend/pkg/bulkhead/bulkhead.go` — a counting semaphore with two admission modes:

```go
// Do runs fn if a slot is free, or fails fast with CodeOverloaded — the
// caller sheds load instead of queueing unboundedly.
func (b *Bulkhead) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	select {
	case b.slots <- struct{}{}:
		...
	default:
		b.rejected.Add(1)
		return apperrors.Newf(apperrors.CodeOverloaded, "bulkhead %q saturated", b.name)
	}
}
```

`Do` (shed) guards the audit service's **synchronous gRPC write path** — an external caller
storm can't starve internal ingestion. `DoWait` (bounded wait) guards the **AMQP path** —
internal traffic briefly queues instead of shedding, because the broker redelivers anyway.
Same primitive, two policies, chosen per caller class. That's the actual skill: bulkheads are
*policy boundaries*, not just semaphores.

## Retry queue + DLQ: backoff without timers

**Problem:** a consumer that fails a message must not spin-retry it (hot loop) nor requeue-
to-head (starves everything behind it). You want *delayed* redelivery with backoff — without
running a scheduler.

**Solution** — broker-native, `backend/pkg/rabbitmq/rabbitmq.go`. Topology per flow:

```
F.ex → F.q (quorum, DLX→F.dlq.ex)          main path
F.retry.ex → F.retry.q (DLX → F.ex)        parking lot; per-MESSAGE TTL
F.dlq.ex → F.dlq                           dead letters
```

A transient failure republishes the message to the retry queue with
`Expiration: backoff(attempt)`; when the TTL lapses, RabbitMQ's dead-letter routing returns
it to the main exchange. Attempt count rides an `x-attempt` header. The whole policy is one
dispatch function:

```go
	case apperrors.IsRetryable(err) && del.Attempt+1 < cfg.MaxAttempts:
		msg := Message{Body: d.Body, Attempt: del.Attempt + 1, ...}
		if pubErr := pub.PublishRetry(ctx, cfg.Flow, msg, cfg.Backoff(del.Attempt)); pubErr != nil {
			// Could not park a retry copy — nack with requeue so the broker
			// redelivers; better duplicate handling than message loss.
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)
	default:
		// permanent or attempts exhausted → DLQ, then ack the original
```

Note the failure-of-the-failure-path handling: if parking the retry copy fails, we prefer a
possible duplicate (nack/requeue) over a possible loss. **Write that preference down in
code comments — it's the most important line in the file.**

The DLQ is not a grave; it's a queue with an API: `GET /api/v1/dlq/:flow` peeks (batched
nack so the peek isn't destructive — see the messaging skill for the bug story),
`POST /api/v1/dlq/:flow/requeue` redrives with attempt reset.

**Verify:** `integration/audit_full_test.go` publishes a poison message ("this is not
json{{{") among 400 valid ones and asserts exactly one DLQ occupant while everything else
lands.

## Transactional outbox: the two-writes problem, solved

**Problem:** "save to DB, then publish to broker" — crash between the two and your systems
disagree forever. Wrap in a DB transaction? The broker isn't in your transaction.

**Solution** — write the *intent to publish* in the same transaction as the state
(`backend/internal/repo/persistent/repo.go`): `CreateExpression` inserts expression + tasks
+ outbox rows atomically. A relay claims and publishes:

```go
-- backend/db/main/queries/outbox.sql
-- name: SelectOutboxBatch :many
SELECT id, kind, payload FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED;
```

The relay transaction: claim (SKIP LOCKED — replicas partition work automatically) →
publish with confirms → mark only what the broker confirmed → commit. Crash anywhere ⇒
unmarked rows republish later ⇒ at-least-once ⇒ consumers dedupe. `SKIP LOCKED` is the
unsung hero: N stateless orchestrators run this loop concurrently with zero coordination
infrastructure.

**Verify:** `integration/pipeline_test.go` drains the outbox with a capture function and
asserts exact fan-out contents at each wave — including that replaying a result produces
*zero* new outbox rows.

## Idempotency: the layering that survives Redis loss

The audit pipeline's exactly-once *effect* is two layers with carefully chosen ordering
(`backend/internal/usecase/audit/ingest.go`):

```go
	// Fast-path dedup: a hit means the event is already durably stored
	// (keys are only written after successful flush), so acking is safe.
	if seen, err := i.dedup.Seen(ctx, ev.ID.String()); err == nil && seen {
		i.deduplicated.Add(1)
		return nil
	} // On dedup-store error: fall through — PG ON CONFLICT is the backstop.
```

```go
	// Mark AFTER durable success — a pre-flush mark could drop a
	// redelivered event whose first flush failed.
	i.dedup.MarkSeen(ctx, it.ev.ID.String(), constants.AuditDedupTTL)
```

The ordering argument deserves restating because getting it backwards *silently loses data*:
if you mark-seen *before* the flush and the flush fails, the redelivered message hits the
dedup filter and gets acked — the event is gone. Mark-after-durable means the worst case is
a duplicate insert attempt, which the SQL absorbs:

```sql
-- backend/db/audit/queries/events.sql
INSERT INTO audit_events (...)
SELECT unnest($1::uuid[]), ...
ON CONFLICT DO NOTHING;
```

Redis here is an *optimization*, never the source of truth. Flush Redis entirely and
correctness holds — that exact scenario is a test (`TestAuditDedupSurvivesRedisLoss`).

For the calculator flow, idempotency is state-transition-shaped instead:
`CompleteTask ... WHERE status NOT IN ('done','failed')` — a duplicate result finds no row
and becomes a no-op. Two idempotency styles; pick by whether you have natural state
transitions (use them) or append-only writes (use keys + ON CONFLICT).

## Graceful shutdown: a choreography

`backend/internal/app/audit.go`, the shutdown goroutine:

```go
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down gracefully: draining batcher")
		shCtx, cancel := context.WithTimeout(context.Background(), constants.DefaultShutdownTimeout)
		defer cancel()
		_ = app.ShutdownWithContext(shCtx)                          // 1. stop HTTP intake
		stopGRPCGracefully(grpcSrv, constants.DefaultShutdownTimeout, log) // 2. drain RPCs, bounded
		ingestor.Close()                                            // 3. final batch flush
		return nil
	})
```

And the deadline-bounded gRPC stop (`backend/internal/app/runtime.go`) — because
`GracefulStop` alone waits *unboundedly* on a stuck stream:

```go
func stopGRPCGracefully(srv *grpc.Server, deadline time.Duration, log *zap.Logger) {
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(deadline):
		log.Warn("gRPC graceful drain exceeded deadline — forcing stop")
		srv.Stop()
		<-done
	}
}
```

Order is the whole point: **intake first** (nothing new enters), **drain second** (in-flight
work completes or is nacked back to the broker), **flush last** (batched data and telemetry
leave the process). The agent adds `pool.Close()` which drains queued tasks; AMQP unacked
messages auto-requeue on connection close — the broker cleans up what the drain couldn't.

## Anti-patterns seen in the wild

**Retry-on-everything.**
```go
// ❌ retries validation errors, auth failures, division by zero…
for i := 0; i < 3; i++ { if err = call(); err == nil { break } }
```
Fix: classification first (`IsRetryable`), retries as policy around it.

**The immortal breaker.** A breaker that opens but whose half-open path was never tested —
it works fine until the first real outage, then never recloses (probe logic bug) and you're
down *after* the dependency recovered. Test the recovery path explicitly
(`TestHalfOpenRecovery`).

**In-memory retry buffers for durable data.** A slice of failed messages retried by a
goroutine = data loss on deploy. If it must survive, it lives in the broker (retry queue) or
the DB (outbox). Memory is for *retryable reads*, never must-not-lose writes.

**Shutdown by `os.Exit` after a sleep.** `time.Sleep(5s); os.Exit(0)` "usually works" and
occasionally truncates a batch mid-flush. Deterministic drain (WaitGroups, Close methods
that block, deadline-bounded stops) or it didn't happen.

## PR review checklist

- [ ] New failure path: classified via `apperrors` code; retryability is deliberate
- [ ] Retries: jittered, capped attempts, ctx-aware sleep, permanent short-circuit
- [ ] External dependency call sites: breaker-wrapped (or a written reason why not)
- [ ] New queue consumer: max attempts + DLQ route; poison (undecodable) → permanent
- [ ] Any "write to X then tell Y" sequence: outbox or documented tolerance for divergence
- [ ] Consumer handlers idempotent — replay the same message twice in a test
- [ ] Dedup marking happens after durable success, never before
- [ ] Shutdown ordering: intake → drain (bounded) → flush; nothing dropped silently
- [ ] Every shed/trip/retry emits a metric or log with enough context to act on

## How to verify

```bash
cd backend
go test -race ./pkg/retry/ ./pkg/circuitbreaker/ ./pkg/bulkhead/
make test-integration       # outbox waves, dedup, poison→DLQ, immutability
# live: kill an agent mid-computation and watch the task complete elsewhere
docker kill perfect-go-service-agent-1-1
```
