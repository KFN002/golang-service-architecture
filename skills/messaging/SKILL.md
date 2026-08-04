---
name: messaging
description: Use when designing broker-backed communication — AMQP topology, delivery guarantees, publisher confirms, consumer ack policy, poison handling, DLQ operations, and trace context through queues. Grounded in this repo's RabbitMQ layer.
---

# Broker Engineering — RabbitMQ done carefully

`backend/pkg/rabbitmq` is ~500 lines that carry every message in this system: tasks out,
results back, audit events sideways — with confirmed publishes, classified retries, DLQs you
can operate, and traces that survive the hop. This skill is the reasoning behind each line.

## Overview

The broker is the *only* component in this system allowed to hold in-flight work during a
failure. That's a compliment: quorum queues replicate to disk; your process doesn't. Design
accordingly — the broker is the durable buffer, your services are stateless movers, and
every guarantee is spelled out in ack/confirm semantics, not vibes.

## The rules

1. **Declare topology idempotently at every startup.** Any service can boot first; nobody
   depends on a provisioning script. WHY: `ExchangeDeclare`/`QueueDeclare` are no-ops when
   config matches — free convergence.
2. **Publisher confirms on everything that matters.** A publish without a confirm is a
   *maybe*. WHY: TCP accepting bytes ≠ broker owning the message.
3. **Manual acks, always.** Auto-ack means "lose messages on crash" as a feature. WHY: the
   ack is your transaction commit for consumption.
4. **The ack decision is a function of error *class*.** nil→ack; retryable→park with
   backoff; permanent→DLQ. One dispatch function, no per-consumer improvisation.
5. **Per-message TTL + DLX is the retry scheduler.** No timer goroutines, no cron. WHY: the
   broker already has a clock and durability; use them.
6. **Prefetch is your concurrency contract with the broker.** It bounds unacked in-flight
   work per consumer — set it deliberately, in one place.
7. **Trace context rides message headers.** A queue hop that breaks the trace makes the
   trace worthless — the whole point is seeing *across* the async boundary.

## Topology: one shape for every flow

`backend/pkg/rabbitmq/rabbitmq.go` — the package doc is the diagram:

```go
// Topology per logical flow F (declared idempotently at startup):
//
//	F.ex        direct exchange — producers publish here
//	F.q         quorum queue    — consumers read here; poison → F.dlq.ex
//	F.retry.ex  direct exchange — transient failures are republished here
//	F.retry.q   queue whose DLX routes back to F.ex; per-message TTL
//	            implements exponential backoff without any timer process
//	F.dlq.ex    direct exchange → F.dlq — the dead-letter parking lot
```

Three flows (`tasks`, `results`, `audit`) instantiate it via one `Flow` struct and
`DeclareFlow`. Names derive mechanically (`.retry`, `.dlq` suffixes from `pkg/constants`) —
you can read a queue name in the management UI and know exactly what it is. The main queue's
own DLX points at the DLQ so *broker-rejected* messages (malformed, oversized) can't vanish
either:

```go
	if _, err := ch.QueueDeclare(f.Queue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    f.dlqExchange(),
		"x-dead-letter-routing-key": f.RoutingKey,
	}); err != nil {
```

Quorum queues (Raft-replicated) over classic mirrored: predictable failover semantics and
they're the direction RabbitMQ itself points. Lazy/classic queues only for throwaway data.

## Publishing: what a nil error actually means

```go
// Publish sends persistently and waits for the broker confirm — the message
// is on disk (quorum-replicated) before this returns nil.
func (p *Publisher) Publish(ctx context.Context, exchange, key string, msg Message) error {
	...
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key,
		true, // mandatory: unroutable → returned, not silently dropped
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			...
		})
	...
	ok, err := conf.WaitContext(ctx)
```

Unpack the flags, because each is a guarantee someone will one day depend on:
- `DeliveryMode: Persistent` — survives broker restart (paired with durable queues)
- `mandatory: true` — an unroutable message (typo'd routing key, missing binding) comes
  *back* instead of evaporating silently
- confirm wait — nil return ⇒ the quorum has it. This is what lets the outbox relay mark
  rows published.

The publisher holds one confirming channel behind a mutex and reopens lazily on breakage —
channels are cheap but not free; per-publish channel churn shows up under load.

## Consuming: the dispatch decision tree

All consumer policy lives in one function. Handlers return classified errors; `dispatch`
does the rest:

```go
	err := handler(ctx, del)
	switch {
	case err == nil:
		_ = d.Ack(false)

	case apperrors.IsRetryable(err) && del.Attempt+1 < cfg.MaxAttempts:
		// park a copy with backoff TTL, ack the original
		if pubErr := pub.PublishRetry(ctx, cfg.Flow, msg, cfg.Backoff(del.Attempt)); pubErr != nil {
			// Could not park a retry copy — nack with requeue so the broker
			// redelivers; better duplicate handling than message loss.
			_ = d.Nack(false, true)
			return
		}
		_ = d.Ack(false)

	default:
		// permanent or attempts exhausted → DLQ, then ack
```

Why republish-to-retry-queue instead of `Nack(requeue=true)`: a requeued message goes back
to the queue *head* — it retries immediately (hot loop against a down dependency) and blocks
everything behind it. The retry queue gives you delay + backoff + attempt counting via the
`x-attempt` header. Requeue is reserved for exactly one case: *we couldn't even park the
copy* — where a duplicate beats a loss.

Concurrency: the `Async` hook lets the agent dispatch deliveries into its worker pool while
prefetch bounds total in-flight (see the concurrency skill for the full backpressure chain).
Reconnects: the client watches `NotifyClose` and re-dials with backoff; consumer loops
re-establish channels — transient broker restarts are a log line, not an incident.

## Delivery guarantees: do the math once

```
confirmed publish (≥once out) + durable quorum queue + manual ack (≥once in)
    ⇒ at-least-once end to end
at-least-once + idempotent consumer
    ⇒ exactly-once EFFECT
```

Exactly-once *delivery* is not on the menu across process crashes — stop chasing it.
This repo's idempotency (per consumer type):

| Consumer | Idempotency mechanism |
|---|---|
| Results fan-in | `CompleteTask ... WHERE status NOT IN ('done','failed')` — duplicate finds no row |
| Audit ingest | Redis SETNX fast path (post-flush) + PG `ON CONFLICT DO NOTHING` backstop |
| Task compute | Safe to redo: result publish is itself idempotent downstream |

**Verify:** `TestAuditFullPipeline` publishes every event twice; exactly N rows land.
`pipeline_test.go` replays a result; zero new events emitted.

## The DLQ is an operator surface, not a landfill

Peek and redrive endpoints (`/api/v1/dlq/:flow`, `/api/v1/dlq/:flow/requeue`) are part of
the *system*, not an afterthought — failure handling you can't observe and reverse isn't
handling.

**A bug we actually shipped and caught:** the first `InspectDLQ` did `Get` → record →
`Nack(requeue=true)` per message. A requeued message returns to the queue *head* — so the
loop read the same message up to `limit` times. The integration test asserted "DLQ has 1
message" and got 10. The fix — hold deliveries unacked during the peek, then one batched
nack at the end:

```go
	// Deliveries stay unacked during the peek — an immediate per-message
	// nack(requeue) would put the message back at the head and make this
	// loop read it again. One batched nack at the end requeues everything.
	...
	if lastTag > 0 {
		_ = ch.Nack(lastTag, true, true) // multiple=true: requeue the whole peek
	}
```

Two lessons: (1) queue semantics are precise — "requeue" means *head*, not "back of the
line"; (2) integration tests against a real broker catch what any mock would have blessed.

## Trace context through the broker

Headers carry the W3C context; the consumer reconstructs before touching business logic
(`backend/internal/controller/amqp/v1/consumers.go`):

```go
func (a *AgentConsumer) handle(ctx context.Context, d rabbitmq.Delivery) error {
	ctx = otel.ExtractTraceparent(ctx, d.Traceparent)
	ctx, span := otel.Tracer("agent").Start(ctx, "ComputeTask")
```

The subtle half: messages born in the *outbox* embed the traceparent **in the payload at
enqueue time** (inside the submit transaction), and the relay lifts it into the AMQP header.
We initially forgot that link — spans appeared, but as disconnected single-span traces. The
E2E smoke test (assert one trace, multiple services) is what caught it. If you run an
outbox, this embedding is mandatory; there is no ambient context at relay time.

## Message contracts

DTOs live in one package (`backend/internal/usecase/messages`), JSON-tagged, versioned by
being *additive only*:

```go
// ResultMessage is the agent's reply, consumed by the orchestrator (fan-in).
type ResultMessage struct {
	Kind         ResultKind `json:"kind"`
	TaskID       uuid.UUID  `json:"task_id"`
	ExpressionID uuid.UUID  `json:"expression_id"`
	Result       float64    `json:"result,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	WorkerID     string     `json:"worker_id"`
	Attempt      int        `json:"attempt"`
}
```

Rules: new fields optional with safe zero values; never repurpose a field; unknown fields
ignored on decode (rolling deploys mean old consumers *will* meet new messages). A `kind`
discriminator beats separate queues when messages share a lifecycle (started/ok/error all
flow through results).

## Anti-patterns seen in the wild

**Fire-and-forget publishing.**
```go
// ❌ returns before the broker owns anything
ch.Publish(exchange, key, false, false, msg)
```
Under memory pressure or a routing typo, these vanish without a trace. Confirms + mandatory,
or write down why the data is droppable.

**Auto-ack "for throughput".** `autoAck: true` moves loss from "possible on crash" to
"guaranteed on crash". If ack overhead genuinely matters, batch acks (`multiple=true`) —
don't abandon them.

**Requeue-as-retry.** `Nack(requeue=true)` on handler error = immediate hot-loop retry at
queue head, starving everything behind it. That's what the TTL+DLX loop is for.

**The unmonitored DLQ.** A DLQ nobody alerts on is silent data loss with an audit trail.
Expose depth as a metric, page on it, and have the redrive endpoint ready *before* the
incident.

**Topology by hand in the management UI.** Clicked-together exchanges die with the broker
volume and differ per environment. Code declares; environments converge.

## PR review checklist

- [ ] New flow declared via `DeclareFlow` (gets retry + DLQ for free), names from constants
- [ ] Publishes go through the confirming publisher; droppable-on-failure is written down if not
- [ ] Handler returns classified errors — no ack/nack calls inside handlers
- [ ] Max attempts + backoff explicitly configured; poison path tested with a garbage payload
- [ ] Prefetch set deliberately and consistent with the consumer's concurrency bound
- [ ] Traceparent extracted at consumer entry; embedded at outbox-enqueue for relayed messages
- [ ] DTO changes are additive; zero values safe; no field repurposing
- [ ] Integration test touches a real broker for any semantic change (get/ack/TTL behavior)

## How to verify

```bash
cd backend && go test -tags integration -run TestAuditFullPipeline ./integration/
# live poison test:
docker exec perfect-go-service-rabbitmq-1 rabbitmqadmin publish \
  exchange=audit.ex routing_key=event payload='not json {{'
curl -s localhost/api/v1/dlq/tasks | jq       # inspect
open http://localhost:15672                    # management UI: queue depths, rates
```
