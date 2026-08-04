---
name: observability
description: Use when instrumenting a distributed system — OpenTelemetry setup, trace propagation across HTTP/gRPC/queues/browsers, metrics design, structured logging with trace correlation, and health semantics. Grounded in this repo's browser-to-worker single trace.
---

# Observability — one trace from the click to the goroutine

The headline capability of this repo: submit an expression in the browser and see **one
OpenTelemetry trace** — browser span → nginx → Fiber → gateway → usecase → outbox → RabbitMQ
→ agent compute → fan-in — rendered as a waterfall *inside the product's own UI*. This skill
covers every wire that makes that true, plus the metrics and logging that complete the
three pillars. It also documents two real propagation bugs, because the failure modes teach
more than the happy path.

## The rules

1. **Propagation is the product; spans are decoration.** A broken parent link makes every
   downstream span an orphan. Test the *link*, not the span count.
2. **Every async boundary needs an explicit carry.** HTTP headers, gRPC metadata, AMQP
   headers, outbox payloads — context never crosses a boundary by accident.
3. **Persist the trace ID on the domain object.** That's what turns traces from a debug tool
   into a product feature.
4. **Logs carry trace_id/span_id or they're not correlated — they're adjacent.**
5. **Metrics read existing atomics; they don't add locks.** Instrumentation must be free
   at the hot path.
6. **Liveness ≠ readiness.** Conflating them turns one slow dependency into a restart storm.
7. **Sample at the edges, never in the middle.** ParentBased sampling everywhere internal,
   ratio decisions only at trace roots.

## Setup: the provider, and a version-skew landmine

`backend/pkg/otel/otel.go`:

```go
	// Schemaless attributes merge cleanly regardless of the SDK's default
	// resource schema version (a versioned semconv import here would fail
	// Merge with "conflicting Schema URL" whenever the SDK moves ahead).
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(cfg.ServiceName),
	))
	...
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
```

**War story #1:** the first deploy crash-looped all three services with
`otel: conflicting Schema URL: 1.43.0 and 1.26.0`. `resource.Default()` carried the SDK's
semconv schema; our attributes carried the (older) imported one; `Merge` refuses mismatched
URLs *at runtime*. Fix: `resource.NewSchemaless(...)` for your own attributes — merges with
any SDK default forever. This bites at *startup*, so containerized smoke tests catch it and
unit tests don't.

Also here: `Setup` returns a shutdown func that flushes the batcher — call it in the
shutdown choreography or lose the last 2 seconds of spans from every deploy.

## Propagation: every boundary, explicitly

The helpers everything uses:

```go
// InjectTraceparent serializes the current span context to a W3C traceparent
// string for transports without header maps (AMQP message fields).
func InjectTraceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ExtractTraceparent rebuilds a context from a serialized traceparent.
func ExtractTraceparent(ctx context.Context, traceparent string) context.Context {
```

The full boundary inventory for this system — each one is code, none is automatic:

| Boundary | Carry mechanism | Where |
|---|---|---|
| browser → nginx → Fiber | `traceparent` HTTP header (fetch auto-instrumentation) | `frontend/lib/otel-client.ts` |
| Fiber middleware → handlers | span in Fiber ctx | `internal/controller/http/v1/middleware.go` |
| gateway → in-process gRPC server | **metadata matcher + explicit extract** | see war story #2 |
| usecase → outbox → relay → AMQP | traceparent **embedded in payload**, lifted to header | `repo/persistent` + scheduler relay |
| AMQP → consumer handler | header → `ExtractTraceparent` at handler entry | `controller/amqp/v1` |
| result → fan-in | same header carry on the results flow | scheduler `HandleResult` |

**War story #2:** the first E2E trace had *one span*. Two breaks, found in one debugging
session. (a) The in-process grpc-gateway (`RegisterExpressionServiceHandlerServer`) bypasses
interceptors AND the Fiber middleware's context — the fix is a gateway header matcher that
forwards `traceparent` into gRPC metadata, plus explicit extraction in the server
(`internal/controller/grpc/v1/server.go`):

```go
// traceCtx joins the caller's W3C trace when the in-process gateway forwarded
// a traceparent through gRPC metadata (interceptors do not run on this path).
func traceCtx(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if tp := md.Get("traceparent"); len(tp) > 0 {
			return otel.ExtractTraceparent(ctx, tp[0])
		}
	}
	return ctx
}
```

(b) Outbox messages had no context at relay time (the relay runs in a background loop — no
ambient request ctx exists). Fix: embed at *enqueue* time, inside the submit transaction
(`internal/repo/persistent/repo.go`):

```go
		// Embedded so the relay can stamp the AMQP header and the agent's
		// span joins the submitting request's trace.
		Traceparent: otel.InjectTraceparent(ctx),
```

The general law: **middleware you didn't write has paths you didn't instrument.** Verify
propagation with an end-to-end assertion (one trace ID, N services), not by reading code.

## Browser tracing: the trace starts at the click

`frontend/lib/otel-client.ts` — WebTracerProvider + fetch instrumentation + OTLP/HTTP:

```ts
  const provider = new WebTracerProvider({
    resource: resourceFromAttributes({ "service.name": "web-browser" }),
    spanProcessors: [
      new BatchSpanProcessor(new OTLPTraceExporter({ url: "/v1/traces" })),
    ],
  });
  ...
      new FetchInstrumentation({
        // Same-origin only: never leak trace headers to third parties.
        propagateTraceHeaderCorsUrls: [],
```

Design decisions worth stealing:
- **Export through your own origin** (`/v1/traces` → nginx → collector). The collector stays
  on the private network; the browser needs no CORS exception; rate limiting applies
  (`limit_req zone=traces`).
- **Empty `propagateTraceHeaderCorsUrls`** — trace headers leak timing and IDs; they go to
  your API only.
- User actions get explicit root spans (`withSpan("ui.submit_expression", ...)`) — "the
  click" becomes a real, named node, not an anonymous fetch.

## Persisted trace IDs: traces as a product feature

The submit path stamps the row (`expressions.trace_id`); the API returns it; the UI's Trace
tab fetches `/jaeger-api/traces/{id}` (nginx-proxied Jaeger query API) and renders the
waterfall itself (`frontend/components/trace-waterfall.tsx` — parent/child flatten, per-
service colors, queue-wait visible as gaps). Every expression is permanently linked to its
distributed execution story. Cost: one text column. This is the highest-leverage
observability feature in the repo.

## Metrics: gauges over atomics, zero hot-path cost

`backend/internal/app/metrics.go` — the pattern is `GaugeFunc`/`CounterFunc` closures over
state the code *already maintains*:

```go
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "calc", Subsystem: "pool", Name: "workers", ConstLabels: labels,
		Help: "Current goroutine workers in the pool.",
	}, func() float64 { return float64(pool.Stats().Workers) })
```

`pool.Stats()` is atomic loads. Scrape-time evaluation means the hot path pays **nothing**
for being observable. The audit pipeline exposes its whole shape the same way: accepted /
deduplicated / flushed / flush_errors counters, backlog gauge, bulkhead in-flight and
rejections — enough to reconstruct the pipeline's health from Grafana alone.

Naming discipline: `namespace_subsystem_name` (`calc_pool_workers`, `audit_flushed_total`),
`_total` suffix for counters, ConstLabels for identity (instance_id) — never for unbounded
values (no user IDs, no expression IDs: cardinality is a bill and an outage).

## Logging: correlated or it didn't happen

`backend/pkg/logger/logger.go` — two personalities (colored/emoji dev console, sampled JSON
prod) and the function that ties pillars together:

```go
// WithTrace returns a child logger annotated with the span context found in
// ctx, if any. Log lines become clickable from trace views.
func WithTrace(ctx context.Context, log *zap.Logger) *zap.Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return log
	}
	return log.With(
		zap.String("trace_id", sc.TraceID().String()),
		zap.String("span_id", sc.SpanID().String()),
	)
}
```

The HTTP access log uses it, so *every request line* carries its trace ID — grep a trace ID
across all services' logs and you get the correlated story. Prod sampling
(`NewSamplerWithOptions(core, time.Second, 100, 100)`) keeps hot-loop logging from becoming
the bottleneck it's supposed to observe.

## Health: two different questions

`/healthz` — "is the process alive?" — always 200 if serving. Restarting on this is safe.
`/readyz` — "can I do useful work?" — probes PG/Redis/RabbitMQ (`readiness(...)` helper);
LBs stop routing on failure. **Never restart on readiness**: if PG blips, restarting every
API pod turns a blip into a stampede. Health endpoints are registered *before* the rate
limiter in the middleware chain — probes must never be shed (a lesson usually learned during
an incident, encoded here in registration order).

## Anti-patterns seen in the wild

**Span soup.** Instrumenting every function: 400-span traces where nothing is findable and
the collector bill is the biggest span of all. Instrument *boundaries* (transport in/out,
storage, queue hops) and *decisions* (retry, shed, breaker). This repo: ~8 spans per
expression, each one meaningful.

**Log-and-rethrow at every layer.**
```go
// ❌ the same error, logged four times at four layers, alerted twice
if err != nil { log.Error("failed", err); return err }
```
Log where you *handle* (top of the stack, with trace ID); wrap with context everywhere else.

**The cardinality bomb.** `metric.WithLabelValues(userID)` — Prometheus eats a time series
per unique value until it eats your memory. IDs belong in traces and logs, never labels.

**Readiness that checks nothing / liveness that checks everything.** Both directions kill
you: a readyz returning static 200 routes traffic to a pod with no DB; a healthz probing PG
restart-loops the fleet during a DB failover.

**Trusting instrumentation you didn't verify end-to-end.** Our single-span trace *looked*
instrumented — every service had spans, every middleware was installed. Only the E2E
assertion ("this trace ID has spans from ≥2 services") exposed the broken links.

## PR review checklist

- [ ] New async boundary (queue, cron, outbox-like): explicit inject at write, extract at read
- [ ] New entry point (handler/consumer): span started from *extracted* context, not `context.Background()`
- [ ] Errors recorded on spans where handled; span status set on failure paths
- [ ] New long-lived component: gauge/counter funcs over its existing atomics
- [ ] No unbounded label values on any metric
- [ ] Log statements at handling sites use `logger.WithTrace(ctx, log)`
- [ ] New dependency added to `/readyz` probes — and *not* to `/healthz`
- [ ] E2E propagation assertion updated if the topology changed

## How to verify

```bash
TP="00-$(openssl rand -hex 16)-$(openssl rand -hex 8)-01"
curl -s -X POST localhost/api/v1/expressions -H "traceparent: $TP" \
  -H 'Content-Type: application/json' -d '{"raw":"(5+5)*2"}' | jq .traceId
# after ~4s (demo latencies):
curl -s "localhost/jaeger-api/traces/$(echo $TP | cut -d- -f2)" \
  | jq '[.data[0].spans[].processID] | length'   # expect ≥6 spans, 2+ services
```

If the trace ID you sent comes back on the expression and Jaeger shows multi-service spans
under it, every wire in this skill is intact.
