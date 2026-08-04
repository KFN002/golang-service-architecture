// The guided tour: how this system works and how to build one like it.
// Server component — pure content.

import { Card } from "@/components/ui";

const steps = [
  {
    n: "01",
    title: "The click is the trace root",
    body: `When you press Compute, the browser's OpenTelemetry SDK opens a span
    (ui.submit_expression) and injects a W3C traceparent header into the fetch.
    Everything that happens next — in six services — joins that one trace.`,
    file: "web/lib/otel-client.ts",
  },
  {
    n: "02",
    title: "nginx spreads the load",
    body: `nginx terminates the request and picks the least-connected of three
    stateless orchestrator replicas. Rate limits (limit_req) and connection caps
    are enforced here first — the outermost ring of defense in depth.`,
    file: "deploy/nginx/nginx.conf",
  },
  {
    n: "03",
    title: "Fiber + grpc-gateway, one binary",
    body: `The REST call lands on a Fiber v3 server (fasthttp) which hosts the
    grpc-gateway mux in-process — no loopback hop. Validation rejects garbage
    (pkg/validator), a shunting-yard parser builds an AST, and the planner
    flattens it into a DAG of single-operation tasks.`,
    file: "internal/usecase/expression/",
  },
  {
    n: "04",
    title: "The transactional outbox",
    body: `Expression, tasks and outgoing messages commit in ONE PostgreSQL
    transaction. A relay loop claims outbox rows with FOR UPDATE SKIP LOCKED
    (replicas share the work without stepping on each other) and publishes them
    to RabbitMQ with publisher confirms. DB state and broker can never diverge.`,
    file: "internal/usecase/scheduler/",
  },
  {
    n: "05",
    title: "Fan-out to the worker fleet",
    body: `Ready tasks land on a quorum queue. Agent replicas compete for them
    (competing consumers — distribution smarter than any round-robin). Inside
    each agent an auto-scaling goroutine pool grows with backlog and shrinks
    when idle; every resize is an audit event you can watch on /workers.`,
    file: "pkg/workerpool/",
  },
  {
    n: "06",
    title: "Fan-in unlocks the DAG",
    body: `Results return on the results queue. The orchestrator applies each
    idempotently (duplicates from at-least-once delivery are no-ops), fills the
    value into dependent tasks, decrements their dependency counters, and
    enqueues newly-ready operations — until the root task completes the
    expression. Every state change streams to this dashboard over Redis
    pub/sub → SSE.`,
    file: "internal/repo/persistent/repo.go",
  },
  {
    n: "07",
    title: "When things go wrong (on purpose)",
    body: `Transient failures ride a TTL + dead-letter-exchange retry loop with
    exponential backoff; poison messages park in a DLQ you can inspect and
    redrive over the API. Circuit breakers fail fast when a dependency dies;
    retries are jittered so replicas never stampede. Try dividing by zero —
    a permanent, typed failure that marks the expression failed instantly.`,
    file: "pkg/circuitbreaker/ · pkg/retry/ · pkg/rabbitmq/",
  },
  {
    n: "08",
    title: "The audit service never forgets",
    body: `Every event also flows to a separate microservice with its own
    PostgreSQL and Redis. Ingestion is micro-batched (double-buffered, group
    committed with unnest + ON CONFLICT), deduplicated, bulkheaded, rate
    limited and load-shed. Storage is daily-partitioned and immutable — an
    UPDATE is rejected by trigger. That is an append-only design.`,
    file: "internal/usecase/audit/",
  },
  {
    n: "09",
    title: "See it, measure it, prove it",
    body: `Jaeger shows the full distributed trace (the Trace tab renders it
    right here). Prometheus scrapes every service; Grafana dashboards ship
    pre-provisioned. Logs are zap with trace_id on every line — click from a
    log to its trace. The integration suite runs the real thing in containers:
    the full audit pipeline test asserts dedup, immutability, DLQ routing and
    keyset pagination against real PG18 + Redis + RabbitMQ.`,
    file: "integration/",
  },
];

export default function TourPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <header className="space-y-2">
        <h1 className="text-2xl font-bold">How this works</h1>
        <p className="text-sm text-muted">
          Follow one addition through six services. Each step names the code that does it —
          the whole point of this project is that you can go read it.
        </p>
      </header>

      <Card className="overflow-x-auto">
        <pre className="text-xs leading-relaxed text-muted">
{`browser ──▶ nginx ──▶ orchestrator ×3 ──▶ PostgreSQL 18 (DAG + outbox)
   │                        │
   │ OTLP                   ├──▶ RabbitMQ 4 ──▶ agent ×N (worker pools)
   ▼                        │        │              │
Jaeger ◀── otel-collector ◀─┘        │   results    │
                                     ◀──────────────┘
              audit ×2 ◀── audit.exchange (own PG18 + own Redis, append-only)
              Redis 8 ── pub/sub ──▶ SSE ──▶ this dashboard`}
        </pre>
      </Card>

      <div className="space-y-4">
        {steps.map((s) => (
          <Card key={s.n} className="flex gap-4">
            <span className="font-mono text-2xl font-black text-accent">{s.n}</span>
            <div className="space-y-1">
              <h2 className="font-semibold">{s.title}</h2>
              <p className="text-sm leading-relaxed text-muted">{s.body}</p>
              <code className="text-xs text-cyan-c">{s.file}</code>
            </div>
          </Card>
        ))}
      </div>

      <Card className="space-y-2">
        <h2 className="font-semibold">Try it yourself</h2>
        <ul className="list-inside list-disc space-y-1 text-sm text-muted">
          <li>Submit <code className="text-accent">((1+2)*(3+4))/7</code> and watch the DAG light up in waves.</li>
          <li>Open the Trace tab — find the queue-wait gaps between publish and compute.</li>
          <li>Submit <code className="text-accent">1/0</code> — a typed permanent failure, no retries.</li>
          <li>Submit twenty expressions fast and watch /workers autoscale the pools.</li>
          <li>Kill an agent container mid-computation — the task retries on another replica.</li>
          <li>Try to UPDATE a row in audit PG — the trigger will refuse.</li>
        </ul>
      </Card>
    </div>
  );
}
