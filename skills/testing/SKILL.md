---
name: testing
description: Use when designing a test strategy for a distributed Go system — table-driven units with race detection, testcontainers integration, full-pipeline guarantee tests, E2E smoke, and CI drift checks. Grounded in this repo's suite and the real bugs each tier caught.
---

# Testing Distributed Systems — a pyramid that caught real bugs

Every tier of this repo's test pyramid caught at least one genuine bug before it reached a
user. That's the standard: a test suite is measured by the bugs it intercepts, not the
coverage number it prints. This skill walks each tier, the techniques inside it, and the
specific catches that justify its existence.

## The pyramid, with receipts

| Tier | Command | Real bugs caught here |
|---|---|---|
| Unit (`-race`) | `make test` | batcher oversize batches; parser unary-minus precedence (`2*-3` = −3, not −6); planner limit never firing |
| Integration (real containers) | `make test-integration` | `InspectDLQ` re-reading the same message; `date + $1` operator ambiguity in partition DDL |
| E2E smoke (live stack) | curl scripts | gateway returning 500 for invalid input; single-span traces (broken propagation) |
| Static | `make lint` / `make vuln` / `make generate-check` | int-narrowing overflow sites; stale generated code |

Notice the pattern: each tier catches what the tier below *structurally cannot see*. Unit
tests can't see queue-head semantics; integration tests can't see nginx or the in-process
gateway path; only static analysis reads the code you didn't run.

## The rules

1. **`-race` is not optional, ever.** It's in `make test` and CI. A race caught in CI is a
   3 AM page you'll never get.
2. **Test against a reference implementation when one exists.** The strongest oracle is an
   independent, trivially-correct second implementation.
3. **Choreograph concurrency with channels; poll only for *eventual* states — always with a
   deadline that fails loudly.** `sleep(100ms); assert` is a flake generator.
4. **Integration tests run the real dependency.** Mock brokers bless code that real brokers
   reject.
5. **One big test per pipeline that asserts *guarantees*, not functions.** Five properties
   in one scenario beats five isolated scenarios that never interact.
6. **Test the replay.** At-least-once systems must prove the second delivery is a no-op.
7. **Fakes live at ports; nothing else is mockable.** If you need to mock something that
   isn't a port, your architecture is telling you something.
8. **Generated code is verified by drift check, not by tests.**

## Unit tier: oracles and choreography

**The reference-implementation oracle** — the entire distributed pipeline must agree with a
10-line recursive evaluator (`backend/internal/usecase/expression/parser_test.go`):

```go
// evalAST walks the AST directly — the reference implementation the
// distributed pipeline must agree with.
func evalAST(n *Node) float64 {
	if n.IsLeaf() {
		return n.Value
	}
	l, r := evalAST(n.Left), evalAST(n.Right)
	switch n.Op {
	case "+": return l + r
	...
}

	cases := map[string]float64{
		"2 + 2 * 3":         8,
		"2 * -3":            -6,
		"100 - 10 - 5":      85, // left associativity
		"((1+2)*(3+4))/7":   3,
	}
```

This table caught the unary-minus bug on its first run: the naive `0 - x` rewrite made
`2 * -3` parse as `(2*0) - 3 = -3`. The case list is also a *specification* — associativity
and precedence are pinned in executable form.

**Channel choreography for deterministic concurrency**
(`backend/pkg/workerpool/workerpool_test.go`) — "worker is busy" isn't a sleep, it's a fact
established by a blocking channel:

```go
	block := make(chan struct{})
	for range 32 {
		p.Submit(func(context.Context) { <-block })   // workers now provably occupied
	}
	time.Sleep(100 * time.Millisecond) // let autoscaler observe backlog
	if got := p.Stats().Workers; got != 4 {
		t.Errorf("workers = %d, want max 4", got)
	}
	close(block)
```

And for *eventual* states (scale-down), deadline-polling that fails with diagnostics:

```go
	deadline := time.After(2 * time.Second)
	for p.Stats().Workers != 1 {
		select {
		case <-deadline:
			t.Fatalf("did not scale down to Min, workers=%d", p.Stats().Workers)
		case <-time.After(10 * time.Millisecond):
		}
	}
```

The contract-catch worth remembering: `TestFlushBySize` asserted "no batch exceeds MaxSize"
and immediately exposed that fast producers could out-run the flush loop and grow a 25-item
batch against a 10 cap. The *contract as assertion* found what reading the code didn't.

## Integration tier: real containers, honest helpers

`backend/integration/containers_test.go` centralizes container lifecycle — request, wait
strategy, cleanup, connection — so each test reads as intent:

```go
	req := testcontainers.ContainerRequest{
		Image:        "postgres:18-alpine",         // the SAME image compose runs
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	...
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
```

Details that keep this tier trustworthy: `WithOccurrence(2)` (PG logs that line during init
*and* on the real start — waiting for one gives you a half-booted DB); `t.Cleanup` over
`defer` (runs even on `t.Fatal` in helpers); migrations applied through the *production*
embed path, so the tests validate the migrations too. Build tag `//go:build integration`
keeps the fast loop fast; `make test-integration` opts in.

## The genre piece: one test, five guarantees

`TestAuditFullPipeline` (`backend/integration/audit_full_test.go`) runs the production
consumer against real PG + Redis + RabbitMQ and asserts the *guarantee sheet* of the
pipeline in one scenario:

```go
// It asserts, in order:
//  1. every published event lands durably in the partitioned store,
//  2. duplicates (at-least-once redelivery) collapse to one row,
//  3. poison messages route to the DLQ without wedging the pipeline,
//  4. the store is truly append-only (UPDATE/DELETE rejected by trigger),
//  5. keyset pagination walks the full set without gaps or overlaps,
```

Selected mechanics — publish everything twice, then demand *exactness*:

```go
	for i := range unique {
		msg := messages.NewAudit(...)
		for rep := range 2 {                      // duplicate delivery, deliberately
			pub.Publish(...)
		}
	}
	...
	if total != unique {
		t.Fatalf("stored rows = %d, want exactly %d (dedup must collapse duplicates)", total, unique)
	}
```

And the pagination walk that catches cursor tie-bugs no manual test ever will:

```go
	seen := make(map[uuid.UUID]bool)
	for {
		page, _ := store.Query(ctx, filter)
		if len(page) == 0 { break }
		for _, ev := range page {
			if seen[ev.ID] {
				t.Fatalf("event %s returned twice — keyset overlap", ev.ID)
			}
			seen[ev.ID] = true
		}
		last := page[len(page)-1]
		filter.CursorTime, filter.CursorID = last.OccurredAt, last.ID
	}
```

**This tier's trophy:** the DLQ assertion (`want 1, got 10`) exposed that per-message
`Nack(requeue=true)` returns messages to the queue *head* — the peek loop kept re-reading
one message. A mocked broker would have modeled our wrong assumption and passed. The whole
argument for real-dependency testing is in that one failure.

The companion `pipeline_test.go` does the same for the DAG choreography — with the broker
replaced by a *capture function* at the port boundary (the architecture makes that swap
one line), asserting exact fan-out waves, argument propagation (`add.Arg1==2, add.Arg2==6`),
**replay idempotency** (re-applying a result yields zero new events), and audit-trail
completeness read straight from the outbox.

## E2E smoke: the tier that catches composition

Two bugs survived unit + integration and fell only to curl against the live stack:

1. **500 instead of 400** — the in-process grpc-gateway bypasses interceptors, so typed
   errors never became statuses. No unit test exercises nginx→Fiber→gateway→server as one
   path.
2. **Single-span traces** — every service was "instrumented", every middleware installed;
   the *links* were broken (gateway metadata, outbox context). Only "assert one trace ID
   spans ≥2 services in Jaeger" could see it.

The E2E checklist that now guards them: submit → correct result → tasks split across agents
→ 400/404 mapping → multi-service trace → audit trail present → DLQ empty. Cheap, brutal,
composition-shaped.

## Static tier: drift checks as tests

`make generate-check` regenerates buf + sqlc output and fails on `git diff` — hand-edited or
stale generated code cannot merge. `make lint` runs **all** golangci linters (documented
disable list) — which is a real test tier: gosec's G115 sweep forced explicit clamps on
every int narrowing. `make vuln` (govulncheck) is call-graph-aware dependency scanning.
These run in CI as the same Makefile targets developers run locally — one source of truth,
no "passes on my machine".

## What NOT to test

- **Generated code internals** — drift-check them instead.
- **The language/stdlib** (JSON round-trips of plain structs, context cancellation itself).
- **Private helpers through contortions** — test the exported behavior; if a helper needs
  direct testing, it's asking to be a package.
- **Exact log strings / metric names** — assert *that* signals fire where behavior-relevant,
  not their prose.
- **Timing precision** — assert ordering and eventual convergence, never "took ~100ms".

## Anti-patterns seen in the wild

**The mock that models your misunderstanding.**
```go
// ❌ mockBroker.Requeue() appends to the tail — real RabbitMQ requeues to the HEAD
```
The mock encodes the same wrong assumption as the code; both nod at each other in review.
Real dependency, or a fake at a port boundary that's too thin to be wrong.

**Coverage theater.** 90% coverage where every assertion is `err == nil`. The audit pipeline
test would "cover" the same lines with zero guarantees checked. Review *assertions*, not
percentages.

**Flake quarantine as a lifestyle.** A `//flaky, rerun` comment is a race report you chose
to ignore. Every flake in this repo's history was a real ordering bug (batcher timing, pool
scale-down) fixed by *choreography*, not by retries in CI.

**Test-only branches in production code.** `if testing { ... }` means production runs
unverified paths. This repo's alternative: ports. `nopNotifier{}` implements a real
interface; production wiring is untouched.

## PR review checklist

- [ ] New logic: table-driven cases including the boundary that scared you into writing it
- [ ] New concurrent code: contention test under `-race` (blocking-channel choreography)
- [ ] New queue/DB semantics: integration test against the real image compose uses
- [ ] New consumer: duplicate-delivery replay test proving no-op
- [ ] Pagination: full-walk test (no gaps, no overlaps)
- [ ] Eventual assertions: deadline-polled with diagnostic failure, never bare sleeps
- [ ] Guarantee-sheet test updated if a pipeline's promises changed
- [ ] `make test test-integration lint generate-check` all green

## How to verify the suite itself

```bash
cd backend
go test -race -count=3 ./pkg/... ./internal/...   # flake hunt: 3 fresh runs
make test-integration                              # real PG18 / Redis 8 / RabbitMQ 4
# mutation spot-check: break something on purpose —
#   flip `unmet_deps - 1` to `- 2` in tasks.sql, regenerate, run pipeline_test
#   → it must fail. A suite that survives sabotage isn't testing.
```
