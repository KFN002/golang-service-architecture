# 📚 Skills — the repo as a curriculum

Every directory here is one **SKILL.md**: a deep, opinionated teaching document distilling
how `perfect-go-service` solves one class of problem — with real code excerpts from this
repo, the bugs we actually hit, decision tables, anti-pattern galleries, and PR checklists
you can apply tomorrow.

They follow the [agent-skills convention](https://agentskills.io) (frontmatter `name` +
`description`), so you can drop this folder into an AI coding agent's skill path — or just
read them as engineering essays.

## The skills

| Skill | One-liner | Read it before you… |
|---|---|---|
| [golang-architecture](golang-architecture/SKILL.md) | Layering, ports owned by consumers, hand-rolled DI, the error taxonomy, single-point transport mapping | structure a new Go service or review package boundaries |
| [concurrency](concurrency/SKILL.md) | Auto-scaling pools, double-buffered batching, composed backpressure, atomics-vs-mutex-vs-channel, leak-free lifecycles | write your next `go` statement |
| [fault-tolerance](fault-tolerance/SKILL.md) | Retry+jitter, circuit breakers, bulkheads, TTL+DLX retry queues, DLQ redrive, transactional outbox, idempotency layering, shutdown choreography | let a service talk to anything that can fail |
| [highload](highload/SKILL.md) | Fastest-stack selection, group commit, singleflight caching, keyset pagination, partitioning+BRIN, three-ring throttling, container-aware runtime | chase throughput or field a load spike |
| [messaging](messaging/SKILL.md) | AMQP topology, confirms semantics, ack decision trees, delivery-guarantee math, poison handling, trace context through brokers | design anything queue-shaped |
| [observability](observability/SKILL.md) | One trace browser→worker, propagation across every boundary (and the two bugs that broke it), metrics over atomics, correlated logs, health semantics | claim your system is "instrumented" |
| [security](security/SKILL.md) | Eight defense rings: allow-list validation, layered limits, structural DB guards, scratch containers, netsegments, supply-chain checks | ship anything internet-facing |
| [frontend](frontend/SKILL.md) | Next.js islands, design tokens, hand-rolled primitives, SSE with reconnect, custom SVG dataviz, same-origin API architecture | build a real-time dashboard |
| [testing](testing/SKILL.md) | The pyramid with receipts: oracle tests, channel choreography, testcontainers, guarantee-sheet tests, E2E composition catches, drift checks | trust a green checkmark |

## How they're written

- **Everything is grounded** — excerpts come from this repo with paths; claims name the
  test that proves them (`TestAuditFullPipeline`, `pipeline_test.go`, the E2E smoke).
- **War stories included** — the OTel schema-URL crash, the DLQ head-requeue re-read, the
  in-process gateway that bypassed interceptors, the unary-minus precedence bug. Failure
  teaches faster than success.
- **Actionable endings** — every skill closes with a PR review checklist and a
  "how to verify" command block.

## Suggested reading order

New to the repo: **golang-architecture → concurrency → fault-tolerance** (the spine), then
**messaging + observability** (the connective tissue), then **highload + security**
(the pressure layers), with **frontend** and **testing** as you touch those surfaces.
