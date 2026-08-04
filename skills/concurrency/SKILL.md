---
name: concurrency
description: Use when writing or reviewing concurrent Go — worker pools, batching, backpressure, channel vs mutex vs atomic choices, goroutine lifecycles, and race-free shutdown. Grounded in this repo's auto-scaling pool and double-buffered batcher.
---

# Go Concurrency Patterns — the working set

This repo runs thousands of goroutines across three services without a single data race
(`-race` on every test run, all green). This skill is the pattern set that makes that true:
an auto-scaling worker pool, a double-buffered micro-batcher, composed backpressure, and the
lifecycle discipline that lets it all shut down cleanly.

## Overview

Concurrency bugs are architecture bugs. You don't fix them with more locks — you fix them by
deciding, per piece of state, exactly one ownership story:

- **owned by one goroutine** → no synchronization (channels move data *to* the owner)
- **read-mostly counters/flags** → atomics
- **compound invariants** → one mutex, smallest possible critical section
- **lifecycle** → a `stop` channel + `sync.WaitGroup`, never a stored `context.Context`

## The rules

1. **Atomics for counters, a mutex only for decisions.** WHY: counters are independent;
   decisions ("should we scale?") read several values coherently.
2. **Backpressure is designed end-to-end or it doesn't exist.** Every unbounded queue is an
   OOM with a delay. WHY: pressure must propagate to the *source* (here: broker prefetch).
3. **The goroutine that starts work owns its shutdown.** Every `go` has a corresponding
   drain/wait. WHY: leaked goroutines are leaked memory, ports, and file handles — and they
   fire after your test passes.
4. **Don't store contexts in structs.** Store a `stop chan struct{}` and take `ctx` per call.
   WHY: a stored context hides the cancellation graph and outlives its request. (This repo
   refactored exactly this under `containedctx`.)
5. **Never block a hot path on a slow consumer.** Swap buffers, drop-per-client, or shed —
   but never let one slow reader wedge a broadcast.
6. **`select` with `default` is load shedding; without is backpressure.** Choose consciously.
7. **Timers must be drained before Reset.** The `!t.Stop() { select{ case <-t.C: default: } }`
   dance is mandatory or you get ghost fires.
8. **Race detector always.** `-race` in `make test` and CI. A race "that can't happen" is a
   race you haven't scheduled yet.

## Pattern: auto-scaling worker pool

**Problem:** fixed pools waste memory at idle and lag under burst; naive scaling code is a
race-condition farm.

**Solution** — `backend/pkg/workerpool/workerpool.go`. Bookkeeping is atomics; the only
mutex guards the scale decision:

```go
// Pool is the auto-scaling worker pool.
type Pool struct {
	cfg     Config
	queue   chan Task
	workers atomic.Int32
	busy    atomic.Int32
	done    atomic.Int64

	scaleMu sync.Mutex
	onScale OnScale

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}
```

Submission is the backpressure valve — blocking send vs non-blocking shed, side by side:

```go
// Submit enqueues a task, blocking while the queue is full (backpressure).
func (p *Pool) Submit(t Task) bool {
	select {
	case <-p.stop:
		return false
	case p.queue <- t:
		return true
	}
}

// TrySubmit enqueues without blocking; false means "queue full or closed"
// (callers use this to implement load shedding).
func (p *Pool) TrySubmit(t Task) bool {
	select {
	case <-p.stop:
		return false
	case p.queue <- t:
		return true
	default:
		return false
	}
}
```

Scaling: the autoscaler observes backlog and grows under a mutex (re-checking under the lock —
the classic double-check), workers self-retire on idle, and shutdown drains the queue so
accepted work is never dropped:

```go
// maybeGrow adds workers proportionally to the observed backlog.
func (p *Pool) maybeGrow() {
	backlog := len(p.queue)
	if backlog == 0 || p.workers.Load() >= p.max32 {
		return
	}
	p.scaleMu.Lock()
	defer p.scaleMu.Unlock()
	grow := backlog/2 + 1
	for i := 0; i < grow && p.workers.Load() < p.max32; i++ {
		p.spawn(true)
	}
}
```

| ❌ Wrong | ✅ Right (this repo) |
|---|---|
| `len(ch)` checked, then acted on, no lock | `len` only as a *hint*; the decisive check re-runs under `scaleMu` |
| Workers killed via shared `cancel()` mid-task | Workers finish current task; retire via idle timer or drain on `stop` |
| Unbounded `go handle(msg)` per message | Bounded queue + bounded workers; Submit blocks |
| Pool exposes its channel | Pool exposes `Submit/TrySubmit/Stats/Close` only |

**How to verify:** `workerpool_test.go` — `TestPoolAutoscalesUpAndRespectsMax` (blocks workers,
asserts ceiling), `TestPoolScalesDownWhenIdle` (deadline-polls back to Min),
`TestTrySubmitShedsWhenFull`. All under `-race`.

## Pattern: double-buffered micro-batcher

**Problem:** per-item writes can't keep up (per-row INSERT ≈ network RTT each); naive batching
blocks producers during flush.

**Solution** — `backend/pkg/batcher/batcher.go`. Producers append under a short mutex; the
flusher *swaps* the buffer in O(1) and works on the full one outside the lock:

```go
// swapAndFlush swaps the active buffer for an empty one (O(1)) and flushes
// the filled buffer outside the lock.
func (b *Batcher[T]) swapAndFlush() {
	b.mu.Lock()
	if len(b.active) == 0 {
		b.mu.Unlock()
		return
	}
	batch := b.active
	b.active = make([]T, 0, b.cfg.MaxSize)
	b.mu.Unlock()

	// Producers may outrun the loop between kicks, so the swapped buffer can
	// exceed MaxSize — honor the contract by flushing in MaxSize chunks.
	for start := 0; start < len(batch); start += b.cfg.MaxSize {
```

That comment records a real bug the unit test caught: 25 rapid Adds before the loop woke
produced a 25-item batch against `MaxSize: 10`. The test (`TestFlushBySize`) asserted the
contract; the chunk loop restored it. **Write the contract test before you trust the buffer
math.**

The audit ingester composes this with per-item acknowledgment — each event carries its own
completion channel, so an AMQP ack waits for *its* batch's durable flush
(`backend/internal/usecase/audit/ingest.go`):

```go
// item couples an event with its flush acknowledgment.
type item struct {
	ev   entity.AuditEvent
	done chan error
}
...
	it := item{ev: ev, done: make(chan error, 1)}
	b.batch.Add(it)
	select {
	case err := <-it.done:
		return err                 // ack/nack decision rides the flush result
	case <-time.After(i.cfg.FlushTimeout):
		return apperrors.New(apperrors.CodeUnavailable, "audit flush timeout")
	case <-ctx.Done():
```

This is group commit with end-to-end durability: unflushed event ⇒ unacked message ⇒ broker
redelivers. The buffered `done` chan (size 1) matters — the flusher must never block sending
a result to a caller that already timed out.

## Pattern: composed backpressure

The agent's intake chain, end to end:

```
RabbitMQ prefetch (64) → consumer loop → pool.Submit (blocks) → queue (64) → workers (2..24)
```

Wired in `backend/internal/controller/amqp/v1/consumers.go`:

```go
		// Fan-out into the auto-scaling pool; Submit blocks when the pool
		// queue is full — backpressure all the way to the broker prefetch.
		Async: func(fn func()) bool {
			return a.pool.Submit(func(context.Context) { fn() })
		},
```

When workers saturate: queue fills → Submit blocks → consumer loop stalls → unacked count
hits prefetch → **the broker stops sending**. Messages wait durably in RabbitMQ where they
belong — not in your process memory. No component "implements backpressure"; the *composition*
does. That's the design skill.

## Pattern: fan-out / fan-in without channels

Fan-in across *processes* can't use channels. This repo's fan-in is a DB transaction:
results arrive via queue, and dependency bookkeeping is an atomic SQL update
(`FillArg1FromResult`: `SET arg1_value=$2, unmet_deps = unmet_deps-1 ... RETURNING unmet_deps`).
When `unmet_deps` hits 0, the dependent is enqueued. Concurrency control is
`FOR UPDATE SKIP LOCKED` — replicas *compete without colliding*.

Lesson: channels are an in-process tool. Distributed fan-in = idempotent state transitions +
atomic counters in the store. Same shape, different substrate.

## Lifecycles: errgroup + stop channels

Every service runs its loops under one `errgroup` (`backend/internal/app/agent.go`):

```go
	g, gctx := errgroup.WithContext(ctx)
	g.Go(runPprof(gctx, cfg.PprofEnabled, log))
	g.Go(func() error { return consumer.Run(gctx, mq, cfg.Prefetch) })
	g.Go(func() error { ... return app.Listen(...) })
	g.Go(func() error {
		<-gctx.Done()
		log.Info("draining worker pool")
		pool.Close() // waits for queued tasks to finish
		_ = app.Shutdown()
		return nil
	})
	err = g.Wait()
```

One goroutine fails ⇒ `gctx` cancels ⇒ every loop exits ⇒ the shutdown goroutine drains.
`signal.NotifyContext(ctx, SIGINT, SIGTERM)` at the top makes Ctrl-C the same path as a
crash — one shutdown story, tested implicitly every time you stop the process.

Inside components, lifecycle is `stop chan struct{}` + `sync.Once` + `WaitGroup` — see the
pool's `Close`:

```go
func (p *Pool) Close() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.wg.Wait()
}
```

`sync.Once` makes Close idempotent (double-close of a channel panics); `wg.Wait` makes it
*mean* something. `wg.Go(...)` (Go 1.25+) removes the Add/Done pairing you can get wrong.

## Decision table: atomic vs mutex vs channel

| State | Tool | Repo example |
|---|---|---|
| Independent counter/gauge | `atomic.Int32/64` | pool `workers`, `busy`, `done`; ingest stats |
| One-shot flag + waiters | `chan struct{}` + `sync.Once` | every `stop` channel |
| Multi-field invariant | `sync.Mutex`, tight scope | batcher's `active` swap; limiter buckets |
| Read-heavy registry | `sync.RWMutex` | SSE hub client map |
| Hand-off of work items | buffered channel | pool `queue` |
| Cross-process coordination | DB row + `SKIP LOCKED` | outbox claim; dependency counters |
| Result of one async op | 1-buffered `chan error` | batcher item ack |

## Anti-patterns seen in the wild

**The goroutine leak with a smile.**

```go
// ❌ if no one reads results, this goroutine lives forever
go func() { results <- compute() }()
```

Fix: buffered channel sized to the fan-out, or select on a done/stop channel. Every `go`
needs an exit story you can point at.

**Broadcast that trusts its slowest listener.** One stuck SSE client blocks the hub's send,
which blocks the Redis reader, which backs up everything. This repo's hub
(`internal/controller/http/v1/sse.go`):

```go
	for _, ch := range h.clients {
		select {
		case ch <- data:
		default: // slow client — drop for them, never block the hub
		}
	}
```

Per-client buffered channel + drop-on-full. UI events are refreshable; hub liveness isn't.

**Time-based test synchronization.** `sleep(100ms); assert(...)` is a flake generator.
This repo's tests block on *channels* to choreograph ("worker occupied" = it received from
`block`), and use deadline-*polling* only for eventually-true states (scale-down), with a
hard timeout that fails loudly.

**Mutex around I/O.** Holding a lock across a network call serializes your whole service on
your slowest dependency. The batcher pattern is the antidote: lock → swap pointers → unlock →
do I/O on private data.

## PR review checklist

- [ ] Every `go` statement: where does it exit? Who waits for it?
- [ ] Every channel: bounded? Who closes it? (Answer must be "the sender" or "nobody + stop chan")
- [ ] Every mutex section: no I/O, no callbacks, no `<-ch` inside
- [ ] Shared numbers are atomics; shared invariants are one mutex — no mixed access to the same field
- [ ] Shutdown: intake stops *before* drain; drain has a deadline or a guaranteed-finite queue
- [ ] No stored `context.Context` in struct fields
- [ ] Timer `Reset` preceded by the drain dance
- [ ] New concurrent code has a test that runs it under real parallelism (`-race`, multiple goroutines, contention)

## How to verify

```bash
cd backend
go test -race -count=2 ./pkg/workerpool/ ./pkg/batcher/ ./pkg/bulkhead/ ./pkg/ratelimit/
```

`-count=2` reruns without cached results — cheap flake detection. If you touched scheduling
logic, also run the integration pipeline test: real PG, real contention, real fan-in.
