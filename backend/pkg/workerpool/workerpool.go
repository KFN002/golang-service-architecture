// Package workerpool implements an auto-scaling goroutine worker pool.
//
// The pool grows when the submit queue backs up and shrinks when workers sit
// idle, between configurable Min and Max bounds. All bookkeeping is done with
// atomics; the only lock is around the scale-decision itself. The zero-alloc
// hot path is Submit → channel → worker.
package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// maxPoolSize bounds configuration so int32 counters can never overflow.
const maxPoolSize = 1 << 20

// Config tunes the pool.
type Config struct {
	Min         int           // workers always alive
	Max         int           // hard ceiling
	QueueSize   int           // buffered submit queue length
	IdleTimeout time.Duration // a worker above Min exits after this idle time
	ScaleEvery  time.Duration // how often the autoscaler evaluates backlog
}

func (c *Config) defaults() {
	if c.Min <= 0 {
		c.Min = 1
	}
	if c.Min > maxPoolSize {
		c.Min = maxPoolSize
	}
	if c.Max < c.Min {
		c.Max = c.Min
	}
	if c.Max > maxPoolSize {
		c.Max = maxPoolSize
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 64
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.ScaleEvery <= 0 {
		c.ScaleEvery = 250 * time.Millisecond
	}
}

// Task is one unit of work. The pool passes a background context: tasks that
// need cancellation semantics carry their own context in their closure.
type Task func(ctx context.Context)

// OnScale is notified after every pool-size change (for metrics/audit).
type OnScale func(from, to int32, reason string)

// Pool is the auto-scaling worker pool.
type Pool struct {
	cfg     Config
	min32   int32 // cfg.Min, pre-clamped (see maxPoolSize)
	max32   int32 // cfg.Max, pre-clamped
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

// New creates and starts a pool with Min workers.
func New(cfg Config, onScale OnScale) *Pool {
	cfg.defaults()
	p := &Pool{
		cfg:     cfg,
		min32:   int32(cfg.Min), // #nosec G115 -- clamped to maxPoolSize in defaults
		max32:   int32(cfg.Max), // #nosec G115 -- clamped to maxPoolSize in defaults
		queue:   make(chan Task, cfg.QueueSize),
		onScale: onScale,
		stop:    make(chan struct{}),
	}
	for range cfg.Min {
		p.spawn(false)
	}
	p.wg.Go(p.autoscale)
	return p
}

// Submit enqueues a task, blocking while the queue is full (backpressure).
// It returns false if the pool is shutting down.
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

// Stats is a point-in-time snapshot for metrics.
type Stats struct {
	Workers int32
	Busy    int32
	Backlog int
	Done    int64
}

// Stats returns current pool statistics (lock-free reads).
func (p *Pool) Stats() Stats {
	return Stats{
		Workers: p.workers.Load(),
		Busy:    p.busy.Load(),
		Backlog: len(p.queue),
		Done:    p.done.Load(),
	}
}

// Close stops intake, waits for queued tasks to drain, then stops workers.
func (p *Pool) Close() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.wg.Wait()
}

// spawn starts one worker. Transient workers exit after IdleTimeout idle.
func (p *Pool) spawn(transient bool) {
	from := p.workers.Add(1) - 1
	p.notify(from, from+1, "spawn")
	p.wg.Go(func() { p.runWorker(transient) })
}

// runWorker is one worker's lifecycle: execute, idle-retire, drain on stop.
func (p *Pool) runWorker(transient bool) {
	idle := time.NewTimer(p.cfg.IdleTimeout)
	defer idle.Stop()
	for {
		select {
		case t := <-p.queue:
			p.execute(t)
			resetTimer(idle, p.cfg.IdleTimeout, transient)
		case <-idle.C:
			if transient && p.workers.Load() > p.min32 {
				to := p.workers.Add(-1)
				p.notify(to+1, to, "idle")
				return
			}
			idle.Reset(p.cfg.IdleTimeout)
		case <-p.stop:
			p.drain()
			to := p.workers.Add(-1)
			p.notify(to+1, to, "shutdown")
			return
		}
	}
}

// execute runs one task with busy accounting.
func (p *Pool) execute(t Task) {
	p.busy.Add(1)
	t(context.Background())
	p.busy.Add(-1)
	p.done.Add(1)
}

// drain empties the remaining queue during shutdown — accepted work is never
// dropped by a graceful stop.
func (p *Pool) drain() {
	for {
		select {
		case t := <-p.queue:
			t(context.Background())
			p.done.Add(1)
		default:
			return
		}
	}
}

// resetTimer restarts a transient worker's idle clock after each task.
func resetTimer(idle *time.Timer, d time.Duration, transient bool) {
	if !transient {
		return
	}
	if !idle.Stop() {
		select {
		case <-idle.C:
		default:
		}
	}
	idle.Reset(d)
}

// autoscale grows the pool while backlog persists.
func (p *Pool) autoscale() {
	ticker := time.NewTicker(p.cfg.ScaleEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.maybeGrow()
		}
	}
}

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

func (p *Pool) notify(from, to int32, reason string) {
	if p.onScale != nil {
		p.onScale(from, to, reason)
	}
}
