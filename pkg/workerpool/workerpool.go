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
	if c.Max < c.Min {
		c.Max = c.Min
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

// Task is one unit of work.
type Task func(ctx context.Context)

// OnScale is notified after every pool-size change (for metrics/audit).
type OnScale func(from, to int32, reason string)

// Pool is the auto-scaling worker pool.
type Pool struct {
	cfg     Config
	queue   chan Task
	workers atomic.Int32
	busy    atomic.Int32
	done    atomic.Int64

	scaleMu sync.Mutex
	onScale OnScale

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates and starts a pool with Min workers.
func New(cfg Config, onScale OnScale) *Pool {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		cfg:     cfg,
		queue:   make(chan Task, cfg.QueueSize),
		onScale: onScale,
		ctx:     ctx,
		cancel:  cancel,
	}
	for i := 0; i < cfg.Min; i++ {
		p.spawn(false)
	}
	p.wg.Add(1)
	go p.autoscale()
	return p
}

// Submit enqueues a task, blocking while the queue is full (backpressure).
// It returns false if the pool is shutting down.
func (p *Pool) Submit(t Task) bool {
	select {
	case <-p.ctx.Done():
		return false
	case p.queue <- t:
		return true
	}
}

// TrySubmit enqueues without blocking; false means "queue full or closed"
// (callers use this to implement load shedding).
func (p *Pool) TrySubmit(t Task) bool {
	select {
	case <-p.ctx.Done():
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
	p.cancel()
	p.wg.Wait()
}

// spawn starts one worker. transient workers exit after IdleTimeout idle.
func (p *Pool) spawn(transient bool) {
	from := p.workers.Add(1) - 1
	p.notify(from, from+1, "spawn")
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		idle := time.NewTimer(p.cfg.IdleTimeout)
		defer idle.Stop()
		for {
			select {
			case t := <-p.queue:
				p.busy.Add(1)
				t(p.ctx)
				p.busy.Add(-1)
				p.done.Add(1)
				if transient {
					if !idle.Stop() {
						select {
						case <-idle.C:
						default:
						}
					}
					idle.Reset(p.cfg.IdleTimeout)
				}
			case <-idle.C:
				if transient && p.workers.Load() > int32(p.cfg.Min) {
					to := p.workers.Add(-1)
					p.notify(to+1, to, "idle")
					return
				}
				idle.Reset(p.cfg.IdleTimeout)
			case <-p.ctx.Done():
				// Drain remaining queued work before exiting.
				for {
					select {
					case t := <-p.queue:
						t(context.Background())
						p.done.Add(1)
					default:
						to := p.workers.Add(-1)
						p.notify(to+1, to, "shutdown")
						return
					}
				}
			}
		}
	}()
}

// autoscale grows the pool while backlog persists.
func (p *Pool) autoscale() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.ScaleEvery)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			backlog := len(p.queue)
			workers := p.workers.Load()
			if backlog > 0 && workers < int32(p.cfg.Max) {
				p.scaleMu.Lock()
				// Re-check under lock; grow proportionally to backlog.
				grow := backlog/2 + 1
				for i := 0; i < grow && p.workers.Load() < int32(p.cfg.Max); i++ {
					p.spawn(true)
				}
				p.scaleMu.Unlock()
			}
		}
	}
}

func (p *Pool) notify(from, to int32, reason string) {
	if p.onScale != nil {
		p.onScale(from, to, reason)
	}
}
