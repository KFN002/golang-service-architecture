// Package batcher implements generic micro-batching with double buffering.
//
// Producers Add items into the active buffer under a short mutex; a background
// loop swaps buffers when either the size threshold or the flush interval is
// reached and hands the full buffer to the flush function. Because the swap is
// O(1), producers never wait on a flush in progress (write-behind).
package batcher

import (
	"context"
	"sync"
	"time"
)

// Flush persists one batch. It is called from a single goroutine; if it
// returns an error the batch is passed to OnError (e.g. to nack messages).
type Flush[T any] func(ctx context.Context, batch []T) error

// Config tunes the batcher.
type Config struct {
	MaxSize int           // flush when the active buffer reaches this size
	MaxWait time.Duration // ... or when this much time has passed
}

func (c *Config) defaults() {
	if c.MaxSize <= 0 {
		c.MaxSize = 500
	}
	if c.MaxWait <= 0 {
		c.MaxWait = 200 * time.Millisecond
	}
}

// Batcher accumulates items and flushes them in groups.
type Batcher[T any] struct {
	cfg     Config
	flush   Flush[T]
	onError func(batch []T, err error)

	mu     sync.Mutex
	active []T
	kick   chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates and starts a batcher.
func New[T any](cfg Config, flush Flush[T], onError func([]T, error)) *Batcher[T] {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	b := &Batcher[T]{
		cfg:     cfg,
		flush:   flush,
		onError: onError,
		active:  make([]T, 0, cfg.MaxSize),
		kick:    make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
	}
	b.wg.Add(1)
	go b.loop()
	return b
}

// Add appends one item. The mutex covers only the append — flushing happens
// on the background goroutine with a swapped-out buffer.
func (b *Batcher[T]) Add(item T) {
	b.mu.Lock()
	b.active = append(b.active, item)
	full := len(b.active) >= b.cfg.MaxSize
	b.mu.Unlock()
	if full {
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}
}

// Len reports the current buffered item count.
func (b *Batcher[T]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.active)
}

// Close flushes whatever is buffered and stops the loop.
func (b *Batcher[T]) Close() {
	b.cancel()
	b.wg.Wait()
}

func (b *Batcher[T]) loop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.cfg.MaxWait)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.swapAndFlush()
		case <-b.kick:
			b.swapAndFlush()
		case <-b.ctx.Done():
			b.swapAndFlush() // final drain
			return
		}
	}
}

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
	// Use a background context: shutdown must still drain the final batch.
	for start := 0; start < len(batch); start += b.cfg.MaxSize {
		end := min(start+b.cfg.MaxSize, len(batch))
		chunk := batch[start:end]
		if err := b.flush(context.Background(), chunk); err != nil && b.onError != nil {
			b.onError(chunk, err)
		}
	}
}
