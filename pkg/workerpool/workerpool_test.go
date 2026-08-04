package workerpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolProcessesAll(t *testing.T) {
	t.Parallel()
	p := New(Config{Min: 2, Max: 8, QueueSize: 16, ScaleEvery: 10 * time.Millisecond}, nil)
	var n atomic.Int64
	const total = 500
	for i := 0; i < total; i++ {
		if !p.Submit(func(context.Context) { n.Add(1) }) {
			t.Fatal("submit refused while pool open")
		}
	}
	p.Close()
	if n.Load() != total {
		t.Fatalf("processed %d, want %d", n.Load(), total)
	}
}

func TestPoolAutoscalesUpAndRespectsMax(t *testing.T) {
	t.Parallel()
	var maxSeen atomic.Int32
	p := New(Config{Min: 1, Max: 4, QueueSize: 64, ScaleEvery: 5 * time.Millisecond},
		func(_, to int32, _ string) {
			for {
				cur := maxSeen.Load()
				if to <= cur || maxSeen.CompareAndSwap(cur, to) {
					break
				}
			}
		})
	block := make(chan struct{})
	for i := 0; i < 32; i++ {
		p.Submit(func(context.Context) { <-block })
	}
	time.Sleep(100 * time.Millisecond) // let autoscaler observe backlog
	if got := p.Stats().Workers; got != 4 {
		t.Errorf("workers = %d, want max 4", got)
	}
	if maxSeen.Load() > 4 {
		t.Errorf("scaled beyond max: %d", maxSeen.Load())
	}
	close(block)
	p.Close()
}

func TestPoolScalesDownWhenIdle(t *testing.T) {
	t.Parallel()
	p := New(Config{
		Min: 1, Max: 6, QueueSize: 64,
		ScaleEvery:  5 * time.Millisecond,
		IdleTimeout: 30 * time.Millisecond,
	}, nil)
	block := make(chan struct{})
	for i := 0; i < 24; i++ {
		p.Submit(func(context.Context) { <-block })
	}
	time.Sleep(60 * time.Millisecond)
	close(block)

	deadline := time.After(2 * time.Second)
	for {
		if p.Stats().Workers == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("did not scale down to Min, workers=%d", p.Stats().Workers)
		case <-time.After(10 * time.Millisecond):
		}
	}
	p.Close()
}

func TestTrySubmitShedsWhenFull(t *testing.T) {
	t.Parallel()
	p := New(Config{Min: 1, Max: 1, QueueSize: 1}, nil)
	defer p.Close()
	block := make(chan struct{})
	defer close(block)
	p.Submit(func(context.Context) { <-block }) // occupies the worker
	p.Submit(func(context.Context) {})          // fills the queue... eventually
	// Fill until TrySubmit sheds.
	shed := false
	for i := 0; i < 10; i++ {
		if !p.TrySubmit(func(context.Context) {}) {
			shed = true
			break
		}
	}
	if !shed {
		t.Error("TrySubmit never shed with a full queue")
	}
}
