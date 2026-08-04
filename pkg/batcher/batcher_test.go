package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlushBySize(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var batches [][]int
	b := New[int](Config{MaxSize: 10, MaxWait: time.Hour}, func(_ context.Context, batch []int) error {
		mu.Lock()
		batches = append(batches, batch)
		mu.Unlock()
		return nil
	}, nil)
	for i := 0; i < 25; i++ {
		b.Add(i)
	}
	time.Sleep(50 * time.Millisecond)
	b.Close()

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, batch := range batches {
		total += len(batch)
		if len(batch) > 10 {
			t.Errorf("batch of %d exceeds MaxSize", len(batch))
		}
	}
	if total != 25 {
		t.Errorf("flushed %d items, want 25", total)
	}
}

func TestFlushByInterval(t *testing.T) {
	t.Parallel()
	flushed := make(chan []int, 1)
	b := New[int](Config{MaxSize: 1000, MaxWait: 20 * time.Millisecond},
		func(_ context.Context, batch []int) error {
			select {
			case flushed <- batch:
			default:
			}
			return nil
		}, nil)
	defer b.Close()
	b.Add(1)
	b.Add(2)
	select {
	case batch := <-flushed:
		if len(batch) != 2 {
			t.Errorf("batch len = %d, want 2", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("interval flush never happened")
	}
}

func TestCloseDrains(t *testing.T) {
	t.Parallel()
	var n atomic.Int64
	b := New[int](Config{MaxSize: 1000, MaxWait: time.Hour},
		func(_ context.Context, batch []int) error {
			n.Add(int64(len(batch)))
			return nil
		}, nil)
	for i := 0; i < 7; i++ {
		b.Add(i)
	}
	b.Close()
	if n.Load() != 7 {
		t.Errorf("drained %d, want 7", n.Load())
	}
}

func TestOnErrorReceivesBatch(t *testing.T) {
	t.Parallel()
	errs := make(chan int, 1)
	b := New[int](Config{MaxSize: 2, MaxWait: time.Hour},
		func(_ context.Context, batch []int) error { return context.DeadlineExceeded },
		func(batch []int, err error) {
			select {
			case errs <- len(batch):
			default:
			}
		})
	defer b.Close()
	b.Add(1)
	b.Add(2)
	select {
	case n := <-errs:
		if n != 2 {
			t.Errorf("error batch len = %d, want 2", n)
		}
	case <-time.After(time.Second):
		t.Fatal("onError never called")
	}
}

func TestConcurrentAdds(t *testing.T) {
	t.Parallel()
	var n atomic.Int64
	b := New[int](Config{MaxSize: 64, MaxWait: 5 * time.Millisecond},
		func(_ context.Context, batch []int) error {
			n.Add(int64(len(batch)))
			return nil
		}, nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				b.Add(i)
			}
		}()
	}
	wg.Wait()
	b.Close()
	if n.Load() != 2000 {
		t.Errorf("flushed %d, want 2000", n.Load())
	}
}
