package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBurstThenThrottle(t *testing.T) {
	t.Parallel()
	l := New(Config{Rate: 1, Burst: 3})
	defer l.Close()
	for i := 0; i < 3; i++ {
		if !l.Allow("ip1") {
			t.Fatalf("burst request %d denied", i)
		}
	}
	if l.Allow("ip1") {
		t.Error("4th request allowed, want throttled")
	}
	// A different key has its own bucket.
	if !l.Allow("ip2") {
		t.Error("independent key throttled")
	}
}

func TestRefill(t *testing.T) {
	t.Parallel()
	l := New(Config{Rate: 100, Burst: 1})
	defer l.Close()
	if !l.Allow("k") {
		t.Fatal("first denied")
	}
	if l.Allow("k") {
		t.Fatal("second allowed instantly")
	}
	time.Sleep(20 * time.Millisecond) // 100/s → ~2 tokens
	if !l.Allow("k") {
		t.Error("not refilled after wait")
	}
}

func TestConcurrentKeys(t *testing.T) {
	t.Parallel()
	l := New(Config{Rate: 1000, Burst: 5})
	defer l.Close()
	var wg sync.WaitGroup
	var allowed atomic.Int64
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", g%4)
			for i := 0; i < 100; i++ {
				if l.Allow(key) {
					allowed.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	// 4 distinct keys × burst 5 minimum; upper bound is loose (refill during run).
	if a := allowed.Load(); a < 20 {
		t.Errorf("allowed = %d, want ≥ 20", a)
	}
}
