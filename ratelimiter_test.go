package ratelimiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestBucket(rate float64, capacity int) (*TokenBucket, *fakeClock) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewTokenBucket(rate, capacity)
	b.now = clk.Now
	b.last = clk.Now()
	return b, clk
}

func TestBurstThenDeny(t *testing.T) {
	b, _ := newTestBucket(1, 3)
	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	if b.Allow() {
		t.Fatal("4th request should be denied")
	}
}

func TestRefill(t *testing.T) {
	b, clk := newTestBucket(2, 2)
	b.Allow()
	b.Allow()
	if b.Allow() {
		t.Fatal("bucket should be empty")
	}
	clk.Advance(500 * time.Millisecond) // +1 token at 2/s
	if !b.Allow() {
		t.Fatal("should allow after refill")
	}
	if b.Allow() {
		t.Fatal("only one token should have refilled")
	}
	clk.Advance(10 * time.Second) // capped at capacity
	if got := b.Tokens(); got != 2 {
		t.Fatalf("tokens = %v, want 2", got)
	}
}

func TestConcurrentAllow(t *testing.T) {
	b, _ := newTestBucket(1, 100)
	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	if allowed != 100 {
		t.Fatalf("allowed = %d, want 100", allowed)
	}
}

func TestWait(t *testing.T) {
	b := NewTokenBucket(50, 1) // one token every 20ms
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 3; i++ {
		if err := b.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 30*time.Millisecond {
		t.Fatalf("Wait returned too fast: %v", el)
	}
}

func TestWaitCancel(t *testing.T) {
	b := NewTokenBucket(0.1, 1)
	b.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx); err == nil {
		t.Fatal("expected context error")
	}
}

func TestKeyedLimiter(t *testing.T) {
	k := NewKeyedLimiter(1, 1)
	if !k.Allow("a") || k.Allow("a") {
		t.Fatal("key a: want allow then deny")
	}
	if !k.Allow("b") {
		t.Fatal("key b should have its own bucket")
	}
}
