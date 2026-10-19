// Package ratelimiter provides a simple, thread-safe token bucket rate limiter.
package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// TokenBucket allows up to `capacity` events in a burst and refills
// at `rate` tokens per second. It is safe for concurrent use.
type TokenBucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	rate     float64 // tokens per second
	last     time.Time
	now      func() time.Time
}

// NewTokenBucket creates a bucket that starts full.
// rate is tokens added per second; capacity is the maximum burst size.
func NewTokenBucket(rate float64, capacity int) *TokenBucket {
	if rate <= 0 {
		panic("ratelimiter: rate must be > 0")
	}
	if capacity <= 0 {
		panic("ratelimiter: capacity must be > 0")
	}
	return &TokenBucket{
		capacity: float64(capacity),
		tokens:   float64(capacity),
		rate:     rate,
		last:     time.Now(),
		now:      time.Now,
	}
}

// refill adds tokens accrued since the last call. Caller must hold mu.
func (b *TokenBucket) refill() {
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
}

// Allow reports whether one event may happen now, consuming a token if so.
func (b *TokenBucket) Allow() bool {
	return b.AllowN(1)
}

// AllowN reports whether n events may happen now, consuming n tokens if so.
func (b *TokenBucket) AllowN(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		return true
	}
	return false
}

// Wait blocks until a token is available or ctx is done.
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		b.refill()
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Tokens returns the current number of available tokens.
func (b *TokenBucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens
}

// KeyedLimiter keeps a separate TokenBucket per key (e.g. per client IP).
type KeyedLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*TokenBucket
	rate     float64
	capacity int
}

// NewKeyedLimiter creates a limiter that lazily allocates a bucket per key.
func NewKeyedLimiter(rate float64, capacity int) *KeyedLimiter {
	return &KeyedLimiter{
		buckets:  make(map[string]*TokenBucket),
		rate:     rate,
		capacity: capacity,
	}
}

// Allow reports whether an event for key may happen now.
func (k *KeyedLimiter) Allow(key string) bool {
	k.mu.Lock()
	b, ok := k.buckets[key]
	if !ok {
		b = NewTokenBucket(k.rate, k.capacity)
		k.buckets[key] = b
	}
	k.mu.Unlock()
	return b.Allow()
}
