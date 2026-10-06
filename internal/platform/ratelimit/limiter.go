package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter is an in-memory token bucket per key.
type Limiter struct {
	mu      sync.Mutex
	rate    float64 // tokens per second
	burst   float64
	buckets map[string]*bucket
}

func New(perMinute int) *Limiter {
	burst := perMinute / 4
	if burst < 5 {
		burst = 5
	}
	return &Limiter{
		rate:    float64(perMinute) / 60,
		burst:   float64(burst),
		buckets: make(map[string]*bucket),
	}
}

// Allow takes one token for key, or reports false if the bucket is empty.
func (l *Limiter) Allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Cleanup drops buckets that have been idle for a while, until ctx is cancelled.
func (l *Limiter) Cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		l.mu.Lock()
		for k, b := range l.buckets {
			if time.Since(b.last) > 10*time.Minute {
				delete(l.buckets, k)
			}
		}
		l.mu.Unlock()
	}
}
