package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func BenchmarkMiddlewareChain(b *testing.B) {
	cfg := Config{
		Retry:          &RetryConfig{MaxRetries: 3, BackoffBase: 10 * time.Millisecond, BackoffCap: 1 * time.Second},
		CircuitBreaker: &CircuitBreakerConfig{MaxFailures: 5, ResetTimeout: 5 * time.Second},
		RateLimit:      &RateLimitConfig{EventsPerSecond: 1_000_000, Burst: 100_000},
		Timeout:        5 * time.Second,
	}

	chain := Build("bench", cfg,
		WithDeliver(func(_ context.Context, _ event.Event) error {
			return nil
		}),
		WithAckFunc(func(_ uint64) {}),
	)

	ev := event.Event{
		ID:        "bench-1",
		Channel:   "pgcdc:bench",
		Operation: "INSERT",
		Payload:   []byte(`{"row":{"id":"1","name":"Alice"}}`),
		LSN:       42,
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chain(ctx, ev)
	}
}

func BenchmarkMiddlewareChain_MetricsOnly(b *testing.B) {
	chain := Build("bench-minimal", Config{},
		WithDeliver(func(_ context.Context, _ event.Event) error {
			return nil
		}),
	)

	ev := event.Event{
		ID:        "bench-1",
		Channel:   "pgcdc:bench",
		Operation: "INSERT",
		Payload:   []byte(`{"row":{"id":"1"}}`),
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chain(ctx, ev)
	}
}
