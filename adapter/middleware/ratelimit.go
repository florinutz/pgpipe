package middleware

import (
	"context"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/ratelimit"
)

// RateLimit wraps a DeliverFunc with token-bucket rate limiting.
// The limiter blocks until a token is available or ctx is cancelled.
func RateLimit(limiter *ratelimit.Limiter) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			if err := limiter.Wait(ctx); err != nil {
				return ctx.Err()
			}
			return next(ctx, ev)
		}
	}
}
