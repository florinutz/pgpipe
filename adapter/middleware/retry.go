package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
)

// Retry wraps a DeliverFunc with exponential backoff retry. maxRetries is the
// total number of attempts (including the first). Errors from the inner function
// are retried; nil or context errors pass through immediately.
func Retry(maxRetries int, base, cap time.Duration, logger *slog.Logger) Middleware {
	if maxRetries <= 1 {
		return func(next DeliverFunc) DeliverFunc { return next }
	}
	if base <= 0 {
		base = 1 * time.Second
	}
	if cap <= 0 {
		cap = 32 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}

	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			var lastErr error
			for attempt := range maxRetries {
				if attempt > 0 {
					metrics.MiddlewareRetries.Inc()
					wait := backoff.Jitter(attempt, base, cap)
					logger.Info("retrying delivery",
						"event_id", ev.ID,
						"attempt", attempt+1,
						"backoff", wait,
					)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(wait):
					}
				}

				lastErr = next(ctx, ev)
				if lastErr == nil {
					return nil
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			return lastErr
		}
	}
}
