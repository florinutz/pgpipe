package middleware

import (
	"context"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/event"
)

// Ack wraps a DeliverFunc with cooperative checkpoint acknowledgement.
// After the inner function completes (success or handled failure via DLQ),
// the event's LSN is acknowledged. This should be the innermost middleware
// before the actual delivery function (or just after DLQ).
func Ack(ackFn adapter.AckFunc) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			err := next(ctx, ev)
			// Ack after terminal outcome: success, DLQ'd, or intentional skip.
			// Do NOT ack on context cancellation — the event was not handled.
			if err == nil && ackFn != nil && ev.LSN > 0 {
				ackFn(ev.LSN)
			}
			return err
		}
	}
}
