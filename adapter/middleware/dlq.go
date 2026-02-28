package middleware

import (
	"context"

	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
)

// DLQ wraps a DeliverFunc with dead letter queue recording. Terminal errors
// from the inner function are recorded to the DLQ and swallowed (returns nil)
// so the event is considered handled and the pipeline can continue.
func DLQ(adapterName string, d dlq.DLQ) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			err := next(ctx, ev)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Terminal delivery failure — record to DLQ and swallow.
				_ = d.Record(ctx, ev, adapterName, err)
				return nil
			}
			return nil
		}
	}
}
