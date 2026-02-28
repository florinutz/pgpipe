package middleware

import (
	"context"

	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/circuitbreaker"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// CircuitBreak wraps a DeliverFunc with a circuit breaker. When the circuit is
// open, events are sent to the DLQ (if configured) and skipped. Success/failure
// outcomes from the inner function update the breaker state.
func CircuitBreak(adapterName string, cb *circuitbreaker.CircuitBreaker, d dlq.DLQ) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			if !cb.Allow() {
				metrics.CircuitBreakerState.WithLabelValues(adapterName).Set(1) // open
				if d != nil {
					_ = d.Record(ctx, ev, adapterName, &pgcdcerr.CircuitBreakerOpenError{Adapter: adapterName})
				}
				// Return nil — event is handled (DLQ'd or dropped).
				return nil
			}

			err := next(ctx, ev)
			if err != nil {
				cb.RecordFailure()
			} else {
				cb.RecordSuccess()
				metrics.CircuitBreakerState.WithLabelValues(adapterName).Set(0) // closed
			}
			return err
		}
	}
}
