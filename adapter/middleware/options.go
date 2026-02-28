package middleware

import (
	"context"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"go.opentelemetry.io/otel/trace"
)

type options struct {
	deliver DeliverFunc
	dlq     dlq.DLQ
	ackFn   adapter.AckFunc
	tracer  trace.Tracer
	logger  *slog.Logger
}

// Option configures the middleware builder.
type Option func(*options)

// WithDeliver sets the inner delivery function that the middleware wraps.
func WithDeliver(fn DeliverFunc) Option {
	return func(o *options) { o.deliver = fn }
}

// WithDLQ sets the dead letter queue for terminal failures.
func WithDLQ(d dlq.DLQ) Option {
	return func(o *options) { o.dlq = d }
}

// WithAckFunc sets the cooperative checkpoint acknowledgement function.
func WithAckFunc(fn adapter.AckFunc) Option {
	return func(o *options) { o.ackFn = fn }
}

// WithTracer sets the OpenTelemetry tracer for delivery spans.
func WithTracer(t trace.Tracer) Option {
	return func(o *options) { o.tracer = t }
}

// WithLogger sets the logger for middleware components.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// nopDeliver is the default deliver function when none is provided.
func nopDeliver(_ context.Context, _ event.Event) error { return nil }
