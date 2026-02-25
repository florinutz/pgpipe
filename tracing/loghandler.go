package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// TracingHandler wraps a slog.Handler and injects trace_id and span_id
// attributes when a valid OTel span context is present in the context.
type TracingHandler struct {
	inner slog.Handler
}

// NewTracingHandler creates a TracingHandler that wraps inner.
func NewTracingHandler(inner slog.Handler) *TracingHandler {
	return &TracingHandler{inner: inner}
}

func (h *TracingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *TracingHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *TracingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TracingHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *TracingHandler) WithGroup(name string) slog.Handler {
	return &TracingHandler{inner: h.inner.WithGroup(name)}
}
