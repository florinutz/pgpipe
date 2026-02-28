package middleware

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/event"
)

// Tracing wraps a DeliverFunc with an OpenTelemetry span per delivery.
// The span records error status on failure and links to the detector span
// stored on the event.
func Tracing(adapterName string, tracer trace.Tracer) Middleware {
	return func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			var opts []trace.SpanStartOption
			opts = append(opts,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("pgcdc.adapter", adapterName),
					attribute.String("pgcdc.event.id", ev.ID),
					attribute.String("pgcdc.channel", ev.Channel),
					attribute.String("pgcdc.operation", ev.Operation),
				),
			)

			deliverCtx := ctx
			if ev.SpanContext.IsValid() {
				opts = append(opts, trace.WithLinks(trace.Link{SpanContext: ev.SpanContext}))
				deliverCtx = trace.ContextWithRemoteSpanContext(ctx, ev.SpanContext)
			}

			deliverCtx, span := tracer.Start(deliverCtx, "pgcdc.adapter.deliver", opts...)
			defer span.End()

			err := next(deliverCtx, ev)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
	}
}
