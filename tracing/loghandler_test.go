package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestTracingHandler_ValidSpanContext(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	handler := NewTracingHandler(inner)
	logger := slog.New(handler)

	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "test message")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal log output: %v", err)
	}

	if got, want := m["trace_id"], traceID.String(); got != want {
		t.Errorf("trace_id = %q, want %q", got, want)
	}
	if got, want := m["span_id"], spanID.String(); got != want {
		t.Errorf("span_id = %q, want %q", got, want)
	}
}

func TestTracingHandler_InvalidSpanContext(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	handler := NewTracingHandler(inner)
	logger := slog.New(handler)

	logger.InfoContext(context.Background(), "test message")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal log output: %v", err)
	}

	if _, ok := m["trace_id"]; ok {
		t.Error("trace_id should not be present for invalid span context")
	}
	if _, ok := m["span_id"]; ok {
		t.Error("span_id should not be present for invalid span context")
	}
}

func TestTracingHandler_WithAttrs(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	handler := NewTracingHandler(inner)

	derived := handler.WithAttrs([]slog.Attr{slog.String("component", "test")})
	if _, ok := derived.(*TracingHandler); !ok {
		t.Fatal("WithAttrs should return *TracingHandler")
	}

	logger := slog.New(derived)

	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "test")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal log output: %v", err)
	}

	if m["component"] != "test" {
		t.Error("WithAttrs attribute should be present")
	}
	if m["trace_id"] != traceID.String() {
		t.Error("trace_id should still be injected after WithAttrs")
	}
}

func TestTracingHandler_WithGroup(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, nil)
	handler := NewTracingHandler(inner)

	derived := handler.WithGroup("grp")
	if _, ok := derived.(*TracingHandler); !ok {
		t.Fatal("WithGroup should return *TracingHandler")
	}
}

func TestTracingHandler_Enabled(t *testing.T) {
	inner := slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	handler := NewTracingHandler(inner)

	if handler.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("debug should not be enabled when inner level is warn")
	}
	if !handler.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("warn should be enabled when inner level is warn")
	}
}
