package adaptertest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

// RunContractTests runs the standard adapter interface contract tests.
// name is the expected adapter name. startFn wraps the adapter's Start method.
func RunContractTests(t *testing.T, name string, startFn func(ctx context.Context, ch <-chan event.Event) error) {
	t.Helper()

	t.Run("name_non_empty", func(t *testing.T) {
		if name == "" {
			t.Error("adapter name must not be empty")
		}
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ch := make(chan event.Event)
		err := startFn(ctx, ch)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("closed_channel", func(t *testing.T) {
		ch := make(chan event.Event)
		close(ch)
		err := startFn(context.Background(), ch)
		if err != nil {
			t.Errorf("expected nil error on closed channel, got %v", err)
		}
	})

	t.Run("processes_event", func(t *testing.T) {
		ch := make(chan event.Event, 1)
		ch <- event.Event{
			ID:        "test-event-1",
			Channel:   "test-channel",
			Operation: "INSERT",
			Payload:   json.RawMessage(`{"key":"value"}`),
			Source:    "test",
			CreatedAt: time.Now().UTC(),
		}
		close(ch)
		err := startFn(context.Background(), ch)
		if err != nil {
			t.Errorf("expected nil error after processing event, got %v", err)
		}
	})
}

// RunCancelOnly runs only the cancel and process tests (skipping closed_channel).
// Use this for adapters whose Start() does not return on channel close (e.g. grpc server, exec restart loop).
func RunCancelOnly(t *testing.T, name string, startFn func(ctx context.Context, ch <-chan event.Event) error) {
	t.Helper()

	t.Run("name_non_empty", func(t *testing.T) {
		if name == "" {
			t.Error("adapter name must not be empty")
		}
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ch := make(chan event.Event)
		err := startFn(ctx, ch)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

// TestEvent returns a valid event for use in adapter tests.
func TestEvent() event.Event {
	return event.Event{
		ID:        "test-event-1",
		Channel:   "test-channel",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"key":"value"}`),
		Source:    "test",
		CreatedAt: time.Now().UTC(),
	}
}
