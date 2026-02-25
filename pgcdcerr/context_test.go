package pgcdcerr_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

func TestWrapEvent(t *testing.T) {
	cause := fmt.Errorf("connection refused")
	ev := event.Event{
		ID:        "evt-123",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
	}

	err := pgcdcerr.WrapEvent(cause, "kafka", ev)

	want := `kafka[event=evt-123 channel=pgcdc:orders op=INSERT]: connection refused`
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

func TestWrapEvent_Unwrap(t *testing.T) {
	cause := fmt.Errorf("timeout")
	ev := event.Event{ID: "evt-1", Channel: "ch", Operation: "UPDATE"}

	err := pgcdcerr.WrapEvent(cause, "nats", ev)

	if !errors.Is(err, cause) {
		t.Error("errors.Is should match underlying cause via Unwrap")
	}
}

func TestWrapEvent_EmptyFields(t *testing.T) {
	cause := fmt.Errorf("fail")
	ev := event.Event{}

	err := pgcdcerr.WrapEvent(cause, "test", ev)

	want := `test[event= channel= op=]: fail`
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}
