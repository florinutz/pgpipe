package testutil_test

import (
	"testing"
	"time"

	"github.com/florinutz/pgcdc/testutil"
)

func TestLineCapture(t *testing.T) {
	c := testutil.NewLineCapture()
	_, _ = c.Write([]byte("hello\nworld\n"))

	line1 := c.WaitLine(t, time.Second)
	if line1 != "hello" {
		t.Errorf("got %q, want %q", line1, "hello")
	}
	line2 := c.WaitLine(t, time.Second)
	if line2 != "world" {
		t.Errorf("got %q, want %q", line2, "world")
	}
}

func TestMakeEvent(t *testing.T) {
	ev := testutil.MakeEvent("orders", `{"id":1}`)
	if ev.Channel != "orders" {
		t.Errorf("got channel %q, want %q", ev.Channel, "orders")
	}
	if ev.Operation != "INSERT" {
		t.Errorf("got operation %q, want %q", ev.Operation, "INSERT")
	}
	if ev.ID == "" {
		t.Error("expected non-empty event ID")
	}
}

func TestMakeEvents(t *testing.T) {
	evs := testutil.MakeEvents(3, "users", `{"name":"test"}`)
	if len(evs) != 3 {
		t.Fatalf("got %d events, want 3", len(evs))
	}
	for i, ev := range evs {
		if ev.Channel != "users" {
			t.Errorf("event[%d] channel = %q, want %q", i, ev.Channel, "users")
		}
	}
}

func TestStartTestBus(t *testing.T) {
	b := testutil.StartTestBus(t)
	if b == nil {
		t.Fatal("expected non-nil bus")
	}
}
