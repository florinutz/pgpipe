package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/event"
)

func makeEvent(channel string) event.Event {
	ev, _ := event.New(channel, "INSERT", []byte(`{}`), "test")
	return ev
}

func TestBus_FastMode_DropsOnFull(t *testing.T) {
	b := bus.New(1, nil)
	// Buffer of 1. Subscribe before Start so the channel exists.
	sub, err := b.Subscribe("test")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := make(chan struct{})
	go func() {
		close(started)
		_ = b.Start(ctx)
	}()
	<-started
	// Give the bus goroutine a moment to start its select.
	time.Sleep(10 * time.Millisecond)

	// Fill the subscriber channel (buffer=1) by ingest two events fast.
	b.Ingest() <- makeEvent("a")
	b.Ingest() <- makeEvent("b")

	// Give the bus time to fan out.
	time.Sleep(30 * time.Millisecond)

	// Read one event — exactly one should be deliverable.
	select {
	case <-sub:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected at least one event in channel")
	}

	// The second event may have been dropped; if it's there, drain it.
	select {
	case <-sub:
	default:
		// dropped — that's fine in fast mode
	}
}

func TestBus_ReliableMode_BlockingDelivery(t *testing.T) {
	b := bus.New(1, nil)
	b.SetMode(bus.BusModeReliable)

	sub, err := b.Subscribe("test")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() { _ = b.Start(ctx) }()
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)

	// Send two events; the second should block until we drain.
	go func() {
		defer wg.Done()
		b.Ingest() <- makeEvent("a")
		b.Ingest() <- makeEvent("b")
	}()

	// Give the sender goroutine a moment to block on the second event.
	time.Sleep(50 * time.Millisecond)

	// Drain first event.
	select {
	case ev := <-sub:
		if ev.Channel != "a" {
			t.Errorf("expected channel a, got %s", ev.Channel)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first event not delivered")
	}

	// Now the second event should arrive.
	select {
	case ev := <-sub:
		if ev.Channel != "b" {
			t.Errorf("expected channel b, got %s", ev.Channel)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second event not delivered after draining")
	}

	wg.Wait()
}

func TestBus_ReliableMode_ContextCancellation(t *testing.T) {
	b := bus.New(1, nil)
	b.SetMode(bus.BusModeReliable)

	_, err := b.Subscribe("test")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	busErrCh := make(chan error, 1)
	go func() { busErrCh <- b.Start(ctx) }()
	time.Sleep(10 * time.Millisecond)

	// Fill the subscriber channel.
	b.Ingest() <- makeEvent("fill")
	time.Sleep(20 * time.Millisecond)

	// Try to send another event; bus will block (channel full, reliable mode).
	sent := make(chan struct{})
	go func() {
		b.Ingest() <- makeEvent("blocked")
		close(sent)
	}()
	time.Sleep(30 * time.Millisecond)

	// Cancel the context — bus should unblock and return.
	cancel()

	select {
	case err := <-busErrCh:
		if err == nil {
			t.Fatal("expected non-nil error on context cancellation")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("bus did not return after context cancellation")
	}
}
