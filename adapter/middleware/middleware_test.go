package middleware

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func testEvent() event.Event {
	return event.Event{
		ID:        "test-1",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   []byte(`{"row":{"id":"1"}}`),
		LSN:       100,
	}
}

func TestStack_CompositionOrder(t *testing.T) {
	var order []string

	mw1 := func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			order = append(order, "mw1-before")
			err := next(ctx, ev)
			order = append(order, "mw1-after")
			return err
		}
	}
	mw2 := func(next DeliverFunc) DeliverFunc {
		return func(ctx context.Context, ev event.Event) error {
			order = append(order, "mw2-before")
			err := next(ctx, ev)
			order = append(order, "mw2-after")
			return err
		}
	}

	inner := func(ctx context.Context, ev event.Event) error {
		order = append(order, "deliver")
		return nil
	}

	chain := Stack(mw1, mw2)(inner)
	_ = chain(context.Background(), testEvent())

	expected := []string{"mw1-before", "mw2-before", "deliver", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("got %v, want %v", order, expected)
	}
	for i, v := range order {
		if v != expected[i] {
			t.Fatalf("order[%d] = %q, want %q", i, v, expected[i])
		}
	}
}

func TestRetry_SuccessOnFirst(t *testing.T) {
	var calls int
	inner := func(ctx context.Context, ev event.Event) error {
		calls++
		return nil
	}

	chain := Retry(3, 10*time.Millisecond, 100*time.Millisecond, nil)(inner)
	err := chain(context.Background(), testEvent())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("got %d calls, want 1", calls)
	}
}

func TestRetry_SuccessOnRetry(t *testing.T) {
	var calls int
	inner := func(ctx context.Context, ev event.Event) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}

	chain := Retry(5, 10*time.Millisecond, 100*time.Millisecond, nil)(inner)
	err := chain(context.Background(), testEvent())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("got %d calls, want 3", calls)
	}
}

func TestRetry_Exhausted(t *testing.T) {
	inner := func(ctx context.Context, ev event.Event) error {
		return errors.New("always fail")
	}

	chain := Retry(3, 10*time.Millisecond, 100*time.Millisecond, nil)(inner)
	err := chain(context.Background(), testEvent())
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "always fail" {
		t.Fatalf("got %q, want %q", err.Error(), "always fail")
	}
}

func TestRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls int
	inner := func(ctx context.Context, ev event.Event) error {
		calls++
		return errors.New("transient")
	}

	chain := Retry(5, 10*time.Millisecond, 100*time.Millisecond, nil)(inner)
	err := chain(ctx, testEvent())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestDLQ_RecordsTerminalError(t *testing.T) {
	var recorded bool
	d := &mockDLQ{recordFn: func() { recorded = true }}

	inner := func(ctx context.Context, ev event.Event) error {
		return errors.New("terminal")
	}

	chain := DLQ("test", d)(inner)
	err := chain(context.Background(), testEvent())
	if err != nil {
		t.Fatalf("expected nil error (DLQ swallows), got %v", err)
	}
	if !recorded {
		t.Fatal("expected DLQ record")
	}
}

func TestDLQ_PassesSuccess(t *testing.T) {
	var recorded bool
	d := &mockDLQ{recordFn: func() { recorded = true }}

	inner := func(ctx context.Context, ev event.Event) error {
		return nil
	}

	chain := DLQ("test", d)(inner)
	err := chain(context.Background(), testEvent())
	if err != nil {
		t.Fatal(err)
	}
	if recorded {
		t.Fatal("should not record to DLQ on success")
	}
}

func TestAck_AcksOnSuccess(t *testing.T) {
	var ackedLSN uint64
	ackFn := func(lsn uint64) { ackedLSN = lsn }

	inner := func(ctx context.Context, ev event.Event) error {
		return nil
	}

	chain := Ack(ackFn)(inner)
	ev := testEvent()
	_ = chain(context.Background(), ev)
	if ackedLSN != ev.LSN {
		t.Fatalf("got LSN %d, want %d", ackedLSN, ev.LSN)
	}
}

func TestAck_NoAckOnError(t *testing.T) {
	var acked bool
	ackFn := func(lsn uint64) { acked = true }

	inner := func(ctx context.Context, ev event.Event) error {
		return errors.New("fail")
	}

	chain := Ack(ackFn)(inner)
	_ = chain(context.Background(), testEvent())
	if acked {
		t.Fatal("should not ack on error")
	}
}

func TestStartLoop_ChannelClose(t *testing.T) {
	ch := make(chan event.Event, 1)
	ch <- testEvent()
	close(ch)

	var delivered int32
	deliver := func(ctx context.Context, ev event.Event) error {
		atomic.AddInt32(&delivered, 1)
		return nil
	}

	err := StartLoop(context.Background(), ch, deliver, nil)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&delivered) != 1 {
		t.Fatalf("got %d, want 1", atomic.LoadInt32(&delivered))
	}
}

func TestStartLoop_ContextCancel(t *testing.T) {
	ch := make(chan event.Event)
	ctx, cancel := context.WithCancel(context.Background())

	deliver := func(ctx context.Context, ev event.Event) error {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- StartLoop(ctx, ch, deliver, nil) }()

	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestBuild_WithAllMiddleware(t *testing.T) {
	var delivered bool
	cfg := Config{
		Retry:          &RetryConfig{MaxRetries: 2, BackoffBase: 10 * time.Millisecond, BackoffCap: 50 * time.Millisecond},
		CircuitBreaker: &CircuitBreakerConfig{MaxFailures: 5, ResetTimeout: 1 * time.Second},
		RateLimit:      &RateLimitConfig{EventsPerSecond: 1000, Burst: 100},
	}

	chain := Build("test", cfg,
		WithDeliver(func(ctx context.Context, ev event.Event) error {
			delivered = true
			return nil
		}),
	)

	err := chain(context.Background(), testEvent())
	if err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Fatal("expected delivery")
	}
}

func TestBuild_NoConfig(t *testing.T) {
	var delivered bool
	chain := Build("test", Config{},
		WithDeliver(func(ctx context.Context, ev event.Event) error {
			delivered = true
			return nil
		}),
	)

	err := chain(context.Background(), testEvent())
	if err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Fatal("expected delivery")
	}
}

// mockDLQ is a test helper.
type mockDLQ struct {
	recordFn func()
}

func (d *mockDLQ) Record(_ context.Context, _ event.Event, _ string, _ error) error {
	if d.recordFn != nil {
		d.recordFn()
	}
	return nil
}

func (d *mockDLQ) Close() error { return nil }
