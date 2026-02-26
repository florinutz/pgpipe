package bus

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func makeStressEvent(id string) event.Event {
	return event.Event{
		ID:        id,
		Channel:   "test",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id":"` + id + `"}`),
		CreatedAt: time.Now(),
	}
}

func TestBus_StressFastMode(t *testing.T) {
	b := New(256, nil)
	b.SetMode(BusModeFast)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received [10]atomic.Int64
	for i := 0; i < 10; i++ {
		ch, err := b.Subscribe("sub-" + string(rune('A'+i)))
		if err != nil {
			t.Fatal(err)
		}
		idx := i
		go func() {
			for range ch {
				received[idx].Add(1)
			}
		}()
	}

	go func() { _ = b.Start(ctx) }()

	const total = 100000
	for i := 0; i < total; i++ {
		b.Ingest() <- makeStressEvent("ev")
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 10; i++ {
		if received[i].Load() == 0 {
			t.Errorf("subscriber %d received 0 events", i)
		}
	}
}

func TestBus_StressReliableMode(t *testing.T) {
	b := New(32, nil)
	b.SetMode(BusModeReliable)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received [5]atomic.Int64
	for i := 0; i < 5; i++ {
		ch, err := b.Subscribe("sub-" + string(rune('A'+i)))
		if err != nil {
			t.Fatal(err)
		}
		idx := i
		go func() {
			for range ch {
				received[idx].Add(1)
				if idx == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}()
	}

	go func() { _ = b.Start(ctx) }()

	const total = 10000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			select {
			case b.Ingest() <- makeStressEvent("ev"):
			case <-ctx.Done():
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("producer timed out (possible deadlock)")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestBus_StressSubscribeDuringFlood(t *testing.T) {
	b := New(128, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = b.Start(ctx) }()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			select {
			case b.Ingest() <- makeStressEvent("ev"):
			case <-ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			time.Sleep(time.Duration(idx) * time.Millisecond)
			ch, err := b.SubscribeWithFilter("late-"+string(rune('A'+idx)), nil)
			if err != nil {
				return
			}
			for range ch {
			}
		}(i)
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()
}
