package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func benchBusFanOut(b *testing.B, subCount int, mode BusMode) {
	bus := New(4096, nil)
	bus.SetMode(mode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < subCount; i++ {
		ch, _ := bus.Subscribe(fmt.Sprintf("sub-%d", i))
		go func() {
			for range ch {
			}
		}()
	}

	go func() { _ = bus.Start(ctx) }()

	ev := event.Event{
		ID:        "bench",
		Channel:   "test",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id":"1"}`),
		CreatedAt: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Ingest() <- ev
	}
	cancel()
}

func BenchmarkBusFanOut_1Sub(b *testing.B)  { benchBusFanOut(b, 1, BusModeFast) }
func BenchmarkBusFanOut_5Sub(b *testing.B)  { benchBusFanOut(b, 5, BusModeFast) }
func BenchmarkBusFanOut_10Sub(b *testing.B) { benchBusFanOut(b, 10, BusModeFast) }
func BenchmarkBusFanOut_20Sub(b *testing.B) { benchBusFanOut(b, 20, BusModeFast) }

func BenchmarkBusFanOut_Reliable_5Sub(b *testing.B) {
	benchBusFanOut(b, 5, BusModeReliable)
}
