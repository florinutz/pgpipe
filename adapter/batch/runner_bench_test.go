package batch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/testutil"
)

// nopBatcher returns success immediately.
type nopBatcher struct{}

func (nopBatcher) Flush(_ context.Context, batch []event.Event) adapter.FlushResult {
	return adapter.FlushResult{Delivered: len(batch)}
}

func (nopBatcher) BatchConfig() adapter.BatchConfig {
	return adapter.BatchConfig{MaxSize: 1000, FlushInterval: time.Minute}
}

func benchBatchFlush(b *testing.B, batchSize int) {
	b.Helper()
	r := New("bench", nopBatcher{}, batchSize, time.Hour, nil)

	evs := testutil.MakeEvents(batchSize, "bench", `{"id":"1"}`)

	ch := make(chan event.Event, batchSize+1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ev := range evs {
			ch <- ev
		}
		// The channel close triggers a final flush for any remainder,
		// but we rely on size-trigger flush here.
		// Drain any leftover by closing and restarting.
		close(ch)
		_ = r.Start(context.Background(), ch)

		// Re-create channel and runner for next iteration.
		ch = make(chan event.Event, batchSize+1)
		r = New("bench", nopBatcher{}, batchSize, time.Hour, nil)
	}
}

func BenchmarkBatchFlush(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			benchBatchFlush(b, size)
		})
	}
}
