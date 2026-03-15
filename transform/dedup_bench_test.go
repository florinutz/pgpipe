package transform

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func BenchmarkDedup(b *testing.B) {
	for _, maxKeys := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("maxKeys_%d", maxKeys), func(b *testing.B) {
			cache := newLRUDedup(1*time.Hour, maxKeys)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Use modulo to cycle through the key space, producing a mix
				// of hits (duplicates) and misses (new keys).
				key := fmt.Sprintf("key-%d", i%maxKeys)
				cache.seen(key)
			}
		})
	}
}

func BenchmarkDedup_AllUnique(b *testing.B) {
	cache := newLRUDedup(1*time.Hour, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.seen(fmt.Sprintf("unique-%d", i))
	}
}

func BenchmarkDedup_AllDuplicate(b *testing.B) {
	cache := newLRUDedup(1*time.Hour, 100_000)
	cache.seen("same-key")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.seen("same-key")
	}
}

func BenchmarkDedup_TransformFunc(b *testing.B) {
	fn := Dedup("row.id", 1*time.Hour, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := event.Event{
			ID:        fmt.Sprintf("ev-%d", i),
			Channel:   "bench",
			Operation: "INSERT",
			Payload:   json.RawMessage(fmt.Sprintf(`{"row":{"id":"%d"}}`, i%50_000)),
		}
		_, _ = fn(ev)
	}
}
