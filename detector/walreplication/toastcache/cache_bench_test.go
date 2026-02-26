package toastcache

import (
	"fmt"
	"testing"
)

func BenchmarkCachePut_Empty(b *testing.B) {
	c := New(b.N+1, nil)
	row := map[string]any{"id": "1", "name": "Alice"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(1, fmt.Sprintf("%d", i), row)
	}
}

func BenchmarkCachePut_HalfFull(b *testing.B) {
	max := 10000
	c := New(max, nil)
	row := map[string]any{"id": "1", "name": "Alice"}
	for i := 0; i < max/2; i++ {
		c.Put(1, fmt.Sprintf("pre-%d", i), row)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(1, fmt.Sprintf("bench-%d", i%5000), row)
	}
}

func BenchmarkCachePut_Full_LRUEviction(b *testing.B) {
	max := 1000
	c := New(max, nil)
	row := map[string]any{"id": "1", "name": "Alice"}
	for i := 0; i < max; i++ {
		c.Put(1, fmt.Sprintf("pre-%d", i), row)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(1, fmt.Sprintf("new-%d", i), row)
	}
}

func BenchmarkCacheGet_Hit(b *testing.B) {
	c := New(1000, nil)
	c.Put(1, "target", map[string]any{"id": "target"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1, "target")
	}
}

func BenchmarkCacheGet_Miss(b *testing.B) {
	c := New(1000, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(1, "missing")
	}
}

func BenchmarkEvictRelation_100Entries(b *testing.B) {
	benchmarkEvictRelation(b, 100)
}

func BenchmarkEvictRelation_10KEntries(b *testing.B) {
	benchmarkEvictRelation(b, 10000)
}

func benchmarkEvictRelation(b *testing.B, n int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := New(n+1, nil)
		row := map[string]any{"id": "1"}
		for j := 0; j < n; j++ {
			c.Put(1, fmt.Sprintf("%d", j), row)
		}
		b.StartTimer()
		c.EvictRelation(1)
	}
}
