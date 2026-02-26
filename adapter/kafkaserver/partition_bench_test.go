package kafkaserver

import (
	"sync"
	"testing"
)

func BenchmarkPartitionAppend(b *testing.B) {
	p := newPartition(b.N + 1)
	rec := record{Key: []byte("key"), Value: []byte(`{"id":"1"}`)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.append(rec)
	}
}

func BenchmarkPartitionReadFrom(b *testing.B) {
	p := newPartition(10000)
	for i := 0; i < 10000; i++ {
		p.append(record{Key: []byte("key"), Value: []byte(`{"id":"1"}`)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.readFrom(0, 1024*1024)
	}
}

func BenchmarkPartitionAppendRead_Concurrent(b *testing.B) {
	p := newPartition(100000)
	var wg sync.WaitGroup
	b.ResetTimer()

	// Writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rec := record{Key: []byte("key"), Value: []byte(`{"id":"1"}`)}
		for i := 0; i < b.N; i++ {
			p.append(rec)
		}
	}()

	// Reader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			p.readFrom(0, 1024)
		}
	}()

	wg.Wait()
}
