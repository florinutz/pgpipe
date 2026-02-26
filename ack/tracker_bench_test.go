package ack_test

import (
	"fmt"
	"testing"

	"github.com/florinutz/pgcdc/ack"
)

func benchmarkAck(b *testing.B, adapterCount int) {
	tr := ack.New()
	for i := 0; i < adapterCount; i++ {
		tr.Register(fmt.Sprintf("adapter-%d", i), 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("adapter-%d", i%adapterCount)
		tr.Ack(name, uint64(i))
	}
}

func BenchmarkAck_1Adapter(b *testing.B)   { benchmarkAck(b, 1) }
func BenchmarkAck_5Adapters(b *testing.B)  { benchmarkAck(b, 5) }
func BenchmarkAck_20Adapters(b *testing.B) { benchmarkAck(b, 20) }

func BenchmarkMinAckedLSN(b *testing.B) {
	tr := ack.New()
	for i := 0; i < 10; i++ {
		tr.Register(fmt.Sprintf("adapter-%d", i), 0)
		tr.Ack(fmt.Sprintf("adapter-%d", i), uint64(i*100))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.MinAckedLSN()
	}
}
