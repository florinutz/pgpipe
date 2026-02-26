package kafkaserver

import (
	"encoding/json"
	"strings"
	"testing"
)

func BenchmarkExtractKey_Found(b *testing.B) {
	payload := json.RawMessage(`{"id":"order-123","name":"test"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractKey(payload, "id", "fallback")
	}
}

func BenchmarkExtractKey_NotFound(b *testing.B) {
	payload := json.RawMessage(`{"name":"test"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractKey(payload, "id", "fallback")
	}
}

func BenchmarkExtractKey_LargePayload(b *testing.B) {
	// Build a payload with many fields.
	payload := json.RawMessage(`{"id":"order-123","` + strings.Repeat(`field":1,"`, 100) + `last":true}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractKey(payload, "id", "fallback")
	}
}

func BenchmarkPartitionForKey(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		partitionForKey("order-123", 8)
	}
}
