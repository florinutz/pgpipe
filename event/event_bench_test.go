package event

import (
	"encoding/json"
	"testing"
)

func BenchmarkEventNew(b *testing.B) {
	payload := json.RawMessage(`{"id":"1","name":"Alice"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = New("test-channel", "INSERT", payload, "bench")
	}
}
