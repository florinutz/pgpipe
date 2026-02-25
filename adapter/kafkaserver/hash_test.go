package kafkaserver

import (
	"encoding/json"
	"testing"
)

func TestExtractKey(t *testing.T) {
	tests := []struct {
		name     string
		payload  json.RawMessage
		keyCol   string
		fallback string
		want     string
	}{
		{
			name:     "string field",
			payload:  json.RawMessage(`{"id":"abc-123","name":"test"}`),
			keyCol:   "id",
			fallback: "fb",
			want:     "abc-123",
		},
		{
			name:     "numeric field",
			payload:  json.RawMessage(`{"id":42,"name":"test"}`),
			keyCol:   "id",
			fallback: "fb",
			want:     "42",
		},
		{
			name:     "missing field",
			payload:  json.RawMessage(`{"name":"test"}`),
			keyCol:   "id",
			fallback: "fb",
			want:     "fb",
		},
		{
			name:     "empty payload",
			payload:  nil,
			keyCol:   "id",
			fallback: "fb",
			want:     "fb",
		},
		{
			name:     "invalid json",
			payload:  json.RawMessage(`not json`),
			keyCol:   "id",
			fallback: "fb",
			want:     "fb",
		},
		{
			name:     "custom key column",
			payload:  json.RawMessage(`{"user_id":"u-1","id":"e-1"}`),
			keyCol:   "user_id",
			fallback: "fb",
			want:     "u-1",
		},
		{
			name:     "boolean field",
			payload:  json.RawMessage(`{"id":true}`),
			keyCol:   "id",
			fallback: "fb",
			want:     "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractKey(tt.payload, tt.keyCol, tt.fallback)
			if got != tt.want {
				t.Errorf("extractKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPartitionForKey(t *testing.T) {
	// Same key should always go to the same partition.
	p1 := partitionForKey("key-1", 8)
	p2 := partitionForKey("key-1", 8)
	if p1 != p2 {
		t.Errorf("same key got different partitions: %d vs %d", p1, p2)
	}

	// Partition should be in range.
	for _, key := range []string{"a", "b", "c", "hello", "world", "12345"} {
		p := partitionForKey(key, 8)
		if p < 0 || p >= 8 {
			t.Errorf("partitionForKey(%q, 8) = %d, out of range", key, p)
		}
	}

	// Different keys should distribute across partitions (statistical, not strict).
	seen := make(map[int32]bool)
	for i := 0; i < 100; i++ {
		key := "key-" + string(rune('A'+i%26)) + string(rune('0'+i%10))
		p := partitionForKey(key, 8)
		seen[p] = true
	}
	if len(seen) < 3 {
		t.Errorf("poor distribution: only %d partitions used out of 8", len(seen))
	}
}

func TestPartitionForKeyEdgeCases(t *testing.T) {
	// Zero partitions.
	if got := partitionForKey("key", 0); got != 0 {
		t.Errorf("partitionForKey with 0 partitions: got %d, want 0", got)
	}

	// Single partition.
	if got := partitionForKey("any-key", 1); got != 0 {
		t.Errorf("partitionForKey with 1 partition: got %d, want 0", got)
	}
}
