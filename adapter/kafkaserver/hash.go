package kafkaserver

import (
	"encoding/json"
	"hash/fnv"
)

// extractKey extracts a partition key from the event payload JSON.
// It looks for the configured key column in the payload. If the key column
// is not found or the payload is not valid JSON, it falls back to the
// provided fallback string (typically event.ID).
func extractKey(payload json.RawMessage, keyColumn, fallback string) string {
	if len(payload) == 0 {
		return fallback
	}

	// Fast path: try to extract the key field directly.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		return fallback
	}

	raw, ok := obj[keyColumn]
	if !ok || len(raw) == 0 {
		return fallback
	}

	// Try to unquote if it's a JSON string.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}

	// Use the raw JSON representation (numbers, booleans, etc).
	return string(raw)
}

// partitionForKey computes the partition index for a given key using FNV-1a.
func partitionForKey(key string, partitionCount int) int32 {
	if partitionCount <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int32(h.Sum32() % uint32(partitionCount))
}
