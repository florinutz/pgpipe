package kafkaserver

import (
	"bytes"
	"encoding/json"
)

// extractKey extracts a partition key from the event payload JSON.
// It looks for the configured key column in the payload. If the key column
// is not found or the payload is not valid JSON, it falls back to the
// provided fallback string (typically event.ID).
//
// Uses a targeted scan to find the key field without parsing the entire
// JSON object. For pgcdc events the key is always at the top level.
func extractKey(payload json.RawMessage, keyColumn, fallback string) string {
	if len(payload) == 0 {
		return fallback
	}

	// Build the search needle: `"keyColumn"`
	needle := []byte(`"` + keyColumn + `"`)
	idx := bytes.Index(payload, needle)
	if idx < 0 {
		return fallback
	}

	// Skip past the key name and find the colon.
	rest := payload[idx+len(needle):]
	colonFound := false
	for i, b := range rest {
		if b == ':' {
			rest = rest[i+1:]
			colonFound = true
			break
		}
		// Only whitespace is allowed between key and colon.
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			return fallback
		}
	}
	if !colonFound {
		return fallback
	}

	// Decode just the value at this position.
	dec := json.NewDecoder(bytes.NewReader(rest))
	tok, err := dec.Token()
	if err != nil {
		return fallback
	}

	switch v := tok.(type) {
	case string:
		return v
	case float64:
		// Use raw bytes to avoid float formatting overhead.
		// Re-scan to extract the raw number token.
		return extractRawNumber(rest)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return fallback
	default:
		return fallback
	}
}

// extractRawNumber extracts the raw number string from JSON bytes,
// skipping leading whitespace. This avoids float64 formatting overhead.
func extractRawNumber(data []byte) string {
	// Skip whitespace.
	start := 0
	for start < len(data) {
		b := data[start]
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			break
		}
		start++
	}
	if start >= len(data) {
		return ""
	}
	// Scan the number token: digits, minus, plus, dot, e, E.
	end := start
	for end < len(data) {
		b := data[end]
		if (b >= '0' && b <= '9') || b == '-' || b == '+' || b == '.' || b == 'e' || b == 'E' {
			end++
		} else {
			break
		}
	}
	if end == start {
		return ""
	}
	return string(data[start:end])
}

// partitionForKey computes the partition index for a given key using FNV-1a.
func partitionForKey(key string, partitionCount int) int32 {
	if partitionCount <= 0 {
		return 0
	}
	return int32(fnv1a32(key) % uint32(partitionCount))
}

// fnv1a32 computes FNV-1a 32-bit hash inline, avoiding allocation from fnv.New32a().
func fnv1a32(s string) uint32 {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	hash := offset32
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime32
	}
	return hash
}
