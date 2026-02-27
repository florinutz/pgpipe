package view

import (
	"strconv"
	"strings"
)

// buildGroupKey constructs a group key string and the key values map from
// event metadata and payload using the given group-by field definitions.
// Uses strings.Builder and strconv for efficient serialization.
func buildGroupKey(groupBy []string, groupByParsed [][]string, meta EventMeta, payload map[string]any) (string, map[string]any) {
	if len(groupBy) == 0 {
		return "_global_", nil
	}

	keyValues := make(map[string]any, len(groupBy))
	var b strings.Builder
	// Pre-grow: rough estimate of 16 bytes per field.
	b.Grow(len(groupBy) * 16)

	for i, field := range groupBy {
		var val any
		if i < len(groupByParsed) && groupByParsed[i] != nil {
			val = resolveFieldParsed(field, groupByParsed[i], meta, payload)
		} else {
			val = resolveField(field, meta, payload)
		}
		keyValues[field] = val

		if i > 0 {
			b.WriteByte('\x00')
		}
		appendValue(&b, val)
	}
	return b.String(), keyValues
}

// appendValue writes a value to the builder using type-specific formatting
// to avoid fmt.Sprintf allocations.
func appendValue(b *strings.Builder, val any) {
	switch v := val.(type) {
	case string:
		b.WriteString(v)
	case float64:
		b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case int64:
		b.WriteString(strconv.FormatInt(v, 10))
	case int:
		b.WriteString(strconv.Itoa(v))
	case bool:
		if v {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case nil:
		b.WriteString("<nil>")
	default:
		// Fallback for uncommon types.
		b.WriteString("<nil>")
	}
}
