package view

import "encoding/json"

// normalizeNumeric converts integer types to float64 for consistent JSON
// representation. This replaces the expensive json.Marshal/json.Unmarshal
// round-trip that was previously used for type normalization.
func normalizeNumeric(v any) any {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	case json.Number:
		if f, err := n.Float64(); err == nil {
			return f
		}
		return v
	default:
		return v
	}
}
