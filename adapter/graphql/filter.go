package graphql

import (
	"encoding/json"

	"github.com/florinutz/pgcdc/event"
)

// Filter holds parsed subscription filter criteria.
type Filter struct {
	Channel      string         // exact channel match (empty = all)
	Operation    string         // exact operation match (empty = all)
	FieldFilters map[string]any // payload field=value filters
}

// ParseFilter extracts filter criteria from subscription variables.
// Supported variable keys:
//   - channel: string — exact channel match
//   - operation: string — exact operation match
//   - where: map[string]any — field-level payload filters
func ParseFilter(variables map[string]any) *Filter {
	f := &Filter{}
	if variables == nil {
		return f
	}

	if ch, ok := variables["channel"].(string); ok {
		f.Channel = ch
	}
	if op, ok := variables["operation"].(string); ok {
		f.Operation = op
	}
	if where, ok := variables["where"].(map[string]any); ok {
		f.FieldFilters = where
	}

	return f
}

// Matches reports whether the event passes all filter criteria.
func (f *Filter) Matches(ev event.Event) bool {
	if f == nil {
		return true
	}
	if f.Channel != "" && f.Channel != ev.Channel {
		return false
	}
	if f.Operation != "" && f.Operation != ev.Operation {
		return false
	}
	if len(f.FieldFilters) > 0 {
		return f.matchesPayload(ev)
	}
	return true
}

// matchesPayload checks field-level filters against the event payload.
func (f *Filter) matchesPayload(ev event.Event) bool {
	ev.EnsurePayload()
	if ev.Payload == nil {
		return false
	}

	var payload map[string]any
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		return false
	}

	for key, expected := range f.FieldFilters {
		actual, exists := payload[key]
		if !exists {
			return false
		}
		if !valuesEqual(expected, actual) {
			return false
		}
	}
	return true
}

// valuesEqual compares two values after JSON normalization.
// JSON numbers are always float64, so we normalize for comparison.
func valuesEqual(a, b any) bool {
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case nil:
		return b == nil
	}
	// Fallback: marshal both and compare.
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return string(aj) == string(bj)
}
