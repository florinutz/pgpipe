package view

import (
	"testing"
)

func TestResolveField(t *testing.T) {
	meta := EventMeta{
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Source:    "wal_replication",
	}
	payload := map[string]any{
		"amount": 42.5,
		"region": "us-east",
		"nested": map[string]any{
			"deep": "value",
		},
	}

	tests := []struct {
		field string
		want  any
	}{
		{"channel", "pgcdc:orders"},
		{"operation", "INSERT"},
		{"source", "wal_replication"},
		{"payload.amount", 42.5},
		{"payload.region", "us-east"},
		{"payload.nested.deep", "value"},
		{"payload.nonexistent", nil},
		{"unknown_field", nil},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			got := resolveField(tt.field, meta, payload)
			if got != tt.want {
				t.Errorf("resolveField(%q) = %v (%T), want %v (%T)", tt.field, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		a, b any
		want int
	}{
		{int64(5), int64(5), 0},
		{int64(3), int64(5), -1},
		{int64(5), int64(3), 1},
		{float64(3.14), float64(3.14), 0},
		{float64(1.0), float64(2.0), -1},
		{"abc", "abc", 0},
		{"abc", "def", -1},
		{nil, nil, 0},
		{nil, "x", -2},
		{"x", nil, -2},
		// Cross-type numeric.
		{int64(5), float64(5.0), 0},
		{float64(3.0), int64(5), -1},
	}

	for _, tt := range tests {
		got := compareValues(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("compareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestPredicateWhere(t *testing.T) {
	// Parse a query with a WHERE clause and test the predicate.
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events WHERE channel = 'pgcdc:orders' AND operation = 'INSERT' TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	if def.Where == nil {
		t.Fatal("WHERE predicate is nil")
	}

	tests := []struct {
		name string
		meta EventMeta
		want bool
	}{
		{
			"match both",
			EventMeta{Channel: "pgcdc:orders", Operation: "INSERT"},
			true,
		},
		{
			"wrong channel",
			EventMeta{Channel: "pgcdc:users", Operation: "INSERT"},
			false,
		},
		{
			"wrong operation",
			EventMeta{Channel: "pgcdc:orders", Operation: "UPDATE"},
			false,
		},
		{
			"wrong both",
			EventMeta{Channel: "pgcdc:users", Operation: "DELETE"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := def.Where(tt.meta, nil)
			if got != tt.want {
				t.Errorf("Where(%v) = %v, want %v", tt.meta, got, tt.want)
			}
		})
	}
}

func TestPredicateOR(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events WHERE operation = 'INSERT' OR operation = 'UPDATE' TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		op   string
		want bool
	}{
		{"INSERT", true},
		{"UPDATE", true},
		{"DELETE", false},
	}

	for _, tt := range tests {
		got := def.Where(EventMeta{Operation: tt.op}, nil)
		if got != tt.want {
			t.Errorf("OR predicate(op=%s) = %v, want %v", tt.op, got, tt.want)
		}
	}
}

func TestPredicateComparison(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events WHERE payload.amount > 100 TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		amount any
		want   bool
	}{
		{float64(200), true},
		{float64(100), false},
		{float64(50), false},
	}

	for _, tt := range tests {
		payload := map[string]any{"amount": tt.amount}
		got := def.Where(EventMeta{}, payload)
		if got != tt.want {
			t.Errorf("amount=%v: got %v, want %v", tt.amount, got, tt.want)
		}
	}
}

func TestPredicateIsNull(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events WHERE payload.deleted_at IS NULL TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		payload map[string]any
		want    bool
	}{
		{map[string]any{}, true},
		{map[string]any{"deleted_at": nil}, true},
		{map[string]any{"deleted_at": "2024-01-01"}, false},
	}

	for _, tt := range tests {
		got := def.Where(EventMeta{}, tt.payload)
		if got != tt.want {
			t.Errorf("payload=%v: got %v, want %v", tt.payload, got, tt.want)
		}
	}
}

func TestPredicateIsNotNull(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events WHERE payload.region IS NOT NULL TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		payload map[string]any
		want    bool
	}{
		{map[string]any{"region": "us-east"}, true},
		{map[string]any{}, false},
		{map[string]any{"region": nil}, false},
	}

	for _, tt := range tests {
		got := def.Where(EventMeta{}, tt.payload)
		if got != tt.want {
			t.Errorf("payload=%v: got %v, want %v", tt.payload, got, tt.want)
		}
	}
}
