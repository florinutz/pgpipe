package cmd

import (
	"testing"

	"github.com/florinutz/pgcdc/internal/config"
)

func TestSpecToTransform_UnknownType(t *testing.T) {
	fn := specToTransform(config.TransformSpec{Type: "nonexistent_type"})
	if fn != nil {
		t.Error("unknown type should return nil")
	}
}

func TestSpecToTransform_AllKnownTypes(t *testing.T) {
	tests := []struct {
		name string
		spec config.TransformSpec
	}{
		{"drop_columns", config.TransformSpec{Type: "drop_columns", Columns: []string{"col1"}}},
		{"rename_fields", config.TransformSpec{Type: "rename_fields", Mapping: map[string]string{"old": "new"}}},
		{"mask", config.TransformSpec{Type: "mask", Fields: []config.MaskFieldSpec{{Field: "email", Mode: "redact"}}}},
		{"filter_operations", config.TransformSpec{Type: "filter", Filter: config.FilterSpec{Operations: []string{"INSERT"}}}},
		{"filter_field_in", config.TransformSpec{Type: "filter", Filter: config.FilterSpec{Field: "status", In: []string{"active"}}}},
		{"filter_field_equals", config.TransformSpec{Type: "filter", Filter: config.FilterSpec{Field: "status", Equals: "active"}}},
		{"debezium", config.TransformSpec{Type: "debezium"}},
		{"cloudevents", config.TransformSpec{Type: "cloudevents"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := specToTransform(tt.spec)
			if fn == nil {
				t.Errorf("specToTransform(%q) returned nil", tt.spec.Type)
			}
		})
	}
}

func TestMergeRoutes_CLIWins(t *testing.T) {
	cli := map[string][]string{"webhook": {"orders", "users"}}
	yaml := map[string][]string{"webhook": {"only_orders"}, "kafka": {"logs"}}

	merged := mergeRoutes(cli, yaml)

	if got := merged["webhook"]; len(got) != 2 || got[0] != "orders" {
		t.Errorf("CLI should win for webhook: got %v", got)
	}
	if got := merged["kafka"]; len(got) != 1 || got[0] != "logs" {
		t.Errorf("YAML kafka route should be preserved: got %v", got)
	}
}

func TestMergeRoutes_BothEmpty(t *testing.T) {
	merged := mergeRoutes(nil, nil)
	if merged != nil {
		t.Errorf("both empty should return nil, got %v", merged)
	}
}
