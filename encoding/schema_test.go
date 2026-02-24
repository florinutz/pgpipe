package encoding

import (
	"encoding/json"
	"testing"
)

func TestAvroTypeForPG(t *testing.T) {
	tests := []struct {
		pg   string
		want any
	}{
		{"bool", "boolean"},
		{"int2", "int"},
		{"int4", "int"},
		{"int8", "long"},
		{"float4", "float"},
		{"float8", "double"},
		{"text", "string"},
		{"varchar", "string"},
		{"jsonb", "string"},
		{"bytea", "bytes"},
		{"uuid", map[string]any{"type": "string", "logicalType": "uuid"}},
		{"timestamp", map[string]any{"type": "long", "logicalType": "timestamp-micros"}},
		{"timestamptz", map[string]any{"type": "long", "logicalType": "timestamp-micros"}},
		{"date", map[string]any{"type": "int", "logicalType": "date"}},
		{"unknown_type", "string"}, // fallback
	}

	for _, tt := range tests {
		t.Run(tt.pg, func(t *testing.T) {
			got := AvroTypeForPG(tt.pg)
			// Compare as JSON to handle map equality.
			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(tt.want)
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("AvroTypeForPG(%q) = %s, want %s", tt.pg, gotJSON, wantJSON)
			}
		})
	}
}

func TestGenerateAvroSchema(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeOID: 23, TypeName: "int4"},
		{Name: "name", TypeOID: 25, TypeName: "text"},
		{Name: "active", TypeOID: 16, TypeName: "bool"},
	}

	schemaJSON, err := GenerateAvroSchema("pgcdc", "users", columns)
	if err != nil {
		t.Fatalf("GenerateAvroSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}

	if schema["type"] != "record" {
		t.Errorf("type = %v, want record", schema["type"])
	}
	if schema["name"] != "users" {
		t.Errorf("name = %v, want users", schema["name"])
	}
	if schema["namespace"] != "pgcdc" {
		t.Errorf("namespace = %v, want pgcdc", schema["namespace"])
	}

	fields, ok := schema["fields"].([]any)
	if !ok {
		t.Fatalf("fields is not an array")
	}
	if len(fields) != 3 {
		t.Fatalf("len(fields) = %d, want 3", len(fields))
	}

	// First field should be id with nullable int.
	f0 := fields[0].(map[string]any)
	if f0["name"] != "id" {
		t.Errorf("field[0].name = %v, want id", f0["name"])
	}
	typeArr := f0["type"].([]any)
	if typeArr[0] != "null" {
		t.Errorf("field[0].type[0] = %v, want null", typeArr[0])
	}
	if typeArr[1] != "int" {
		t.Errorf("field[0].type[1] = %v, want int", typeArr[1])
	}
}

func TestGenerateDebeziumEnvelopeSchema(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id", TypeOID: 23, TypeName: "int4"},
		{Name: "name", TypeOID: 25, TypeName: "text"},
	}

	schemaJSON, err := GenerateDebeziumEnvelopeSchema("pgcdc", "orders", columns)
	if err != nil {
		t.Fatalf("GenerateDebeziumEnvelopeSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}

	if schema["type"] != "record" {
		t.Errorf("type = %v, want record", schema["type"])
	}
	if schema["name"] != "orders_Envelope" {
		t.Errorf("name = %v, want orders_Envelope", schema["name"])
	}

	fields, _ := schema["fields"].([]any)
	if len(fields) != 6 {
		t.Fatalf("len(fields) = %d, want 6 (before, after, op, ts_ms, source, transaction)", len(fields))
	}

	// Check field names.
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.(map[string]any)["name"].(string)
	}
	expected := []string{"before", "after", "op", "ts_ms", "source", "transaction"}
	for i, want := range expected {
		if names[i] != want {
			t.Errorf("field[%d].name = %q, want %q", i, names[i], want)
		}
	}
}

func TestGenerateOpaqueSchema(t *testing.T) {
	schemaJSON, err := GenerateOpaqueSchema("pgcdc")
	if err != nil {
		t.Fatalf("GenerateOpaqueSchema: %v", err)
	}

	var schema map[string]any
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}

	if schema["name"] != "OpaqueEvent" {
		t.Errorf("name = %v, want OpaqueEvent", schema["name"])
	}

	fields := schema["fields"].([]any)
	if len(fields) != 1 {
		t.Fatalf("len(fields) = %d, want 1", len(fields))
	}
	f0 := fields[0].(map[string]any)
	if f0["name"] != "payload" {
		t.Errorf("field[0].name = %v, want payload", f0["name"])
	}
}

func TestColumnsHash(t *testing.T) {
	cols1 := []ColumnInfo{
		{Name: "id", TypeName: "int4"},
		{Name: "name", TypeName: "text"},
	}
	cols2 := []ColumnInfo{
		{Name: "name", TypeName: "text"},
		{Name: "id", TypeName: "int4"},
	}

	// Order should not matter — hash is sorted.
	if ColumnsHash(cols1) != ColumnsHash(cols2) {
		t.Error("ColumnsHash should be order-independent")
	}

	// Different columns should produce different hashes.
	cols3 := []ColumnInfo{
		{Name: "id", TypeName: "int8"},
		{Name: "name", TypeName: "text"},
	}
	if ColumnsHash(cols1) == ColumnsHash(cols3) {
		t.Error("ColumnsHash should differ for different types")
	}
}

func TestSanitizeAvroName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"orders", "orders"},
		{"public.orders", "public_orders"},
		{"my-table", "my_table"},
		{"123start", "_23start"},
		{"", "_"},
	}
	for _, tt := range tests {
		got := sanitizeAvroName(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeAvroName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
