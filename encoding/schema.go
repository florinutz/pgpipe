package encoding

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ColumnInfo describes a single column's type metadata.
// Mirrors walreplication.ColumnSchema but decoupled to avoid importing detector.
type ColumnInfo struct {
	Name     string `json:"name"`
	TypeOID  uint32 `json:"type_oid"`
	TypeName string `json:"type_name"`
}

// pgTypeToAvro maps PostgreSQL type names to their Avro schema representation.
// Each value is either a string (simple type) or a map (logical type).
var pgTypeToAvro = map[string]any{
	"bool":        "boolean",
	"int2":        "int",
	"int4":        "int",
	"int8":        "long",
	"float4":      "float",
	"float8":      "double",
	"numeric":     map[string]any{"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 18},
	"text":        "string",
	"varchar":     "string",
	"bpchar":      "string",
	"name":        "string",
	"json":        "string",
	"jsonb":       "string",
	"xml":         "string",
	"bytea":       "bytes",
	"date":        map[string]any{"type": "int", "logicalType": "date"},
	"timestamp":   map[string]any{"type": "long", "logicalType": "timestamp-micros"},
	"timestamptz": map[string]any{"type": "long", "logicalType": "timestamp-micros"},
	"time":        map[string]any{"type": "long", "logicalType": "time-micros"},
	"timetz":      map[string]any{"type": "long", "logicalType": "time-micros"},
	"interval":    "string",
	"uuid":        map[string]any{"type": "string", "logicalType": "uuid"},
	"money":       "string",
	"inet":        "string",
	"cidr":        "string",
	"macaddr":     "string",
	"bit":         "string",
	"varbit":      "string",
	"point":       "string",
	"circle":      "string",
	"tsvector":    "string",
	"tsquery":     "string",
	"int4range":   "string",
	"int8range":   "string",
	"numrange":    "string",
	"tsrange":     "string",
	"tstzrange":   "string",
	"daterange":   "string",
}

// AvroTypeForPG returns the Avro type representation for a PostgreSQL type name.
// Unknown types fall back to "string" (safe default).
func AvroTypeForPG(typeName string) any {
	if t, ok := pgTypeToAvro[typeName]; ok {
		return t
	}
	return "string"
}

// GenerateAvroSchema generates a JSON Avro schema string for a table's columns.
// Each column becomes a nullable union ["null", <type>] to handle NULLs.
func GenerateAvroSchema(namespace, table string, columns []ColumnInfo) (string, error) {
	fields := make([]map[string]any, 0, len(columns))
	for _, col := range columns {
		avroType := AvroTypeForPG(col.TypeName)
		fields = append(fields, map[string]any{
			"name":    col.Name,
			"type":    []any{"null", avroType},
			"default": nil,
		})
	}

	schema := map[string]any{
		"type":      "record",
		"name":      sanitizeAvroName(table),
		"namespace": namespace,
		"fields":    fields,
	}

	data, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("marshal avro schema: %w", err)
	}
	return string(data), nil
}

// GenerateDebeziumEnvelopeSchema generates a Debezium-compatible Avro envelope schema.
// The envelope has before/after (nullable record), op (string), ts_ms (long),
// source (record), and transaction (nullable record).
func GenerateDebeziumEnvelopeSchema(namespace, table string, columns []ColumnInfo) (string, error) {
	// Value record schema (used for before/after).
	valueFields := make([]map[string]any, 0, len(columns))
	for _, col := range columns {
		avroType := AvroTypeForPG(col.TypeName)
		valueFields = append(valueFields, map[string]any{
			"name":    col.Name,
			"type":    []any{"null", avroType},
			"default": nil,
		})
	}

	valueName := sanitizeAvroName(table) + "_Value"
	valueSchema := map[string]any{
		"type":      "record",
		"name":      valueName,
		"namespace": namespace,
		"fields":    valueFields,
	}

	sourceSchema := map[string]any{
		"type":      "record",
		"name":      "Source",
		"namespace": namespace + ".source",
		"fields": []map[string]any{
			{"name": "version", "type": "string"},
			{"name": "connector", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "ts_ms", "type": "long"},
			{"name": "db", "type": "string"},
			{"name": "schema", "type": "string"},
			{"name": "table", "type": "string"},
		},
	}

	transactionSchema := map[string]any{
		"type":      "record",
		"name":      "TransactionBlock",
		"namespace": namespace + ".transaction",
		"fields": []map[string]any{
			{"name": "id", "type": "string"},
			{"name": "total_order", "type": "long"},
			{"name": "data_collection_order", "type": "long"},
		},
	}

	envelopeSchema := map[string]any{
		"type":      "record",
		"name":      sanitizeAvroName(table) + "_Envelope",
		"namespace": namespace,
		"fields": []map[string]any{
			{"name": "before", "type": []any{"null", valueSchema}, "default": nil},
			{"name": "after", "type": []any{"null", valueName}, "default": nil},
			{"name": "op", "type": "string"},
			{"name": "ts_ms", "type": "long"},
			{"name": "source", "type": sourceSchema},
			{"name": "transaction", "type": []any{"null", transactionSchema}, "default": nil},
		},
	}

	data, err := json.Marshal(envelopeSchema)
	if err != nil {
		return "", fmt.Errorf("marshal debezium avro schema: %w", err)
	}
	return string(data), nil
}

// GenerateOpaqueSchema generates a generic Avro schema for events without column metadata.
// Payload is stored as raw bytes.
func GenerateOpaqueSchema(namespace string) (string, error) {
	schema := map[string]any{
		"type":      "record",
		"name":      "OpaqueEvent",
		"namespace": namespace,
		"fields": []map[string]any{
			{"name": "payload", "type": "bytes"},
		},
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("marshal opaque schema: %w", err)
	}
	return string(data), nil
}

// ColumnsHash returns a deterministic hash key for a set of columns.
// Used as part of the schema cache key to detect column changes.
func ColumnsHash(columns []ColumnInfo) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = fmt.Sprintf("%s:%s", c.Name, c.TypeName)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// sanitizeAvroName replaces characters invalid in Avro names with underscores.
func sanitizeAvroName(name string) string {
	var b strings.Builder
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9' && i > 0:
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}
