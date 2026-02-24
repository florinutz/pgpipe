package encoding

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/hamba/avro/v2"

	"github.com/florinutz/pgcdc/encoding/registry"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

// AvroEncoder encodes event payloads as binary Avro with optional Schema Registry integration.
type AvroEncoder struct {
	registry  *registry.Client // nil = no registry
	cache     *SchemaCache
	namespace string
	logger    *slog.Logger
}

// AvroOption configures an AvroEncoder.
type AvroOption func(*AvroEncoder)

// WithRegistry configures Schema Registry integration.
func WithRegistry(client *registry.Client) AvroOption {
	return func(e *AvroEncoder) { e.registry = client }
}

// WithNamespace sets the Avro schema namespace (default: "pgcdc").
func WithNamespace(ns string) AvroOption {
	return func(e *AvroEncoder) { e.namespace = ns }
}

// NewAvroEncoder creates an Avro encoder.
func NewAvroEncoder(logger *slog.Logger, opts ...AvroOption) *AvroEncoder {
	if logger == nil {
		logger = slog.Default()
	}
	e := &AvroEncoder{
		cache:     NewSchemaCache(),
		namespace: "pgcdc",
		logger:    logger.With("component", "avro_encoder"),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *AvroEncoder) ContentType() string { return "application/avro" }

func (e *AvroEncoder) Close() error { return nil }

// Encode encodes the data (event payload JSON) into binary Avro.
// If column metadata is present in the event (via --include-schema), typed schemas are generated.
// Otherwise, falls back to opaque mode (raw bytes).
// If a registry is configured, schemas are registered and wire-format headers prepended.
func (e *AvroEncoder) Encode(ev event.Event, data []byte) ([]byte, error) {
	// Parse the payload to inspect structure.
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		// Not JSON object — treat as opaque bytes.
		return e.encodeOpaque(ev, data)
	}

	// Detect Debezium envelope (has before/after/op/source keys).
	if isDebeziumEnvelope(payload) {
		return e.encodeDebezium(ev, payload)
	}

	// Try to extract column metadata from event payload.
	columns := extractColumns(payload)
	table, schema := extractTableInfo(ev)

	if len(columns) == 0 {
		return e.encodeOpaque(ev, data)
	}

	return e.encodeTyped(ev, payload, schema, table, columns)
}

// encodeTyped generates a per-table Avro schema from column metadata and encodes the payload.
func (e *AvroEncoder) encodeTyped(ev event.Event, payload map[string]any, namespace, table string, columns []ColumnInfo) ([]byte, error) {
	if namespace == "" {
		namespace = e.namespace
	}
	cacheKey := namespace + "." + table + ":" + ColumnsHash(columns)

	entry, ok := e.cache.Get(cacheKey)
	if !ok {
		schemaJSON, err := GenerateAvroSchema(namespace, table, columns)
		if err != nil {
			return nil, fmt.Errorf("generate avro schema: %w", err)
		}
		entry = SchemaCacheEntry{Schema: schemaJSON}

		if e.registry != nil {
			subject := topicForEvent(ev) + "-value"
			id, regErr := e.registry.Register(context.Background(), subject, schemaJSON, registry.SchemaTypeAvro)
			if regErr != nil {
				metrics.SchemaRegistryErrors.Inc()
				return nil, fmt.Errorf("register avro schema: %w", regErr)
			}
			metrics.SchemaRegistryRegistrations.Inc()
			entry.RegistryID = id
		}

		e.cache.Put(cacheKey, entry)
	}

	parsed, err := avro.Parse(entry.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}

	// Prepare data: extract the row data, coerce types for Avro.
	rowData := extractRowData(payload)
	record := coerceForAvro(rowData, columns)

	encoded, err := avro.Marshal(parsed, record)
	if err != nil {
		return nil, fmt.Errorf("avro marshal: %w", err)
	}

	metrics.EncodingEncoded.Inc()

	if e.registry != nil && entry.RegistryID > 0 {
		return registry.WireEncode(entry.RegistryID, encoded), nil
	}
	return encoded, nil
}

// encodeDebezium generates a Debezium-compatible Avro envelope schema.
func (e *AvroEncoder) encodeDebezium(ev event.Event, payload map[string]any) ([]byte, error) {
	table, namespace := extractTableInfo(ev)
	if namespace == "" {
		namespace = e.namespace
	}

	// Extract columns from the "after" or "before" value record if present.
	columns := extractColumnsFromDebezium(payload)

	if len(columns) == 0 {
		// No typed columns available — encode as generic JSON bytes.
		return e.encodeOpaquePayload(ev, payload)
	}

	cacheKey := "dbz." + namespace + "." + table + ":" + ColumnsHash(columns)

	entry, ok := e.cache.Get(cacheKey)
	if !ok {
		schemaJSON, err := GenerateDebeziumEnvelopeSchema(namespace, table, columns)
		if err != nil {
			return nil, fmt.Errorf("generate debezium avro schema: %w", err)
		}
		entry = SchemaCacheEntry{Schema: schemaJSON}

		if e.registry != nil {
			subject := topicForEvent(ev) + "-value"
			id, regErr := e.registry.Register(context.Background(), subject, schemaJSON, registry.SchemaTypeAvro)
			if regErr != nil {
				metrics.SchemaRegistryErrors.Inc()
				return nil, fmt.Errorf("register debezium avro schema: %w", regErr)
			}
			metrics.SchemaRegistryRegistrations.Inc()
			entry.RegistryID = id
		}

		e.cache.Put(cacheKey, entry)
	}

	parsed, err := avro.Parse(entry.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse debezium avro schema: %w", err)
	}

	record := coerceDebeziumForAvro(payload, columns)

	encoded, err := avro.Marshal(parsed, record)
	if err != nil {
		return nil, fmt.Errorf("avro marshal debezium: %w", err)
	}

	metrics.EncodingEncoded.Inc()

	if e.registry != nil && entry.RegistryID > 0 {
		return registry.WireEncode(entry.RegistryID, encoded), nil
	}
	return encoded, nil
}

// encodeOpaque encodes data as opaque bytes using a generic schema.
func (e *AvroEncoder) encodeOpaque(ev event.Event, data []byte) ([]byte, error) {
	cacheKey := "opaque:" + e.namespace

	entry, ok := e.cache.Get(cacheKey)
	if !ok {
		schemaJSON, err := GenerateOpaqueSchema(e.namespace)
		if err != nil {
			return nil, fmt.Errorf("generate opaque schema: %w", err)
		}
		entry = SchemaCacheEntry{Schema: schemaJSON}

		if e.registry != nil {
			subject := topicForEvent(ev) + "-value"
			id, regErr := e.registry.Register(context.Background(), subject, schemaJSON, registry.SchemaTypeAvro)
			if regErr != nil {
				metrics.SchemaRegistryErrors.Inc()
				return nil, fmt.Errorf("register opaque schema: %w", regErr)
			}
			metrics.SchemaRegistryRegistrations.Inc()
			entry.RegistryID = id
		}

		e.cache.Put(cacheKey, entry)
	}

	parsed, err := avro.Parse(entry.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse opaque schema: %w", err)
	}

	record := map[string]any{"payload": data}
	encoded, err := avro.Marshal(parsed, record)
	if err != nil {
		return nil, fmt.Errorf("avro marshal opaque: %w", err)
	}

	metrics.EncodingEncoded.Inc()

	if e.registry != nil && entry.RegistryID > 0 {
		return registry.WireEncode(entry.RegistryID, encoded), nil
	}
	return encoded, nil
}

func (e *AvroEncoder) encodeOpaquePayload(ev event.Event, payload map[string]any) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return e.encodeOpaque(ev, data)
}

// isDebeziumEnvelope checks if a payload looks like a Debezium envelope.
func isDebeziumEnvelope(payload map[string]any) bool {
	_, hasOp := payload["op"]
	_, hasSource := payload["source"]
	// Debezium envelopes always have "op" and "source".
	return hasOp && hasSource
}

// extractColumns extracts column metadata from the payload's "columns" field
// (added by --include-schema).
func extractColumns(payload map[string]any) []ColumnInfo {
	raw, ok := payload["columns"]
	if !ok {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var cols []ColumnInfo
	if err := json.Unmarshal(data, &cols); err != nil {
		return nil
	}
	return cols
}

// extractColumnsFromDebezium extracts column metadata from a Debezium envelope.
// Looks in source.columns if present.
func extractColumnsFromDebezium(payload map[string]any) []ColumnInfo {
	// Check top-level "columns" first (pgcdc adds it before Debezium transform).
	if cols := extractColumns(payload); len(cols) > 0 {
		return cols
	}
	// Check source.columns (some configurations).
	if source, ok := payload["source"].(map[string]any); ok {
		if raw, ok := source["columns"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return nil
			}
			var cols []ColumnInfo
			if err := json.Unmarshal(data, &cols); err != nil {
				return nil
			}
			return cols
		}
	}
	return nil
}

// extractTableInfo extracts table and schema from an event's channel.
// "pgcdc:public.orders" → table="orders", schema="public"
// "pgcdc:orders" → table="orders", schema=""
func extractTableInfo(ev event.Event) (table, schema string) {
	ch := ev.Channel
	// Strip "pgcdc:" prefix.
	if idx := strings.Index(ch, ":"); idx >= 0 {
		ch = ch[idx+1:]
	}
	if idx := strings.LastIndex(ch, "."); idx >= 0 {
		return ch[idx+1:], ch[:idx]
	}
	return ch, ""
}

// extractRowData extracts the actual row data from the payload.
// If the payload has a "row" or "data" key wrapping the columns, unwrap it.
// Otherwise return the payload minus metadata keys.
func extractRowData(payload map[string]any) map[string]any {
	// Remove metadata keys that are not row data.
	result := make(map[string]any, len(payload))
	for k, v := range payload {
		switch k {
		case "columns", "schema", "table":
			// Skip metadata.
		default:
			result[k] = v
		}
	}
	return result
}

// coerceForAvro converts JSON-decoded values to types suitable for Avro encoding.
// JSON numbers are float64 by default; we need to match the expected Avro types.
func coerceForAvro(data map[string]any, columns []ColumnInfo) map[string]any {
	result := make(map[string]any, len(columns))
	for _, col := range columns {
		v, exists := data[col.Name]
		if !exists || v == nil {
			result[col.Name] = nil
			continue
		}
		result[col.Name] = coerceValue(v, col.TypeName)
	}
	return result
}

// coerceDebeziumForAvro prepares a Debezium envelope for Avro encoding.
func coerceDebeziumForAvro(payload map[string]any, columns []ColumnInfo) map[string]any {
	result := make(map[string]any, len(payload))

	if before, ok := payload["before"]; ok && before != nil {
		if m, ok := before.(map[string]any); ok {
			result["before"] = coerceForAvro(m, columns)
		} else {
			result["before"] = nil
		}
	} else {
		result["before"] = nil
	}

	if after, ok := payload["after"]; ok && after != nil {
		if m, ok := after.(map[string]any); ok {
			result["after"] = coerceForAvro(m, columns)
		} else {
			result["after"] = nil
		}
	} else {
		result["after"] = nil
	}

	if op, ok := payload["op"]; ok {
		result["op"] = fmt.Sprint(op)
	}

	if tsMs, ok := payload["ts_ms"]; ok {
		result["ts_ms"] = coerceToLong(tsMs)
	} else {
		result["ts_ms"] = int64(0)
	}

	if source, ok := payload["source"].(map[string]any); ok {
		result["source"] = coerceSource(source)
	} else {
		result["source"] = map[string]any{
			"version": "", "connector": "", "name": "",
			"ts_ms": int64(0), "db": "", "schema": "", "table": "",
		}
	}

	if tx, ok := payload["transaction"].(map[string]any); ok {
		result["transaction"] = map[string]any{
			"id":                    fmt.Sprint(tx["id"]),
			"total_order":           coerceToLong(tx["total_order"]),
			"data_collection_order": coerceToLong(tx["data_collection_order"]),
		}
	} else {
		result["transaction"] = nil
	}

	return result
}

func coerceSource(source map[string]any) map[string]any {
	return map[string]any{
		"version":   stringOr(source, "version", ""),
		"connector": stringOr(source, "connector", ""),
		"name":      stringOr(source, "name", ""),
		"ts_ms":     coerceToLong(source["ts_ms"]),
		"db":        stringOr(source, "db", ""),
		"schema":    stringOr(source, "schema", ""),
		"table":     stringOr(source, "table", ""),
	}
}

func stringOr(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprint(v)
	}
	return def
}

// coerceValue converts a JSON-decoded value to a type matching the Avro schema.
func coerceValue(v any, pgType string) any {
	switch pgType {
	case "bool":
		if b, ok := v.(bool); ok {
			return b
		}
		return v
	case "int2", "int4":
		return coerceToInt(v)
	case "int8":
		return coerceToLong(v)
	case "float4":
		return coerceToFloat(v)
	case "float8":
		return coerceToDouble(v)
	case "bytea":
		if s, ok := v.(string); ok {
			return []byte(s)
		}
		return v
	default:
		// All other types map to string in our Avro schema.
		if s, ok := v.(string); ok {
			return s
		}
		// JSON numbers, bools, etc. → string.
		return fmt.Sprint(v)
	}
}

func coerceToInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

func coerceToLong(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

func coerceToFloat(v any) float32 {
	switch n := v.(type) {
	case float64:
		return float32(n)
	case float32:
		return n
	default:
		return 0
	}
}

func coerceToDouble(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	default:
		return 0
	}
}

// topicForEvent derives a topic name from the event channel.
// "pgcdc:orders" → "pgcdc.orders"
func topicForEvent(ev event.Event) string {
	return strings.ReplaceAll(ev.Channel, ":", ".")
}
