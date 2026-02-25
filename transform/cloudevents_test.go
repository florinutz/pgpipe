package transform

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
)

func TestCloudEvents_INSERT(t *testing.T) {
	ev := event.Event{
		ID:        "test-1",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1,"name":"alice"},"old":null}`),
		Source:    "wal",
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		LSN:       12345,
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["specversion"] != "1.0" {
		t.Errorf("specversion = %v, want 1.0", ce["specversion"])
	}
	if ce["id"] != "test-1" {
		t.Errorf("id = %v, want test-1", ce["id"])
	}
	if ce["type"] != "io.pgcdc.change.INSERT" {
		t.Errorf("type = %v, want io.pgcdc.change.INSERT", ce["type"])
	}
	if ce["source"] != "/pgcdc" {
		t.Errorf("source = %v, want /pgcdc", ce["source"])
	}
	if ce["subject"] != "pgcdc:orders" {
		t.Errorf("subject = %v, want pgcdc:orders", ce["subject"])
	}
	if ce["time"] != "2026-01-01T12:00:00Z" {
		t.Errorf("time = %v, want 2026-01-01T12:00:00Z", ce["time"])
	}
	if ce["datacontenttype"] != "application/json" {
		t.Errorf("datacontenttype = %v, want application/json", ce["datacontenttype"])
	}

	// data should be the original payload preserved as-is.
	data, ok := ce["data"].(map[string]any)
	if !ok {
		t.Fatalf("data is not a map: %T", ce["data"])
	}
	if data["op"] != "INSERT" {
		t.Errorf("data.op = %v, want INSERT", data["op"])
	}
	row := data["row"].(map[string]any)
	if row["name"] != "alice" {
		t.Errorf("data.row.name = %v, want alice", row["name"])
	}

	// Extension: pgcdclsn present when LSN > 0.
	if ce["pgcdclsn"] != float64(12345) {
		t.Errorf("pgcdclsn = %v, want 12345", ce["pgcdclsn"])
	}

	// Event envelope fields preserved.
	if got.ID != "test-1" {
		t.Errorf("event ID = %v, want test-1", got.ID)
	}
	if got.Operation != "INSERT" {
		t.Errorf("operation = %v, want INSERT", got.Operation)
	}
}

func TestCloudEvents_UPDATE(t *testing.T) {
	ev := event.Event{
		ID:        "test-2",
		Channel:   "pgcdc:orders",
		Operation: "UPDATE",
		Payload:   json.RawMessage(`{"op":"UPDATE","table":"orders","row":{"id":1,"name":"bob"},"old":{"id":1,"name":"alice"}}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["type"] != "io.pgcdc.change.UPDATE" {
		t.Errorf("type = %v, want io.pgcdc.change.UPDATE", ce["type"])
	}

	// data preserved as-is with old and row.
	data := ce["data"].(map[string]any)
	if data["op"] != "UPDATE" {
		t.Errorf("data.op = %v, want UPDATE", data["op"])
	}
}

func TestCloudEvents_DELETE(t *testing.T) {
	ev := event.Event{
		ID:        "test-3",
		Channel:   "pgcdc:orders",
		Operation: "DELETE",
		Payload:   json.RawMessage(`{"op":"DELETE","table":"orders","row":{"id":1,"name":"alice"},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["type"] != "io.pgcdc.change.DELETE" {
		t.Errorf("type = %v, want io.pgcdc.change.DELETE", ce["type"])
	}
}

func TestCloudEvents_TRUNCATE(t *testing.T) {
	ev := event.Event{
		ID:        "test-4",
		Channel:   "pgcdc:orders",
		Operation: "TRUNCATE",
		Payload:   json.RawMessage(`{"op":"TRUNCATE","table":"orders","row":null,"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["type"] != "io.pgcdc.change.TRUNCATE" {
		t.Errorf("type = %v, want io.pgcdc.change.TRUNCATE", ce["type"])
	}
}

func TestCloudEvents_SNAPSHOT(t *testing.T) {
	ev := event.Event{
		ID:        "test-5",
		Channel:   "pgcdc:orders",
		Operation: "SNAPSHOT",
		Payload:   json.RawMessage(`{"op":"SNAPSHOT","table":"orders","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["type"] != "io.pgcdc.change.SNAPSHOT" {
		t.Errorf("type = %v, want io.pgcdc.change.SNAPSHOT", ce["type"])
	}
}

func TestCloudEvents_WithTransaction(t *testing.T) {
	commitTime := time.Date(2026, 2, 15, 10, 30, 0, 0, time.UTC)
	ev := event.Event{
		ID:        "test-6",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"orders","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Transaction: &event.TransactionInfo{
			Xid:        42,
			CommitTime: commitTime,
			Seq:        3,
		},
		LSN: 99999,
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	// time should use transaction commit time.
	if ce["time"] != "2026-02-15T10:30:00Z" {
		t.Errorf("time = %v, want 2026-02-15T10:30:00Z", ce["time"])
	}

	// Extension attrs present.
	if ce["pgcdclsn"] != float64(99999) {
		t.Errorf("pgcdclsn = %v, want 99999", ce["pgcdclsn"])
	}
	if ce["pgcdctxid"] != float64(42) {
		t.Errorf("pgcdctxid = %v, want 42", ce["pgcdctxid"])
	}
}

func TestCloudEvents_WithoutTransaction(t *testing.T) {
	ev := event.Event{
		ID:        "test-7",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	// time should use CreatedAt.
	if ce["time"] != "2026-01-01T12:00:00Z" {
		t.Errorf("time = %v, want 2026-01-01T12:00:00Z", ce["time"])
	}

	// pgcdclsn absent when LSN == 0.
	if _, ok := ce["pgcdclsn"]; ok {
		t.Error("pgcdclsn should be absent when LSN is 0")
	}

	// pgcdctxid absent when no transaction.
	if _, ok := ce["pgcdctxid"]; ok {
		t.Error("pgcdctxid should be absent when Transaction is nil")
	}
}

func TestCloudEvents_Options(t *testing.T) {
	ev := event.Event{
		ID:        "test-8",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents(WithSource("/my-service"), WithTypePrefix("com.example.cdc"))
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["source"] != "/my-service" {
		t.Errorf("source = %v, want /my-service", ce["source"])
	}
	if ce["type"] != "com.example.cdc.INSERT" {
		t.Errorf("type = %v, want com.example.cdc.INSERT", ce["type"])
	}
}

func TestCloudEvents_UnstructuredPayload(t *testing.T) {
	ev := event.Event{
		ID:        "test-9",
		Channel:   "pgcdc:notifications",
		Operation: "NOTIFY",
		Payload:   json.RawMessage(`"hello world"`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["type"] != "io.pgcdc.change.NOTIFY" {
		t.Errorf("type = %v, want io.pgcdc.change.NOTIFY", ce["type"])
	}

	// data should be the raw string value.
	if ce["data"] != "hello world" {
		t.Errorf("data = %v, want hello world", ce["data"])
	}
}

func TestCloudEvents_NilPayload(t *testing.T) {
	ev := event.Event{
		ID:        "test-10",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["data"] != nil {
		t.Errorf("data = %v, want nil", ce["data"])
	}
}

func TestCloudEvents_LSNPropagation(t *testing.T) {
	ev := event.Event{
		ID:        "test-11",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"op":"INSERT","table":"t","row":{"id":1},"old":null}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		LSN:       777888,
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	// LSN preserved on event struct.
	if got.LSN != 777888 {
		t.Errorf("event.LSN = %d, want 777888", got.LSN)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if ce["pgcdclsn"] != float64(777888) {
		t.Errorf("pgcdclsn = %v, want 777888", ce["pgcdclsn"])
	}
}

func TestCloudEvents_ZeroLSN_NoExtension(t *testing.T) {
	ev := event.Event{
		ID:        "test-12",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id":1}`),
		CreatedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		LSN:       0,
		Transaction: &event.TransactionInfo{
			Xid: 0,
		},
	}

	fn := CloudEvents()
	got, err := fn(ev)
	if err != nil {
		t.Fatal(err)
	}

	var ce map[string]any
	if err := json.Unmarshal(got.Payload, &ce); err != nil {
		t.Fatal(err)
	}

	if _, ok := ce["pgcdclsn"]; ok {
		t.Error("pgcdclsn should be absent when LSN is 0")
	}
	if _, ok := ce["pgcdctxid"]; ok {
		t.Error("pgcdctxid should be absent when Xid is 0")
	}
}
