package encoding

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/florinutz/pgcdc/event"
	pbplugin "github.com/florinutz/pgcdc/plugin/proto"
)

func TestJSONEncoder(t *testing.T) {
	enc := JSONEncoder{}
	ev := event.Event{ID: "test-1", Channel: "pgcdc:orders"}
	data := []byte(`{"id":1,"name":"test"}`)

	encoded, err := enc.Encode(ev, data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(encoded, data) {
		t.Errorf("JSONEncoder should return data unchanged")
	}
	if enc.ContentType() != "application/json" {
		t.Errorf("ContentType = %q, want application/json", enc.ContentType())
	}
}

func TestProtobufEncoderRoundTrip(t *testing.T) {
	enc := NewProtobufEncoder(nil, nil)

	ev := event.Event{
		ID:        "ev-001",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id":1}`),
		Source:    "wal",
		CreatedAt: time.Date(2026, 2, 24, 12, 0, 0, 0, time.UTC),
		Transaction: &event.TransactionInfo{
			Xid:        42,
			CommitTime: time.Date(2026, 2, 24, 12, 0, 0, 0, time.UTC),
			Seq:        1,
		},
	}

	payload := ev.Payload
	encoded, err := enc.Encode(ev, payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Decode the protobuf.
	var pbEvent pbplugin.Event
	if err := proto.Unmarshal(encoded, &pbEvent); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}

	if pbEvent.Id != ev.ID {
		t.Errorf("ID = %q, want %q", pbEvent.Id, ev.ID)
	}
	if pbEvent.Channel != ev.Channel {
		t.Errorf("Channel = %q, want %q", pbEvent.Channel, ev.Channel)
	}
	if pbEvent.Operation != ev.Operation {
		t.Errorf("Operation = %q, want %q", pbEvent.Operation, ev.Operation)
	}
	if !bytes.Equal(pbEvent.Payload, payload) {
		t.Errorf("Payload = %q, want %q", pbEvent.Payload, payload)
	}
	if pbEvent.Source != ev.Source {
		t.Errorf("Source = %q, want %q", pbEvent.Source, ev.Source)
	}
	if pbEvent.Transaction == nil {
		t.Fatal("Transaction is nil")
	}
	if pbEvent.Transaction.Xid != ev.Transaction.Xid {
		t.Errorf("Transaction.Xid = %d, want %d", pbEvent.Transaction.Xid, ev.Transaction.Xid)
	}

	if enc.ContentType() != "application/x-protobuf" {
		t.Errorf("ContentType = %q, want application/x-protobuf", enc.ContentType())
	}
}

func TestAvroEncoderOpaqueMode(t *testing.T) {
	enc := NewAvroEncoder(nil)

	ev := event.Event{
		ID:      "ev-001",
		Channel: "pgcdc:orders",
	}
	// Data without column metadata → opaque mode.
	data := []byte(`{"id":1,"name":"test"}`)

	encoded, err := enc.Encode(ev, data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Error("encoded data should not be empty")
	}
	// Should be binary Avro, not the original JSON.
	if bytes.Equal(encoded, data) {
		t.Error("encoded data should differ from input JSON")
	}
}

func TestAvroEncoderWithColumns(t *testing.T) {
	enc := NewAvroEncoder(nil)

	ev := event.Event{
		ID:      "ev-001",
		Channel: "pgcdc:public.users",
	}
	// Payload with column metadata (as added by --include-schema).
	payload := map[string]any{
		"id":     float64(1),
		"name":   "alice",
		"active": true,
		"columns": []map[string]any{
			{"name": "id", "type_oid": 23, "type_name": "int4"},
			{"name": "name", "type_oid": 25, "type_name": "text"},
			{"name": "active", "type_oid": 16, "type_name": "bool"},
		},
	}
	data, _ := json.Marshal(payload)

	encoded, err := enc.Encode(ev, data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(encoded) == 0 {
		t.Error("encoded data should not be empty")
	}
	if bytes.Equal(encoded, data) {
		t.Error("encoded data should differ from input JSON")
	}
}

func TestParseEncodingType(t *testing.T) {
	tests := []struct {
		input string
		want  EncodingType
		err   bool
	}{
		{"json", EncodingJSON, false},
		{"", EncodingJSON, false},
		{"avro", EncodingAvro, false},
		{"protobuf", EncodingProtobuf, false},
		{"xml", "", true},
	}
	for _, tt := range tests {
		got, err := ParseEncodingType(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("ParseEncodingType(%q) error = %v, wantErr %v", tt.input, err, tt.err)
		}
		if got != tt.want {
			t.Errorf("ParseEncodingType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
