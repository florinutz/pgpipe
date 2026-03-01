//go:build !no_arrow

package arrow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/florinutz/pgcdc/event"
)

func TestChannelBuffer_Add_Get(t *testing.T) {
	buf := newChannelBuffer(3)

	e1 := event.Event{ID: "1", Channel: "ch", CreatedAt: time.Now()}
	e2 := event.Event{ID: "2", Channel: "ch", CreatedAt: time.Now()}
	e3 := event.Event{ID: "3", Channel: "ch", CreatedAt: time.Now()}

	buf.add(e1)
	buf.add(e2)
	buf.add(e3)

	events := buf.get(0)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].ID != "1" || events[1].ID != "2" || events[2].ID != "3" {
		t.Fatalf("wrong event order: %v", events)
	}
}

func TestChannelBuffer_Wraparound(t *testing.T) {
	buf := newChannelBuffer(3)

	for i := 0; i < 5; i++ {
		buf.add(event.Event{ID: string(rune('a' + i)), Channel: "ch"})
	}

	events := buf.get(0)
	if len(events) != 3 {
		t.Fatalf("expected 3 events after wraparound, got %d", len(events))
	}
	// Should have c, d, e (last 3)
	if events[0].ID != "c" || events[1].ID != "d" || events[2].ID != "e" {
		t.Fatalf("wrong events after wraparound: %v", []string{events[0].ID, events[1].ID, events[2].ID})
	}
}

func TestChannelBuffer_GetWithOffset(t *testing.T) {
	buf := newChannelBuffer(5)

	for i := 0; i < 5; i++ {
		buf.add(event.Event{ID: string(rune('a' + i)), Channel: "ch"})
	}

	events := buf.get(3)
	if len(events) != 2 {
		t.Fatalf("expected 2 events with offset 3, got %d", len(events))
	}
	if events[0].ID != "d" || events[1].ID != "e" {
		t.Fatalf("wrong events with offset: %v", []string{events[0].ID, events[1].ID})
	}
}

func TestChannelBuffer_GetBeyondCount(t *testing.T) {
	buf := newChannelBuffer(5)
	buf.add(event.Event{ID: "a", Channel: "ch"})

	events := buf.get(10)
	if events != nil {
		t.Fatalf("expected nil for offset beyond count, got %v", events)
	}
}

func TestOIDToArrowType(t *testing.T) {
	tests := []struct {
		oid      uint32
		expected arrow.DataType
	}{
		{21, arrow.PrimitiveTypes.Int16},
		{23, arrow.PrimitiveTypes.Int32},
		{20, arrow.PrimitiveTypes.Int64},
		{700, arrow.PrimitiveTypes.Float32},
		{701, arrow.PrimitiveTypes.Float64},
		{1700, arrow.PrimitiveTypes.Float64},
		{16, arrow.FixedWidthTypes.Boolean},
		{25, arrow.BinaryTypes.String},
		{1043, arrow.BinaryTypes.String},
		{2950, arrow.BinaryTypes.String},
		{1082, arrow.FixedWidthTypes.Date32},
		{17, arrow.BinaryTypes.Binary},
		{3802, arrow.BinaryTypes.String},
		{9999, arrow.BinaryTypes.String}, // unknown -> utf8
	}

	for _, tt := range tests {
		got := OIDToArrowType(tt.oid)
		if got.ID() != tt.expected.ID() {
			t.Errorf("OIDToArrowType(%d) = %v, want %v", tt.oid, got, tt.expected)
		}
	}
}

func TestDefaultEventSchema(t *testing.T) {
	sc := DefaultEventSchema()
	if sc.NumFields() != 6 {
		t.Fatalf("expected 6 fields, got %d", sc.NumFields())
	}

	names := []string{"id", "channel", "operation", "payload", "source", "created_at"}
	for i, name := range names {
		if sc.Field(i).Name != name {
			t.Errorf("field %d: expected %q, got %q", i, name, sc.Field(i).Name)
		}
	}
}

func TestAdapter_Ingest(t *testing.T) {
	a := New(":0", 100, nil, nil)

	ev := event.Event{
		ID:        "test-1",
		Channel:   "pgcdc:orders",
		Operation: "INSERT",
		Payload:   json.RawMessage(`{"id": 1}`),
		Source:    "wal",
		CreatedAt: time.Now(),
	}
	a.ingest(ev)

	names := a.channelNames()
	if len(names) != 1 || names[0] != "pgcdc:orders" {
		t.Fatalf("expected channel pgcdc:orders, got %v", names)
	}

	events := a.getEvents("pgcdc:orders", 0)
	if len(events) != 1 || events[0].ID != "test-1" {
		t.Fatalf("expected 1 event with ID test-1, got %v", events)
	}

	if a.bufferCount("pgcdc:orders") != 1 {
		t.Fatalf("expected buffer count 1, got %d", a.bufferCount("pgcdc:orders"))
	}
}

func TestAdapter_Name(t *testing.T) {
	a := New(":0", 100, nil, nil)
	if a.Name() != "arrow" {
		t.Errorf("expected name 'arrow', got %q", a.Name())
	}
}
