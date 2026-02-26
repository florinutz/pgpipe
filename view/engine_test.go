package view

import (
	"encoding/json"
	"testing"

	"github.com/florinutz/pgcdc/event"
)

func TestEngine_Process(t *testing.T) {
	def, err := Parse("orders_per_minute",
		"SELECT COUNT(*) as order_count, SUM(payload.amount) as total_revenue, payload.region FROM pgcdc_events WHERE channel = 'pgcdc:orders' AND operation = 'INSERT' GROUP BY payload.region TUMBLING WINDOW 1m",
		EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewEngine([]*ViewDef{def}, nil)

	// Feed events.
	events := []event.Event{
		makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"region": "us-east", "amount": float64(100)}),
		makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"region": "us-east", "amount": float64(200)}),
		makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{"region": "eu-west", "amount": float64(50)}),
		// This should be filtered by WHERE (wrong operation).
		makeTestEvent(t, "pgcdc:orders", "DELETE", map[string]any{"region": "us-east", "amount": float64(999)}),
		// This should be filtered by WHERE (wrong channel).
		makeTestEvent(t, "pgcdc:users", "INSERT", map[string]any{"region": "us-east", "amount": float64(999)}),
	}

	for _, ev := range events {
		engine.Process(ev)
	}

	// Flush and check results.
	results := engine.FlushAll()
	if len(results) != 2 {
		t.Fatalf("expected 2 results (2 regions), got %d", len(results))
	}

	byRegion := make(map[string]map[string]any)
	for _, r := range results {
		var p map[string]any
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		region := p["region"].(string)
		byRegion[region] = p
	}

	usEast := byRegion["us-east"]
	if int64(usEast["order_count"].(float64)) != 2 {
		t.Errorf("us-east order_count = %v, want 2", usEast["order_count"])
	}
	if usEast["total_revenue"].(float64) != 300 {
		t.Errorf("us-east total_revenue = %v, want 300", usEast["total_revenue"])
	}

	euWest := byRegion["eu-west"]
	if int64(euWest["order_count"].(float64)) != 1 {
		t.Errorf("eu-west order_count = %v, want 1", euWest["order_count"])
	}
}

func TestEngine_LoopPrevention(t *testing.T) {
	def, err := Parse("test", "SELECT COUNT(*) as n FROM pgcdc_events TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewEngine([]*ViewDef{def}, nil)

	// This event should be processed.
	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{}))

	// This event should be skipped (loop prevention).
	engine.Process(makeTestEvent(t, "pgcdc:_view:test", "VIEW_RESULT", map[string]any{}))

	results := engine.FlushAll()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	var p map[string]any
	if err := json.Unmarshal(results[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int64(p["n"].(float64)) != 1 {
		t.Errorf("n = %v, want 1 (view result event should be skipped)", p["n"])
	}
}

func TestEngine_MultipleViews(t *testing.T) {
	def1, err := Parse("view1", "SELECT COUNT(*) as n FROM pgcdc_events WHERE channel = 'pgcdc:orders' TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}
	def2, err := Parse("view2", "SELECT COUNT(*) as n FROM pgcdc_events WHERE channel = 'pgcdc:users' TUMBLING WINDOW 1m", EmitRow, 0)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewEngine([]*ViewDef{def1, def2}, nil)

	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{}))
	engine.Process(makeTestEvent(t, "pgcdc:orders", "INSERT", map[string]any{}))
	engine.Process(makeTestEvent(t, "pgcdc:users", "INSERT", map[string]any{}))

	results := engine.FlushAll()
	if len(results) != 2 {
		t.Fatalf("expected 2 results (one per view), got %d", len(results))
	}

	byChannel := make(map[string]map[string]any)
	for _, r := range results {
		var p map[string]any
		if err := json.Unmarshal(r.Payload, &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		byChannel[r.Channel] = p
	}

	v1 := byChannel["pgcdc:_view:view1"]
	if int64(v1["n"].(float64)) != 2 {
		t.Errorf("view1 n = %v, want 2", v1["n"])
	}

	v2 := byChannel["pgcdc:_view:view2"]
	if int64(v2["n"].(float64)) != 1 {
		t.Errorf("view2 n = %v, want 1", v2["n"])
	}
}

func makeTestEvent(t *testing.T, channel, op string, payload map[string]any) event.Event {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := event.New(channel, op, data, "test")
	if err != nil {
		t.Fatal(err)
	}
	return ev
}
