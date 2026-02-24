package transform

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/florinutz/pgcdc/event"
)

func makeEvent(t *testing.T, op string, payload map[string]any) event.Event {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return event.Event{
		ID:        "test-id",
		Channel:   "test",
		Operation: op,
		Payload:   raw,
	}
}

func payloadMap(t *testing.T, ev event.Event) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(ev.Payload, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return m
}

func TestDropColumns(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{
		"name": "Alice",
		"ssn":  "123-45-6789",
		"age":  30,
	})

	fn := DropColumns("ssn", "missing_key")
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	if _, ok := m["ssn"]; ok {
		t.Error("ssn should have been dropped")
	}
	if m["name"] != "Alice" {
		t.Errorf("name = %v, want Alice", m["name"])
	}
	if m["age"] != float64(30) {
		t.Errorf("age = %v, want 30", m["age"])
	}
}

func TestDropColumns_EmptyPayload(t *testing.T) {
	ev := event.Event{ID: "test", Payload: nil}
	result, err := DropColumns("ssn")(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Payload != nil {
		t.Error("expected nil payload for empty input")
	}
}

func TestRenameFields(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{
		"created_at": "2024-01-01",
		"name":       "Alice",
	})

	fn := RenameFields(map[string]string{
		"created_at":  "createdAt",
		"missing_key": "shouldNotAppear",
	})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	if _, ok := m["created_at"]; ok {
		t.Error("created_at should have been renamed")
	}
	if m["createdAt"] != "2024-01-01" {
		t.Errorf("createdAt = %v, want 2024-01-01", m["createdAt"])
	}
	if _, ok := m["shouldNotAppear"]; ok {
		t.Error("missing source key should not create target")
	}
}

func TestMask_Hash(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{"email": "alice@example.com"})
	fn := Mask(MaskField{Field: "email", Mode: MaskHash})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	h := sha256.Sum256([]byte("alice@example.com"))
	expected := fmt.Sprintf("%x", h)
	if m["email"] != expected {
		t.Errorf("email = %v, want SHA-256 hash %s", m["email"], expected)
	}
}

func TestMask_Redact(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{"ssn": "123-45-6789"})
	fn := Mask(MaskField{Field: "ssn", Mode: MaskRedact})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	if m["ssn"] != "***REDACTED***" {
		t.Errorf("ssn = %v, want ***REDACTED***", m["ssn"])
	}
}

func TestMask_Partial(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{"email": "alice@example.com"})
	fn := Mask(MaskField{Field: "email", Mode: MaskPartial})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	masked := m["email"].(string)
	if masked[0] != 'a' || masked[len(masked)-1] != 'm' {
		t.Errorf("partial mask should keep first/last char, got %q", masked)
	}
	if len(masked) != len("alice@example.com") {
		t.Errorf("partial mask length = %d, want %d", len(masked), len("alice@example.com"))
	}
}

func TestMask_MissingField(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{"name": "Alice"})
	fn := Mask(MaskField{Field: "ssn", Mode: MaskRedact})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	if _, ok := m["ssn"]; ok {
		t.Error("missing field should not be added")
	}
}

func TestPartialMask_ShortString(t *testing.T) {
	if got := partialMask("ab"); got != "**" {
		t.Errorf("partialMask(ab) = %q, want **", got)
	}
	if got := partialMask("a"); got != "*" {
		t.Errorf("partialMask(a) = %q, want *", got)
	}
}

func TestFilterOperation(t *testing.T) {
	insert := event.Event{ID: "1", Operation: "INSERT"}
	del := event.Event{ID: "2", Operation: "DELETE"}
	update := event.Event{ID: "3", Operation: "UPDATE"}

	fn := FilterOperation("INSERT", "UPDATE")

	if _, err := fn(insert); err != nil {
		t.Errorf("INSERT should pass, got %v", err)
	}
	if _, err := fn(update); err != nil {
		t.Errorf("UPDATE should pass, got %v", err)
	}
	if _, err := fn(del); !errors.Is(err, ErrDropEvent) {
		t.Errorf("DELETE should be dropped, got %v", err)
	}
}

func TestFilterField(t *testing.T) {
	published := makeEvent(t, "INSERT", map[string]any{"status": "published"})
	draft := makeEvent(t, "INSERT", map[string]any{"status": "draft"})

	fn := FilterField("status", "published")

	if _, err := fn(published); err != nil {
		t.Errorf("published should pass, got %v", err)
	}
	if _, err := fn(draft); !errors.Is(err, ErrDropEvent) {
		t.Errorf("draft should be dropped, got %v", err)
	}
}

func TestFilterFieldIn(t *testing.T) {
	ev1 := makeEvent(t, "INSERT", map[string]any{"status": "published"})
	ev2 := makeEvent(t, "INSERT", map[string]any{"status": "archived"})
	ev3 := makeEvent(t, "INSERT", map[string]any{"status": "draft"})

	fn := FilterFieldIn("status", "published", "archived")

	if _, err := fn(ev1); err != nil {
		t.Errorf("published should pass, got %v", err)
	}
	if _, err := fn(ev2); err != nil {
		t.Errorf("archived should pass, got %v", err)
	}
	if _, err := fn(ev3); !errors.Is(err, ErrDropEvent) {
		t.Errorf("draft should be dropped, got %v", err)
	}
}

func TestFilterExpression(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{"amount": float64(150)})

	fn := FilterExpression(func(m map[string]any) bool {
		v, ok := m["amount"].(float64)
		return ok && v > 100
	})

	if _, err := fn(ev); err != nil {
		t.Errorf("amount>100 should pass, got %v", err)
	}

	small := makeEvent(t, "INSERT", map[string]any{"amount": float64(50)})
	if _, err := fn(small); !errors.Is(err, ErrDropEvent) {
		t.Errorf("amount<100 should be dropped, got %v", err)
	}
}

func TestFilterExpression_NonObjectPayload(t *testing.T) {
	ev := event.Event{ID: "1", Payload: json.RawMessage(`"hello"`)}
	fn := FilterExpression(func(m map[string]any) bool { return false })
	// Non-object payloads pass through.
	if _, err := fn(ev); err != nil {
		t.Errorf("non-object should pass through, got %v", err)
	}
}

func TestChain(t *testing.T) {
	ev := makeEvent(t, "INSERT", map[string]any{
		"name":       "Alice",
		"ssn":        "123-45-6789",
		"created_at": "2024-01-01",
	})

	fn := Chain(
		DropColumns("ssn"),
		RenameFields(map[string]string{"created_at": "createdAt"}),
	)

	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m := payloadMap(t, result)
	if _, ok := m["ssn"]; ok {
		t.Error("ssn should have been dropped")
	}
	if _, ok := m["created_at"]; ok {
		t.Error("created_at should have been renamed")
	}
	if m["createdAt"] != "2024-01-01" {
		t.Errorf("createdAt = %v, want 2024-01-01", m["createdAt"])
	}
}

func TestChain_ShortCircuitOnDrop(t *testing.T) {
	called := false
	fn := Chain(
		FilterOperation("UPDATE"),
		func(ev event.Event) (event.Event, error) {
			called = true
			return ev, nil
		},
	)

	ev := event.Event{ID: "1", Operation: "INSERT"}
	_, err := fn(ev)
	if !errors.Is(err, ErrDropEvent) {
		t.Errorf("expected ErrDropEvent, got %v", err)
	}
	if called {
		t.Error("second transform should not have been called")
	}
}

func TestChain_Nil(t *testing.T) {
	fn := Chain()
	if fn != nil {
		t.Error("Chain() with no args should return nil")
	}
}

func TestChain_Single(t *testing.T) {
	single := DropColumns("x")
	fn := Chain(single)
	// Chain of 1 should return the original function.
	ev := makeEvent(t, "INSERT", map[string]any{"x": 1, "y": 2})
	result, err := fn(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := payloadMap(t, result)
	if _, ok := m["x"]; ok {
		t.Error("x should have been dropped")
	}
}

func TestWithPayload_NonObject(t *testing.T) {
	ev := event.Event{ID: "1", Payload: json.RawMessage(`[1,2,3]`)}
	result, err := withPayload(ev, func(m map[string]any) {
		m["injected"] = true
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Array payload should pass through unchanged.
	if string(result.Payload) != `[1,2,3]` {
		t.Errorf("payload = %s, want [1,2,3]", result.Payload)
	}
}
