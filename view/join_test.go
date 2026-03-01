package view

import (
	"testing"
	"time"
)

func TestIntervalJoinWindow_MatchWithin(t *testing.T) {
	def := &JoinDef{
		Name:       "test_join",
		LeftAlias:  "a",
		RightAlias: "b",
		LeftKey:    "order_id",
		RightKey:   "id",
		Within:     30 * time.Second,
		LeftChan:   "pgcdc:orders",
		RightChan:  "pgcdc:payments",
	}
	jw := NewIntervalJoinWindow(def)

	now := time.Now()
	leftPayload := map[string]any{"order_id": "123", "amount": 100}
	rightPayload := map[string]any{"id": "123", "status": "paid"}

	// Add left first, then right within WITHIN — should match.
	matches := jw.AddLeft(now, leftPayload)
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches before right arrives, got %d", len(matches))
	}

	matches = jw.AddRight(now.Add(10*time.Second), rightPayload)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
}

func TestIntervalJoinWindow_RightFirst(t *testing.T) {
	def := &JoinDef{
		Name:       "test_join",
		LeftAlias:  "a",
		RightAlias: "b",
		LeftKey:    "order_id",
		RightKey:   "id",
		Within:     30 * time.Second,
		LeftChan:   "pgcdc:orders",
		RightChan:  "pgcdc:payments",
	}
	jw := NewIntervalJoinWindow(def)

	now := time.Now()
	leftPayload := map[string]any{"order_id": "456", "amount": 200}
	rightPayload := map[string]any{"id": "456", "status": "paid"}

	// Add right first, then left within WITHIN — should match.
	matches := jw.AddRight(now, rightPayload)
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches before left arrives, got %d", len(matches))
	}

	matches = jw.AddLeft(now.Add(5*time.Second), leftPayload)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matches))
	}
}

func TestIntervalJoinWindow_OutsideWindow(t *testing.T) {
	def := &JoinDef{
		Name:       "test_join",
		LeftAlias:  "a",
		RightAlias: "b",
		LeftKey:    "order_id",
		RightKey:   "id",
		Within:     5 * time.Second,
		LeftChan:   "pgcdc:orders",
		RightChan:  "pgcdc:payments",
	}
	jw := NewIntervalJoinWindow(def)

	now := time.Now()
	leftPayload := map[string]any{"order_id": "789", "amount": 300}
	rightPayload := map[string]any{"id": "789", "status": "paid"}

	// Add left, then right OUTSIDE WITHIN — should NOT match.
	// Left entry was added at now. Right arrives at now+10s. Within=5s. diff=10s > 5s.
	jw.AddLeft(now, leftPayload)
	matches := jw.AddRight(now.Add(10*time.Second), rightPayload)
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches outside window, got %d", len(matches))
	}
}

func TestIntervalJoinWindow_DifferentKeys(t *testing.T) {
	def := &JoinDef{
		Name:       "test_join",
		LeftAlias:  "a",
		RightAlias: "b",
		LeftKey:    "order_id",
		RightKey:   "id",
		Within:     30 * time.Second,
		LeftChan:   "pgcdc:orders",
		RightChan:  "pgcdc:payments",
	}
	jw := NewIntervalJoinWindow(def)

	now := time.Now()
	leftPayload := map[string]any{"order_id": "111"}
	rightPayload := map[string]any{"id": "999"} // different key

	jw.AddLeft(now, leftPayload)
	matches := jw.AddRight(now.Add(time.Second), rightPayload)
	if len(matches) != 0 {
		t.Fatalf("expected 0 matches for different keys, got %d", len(matches))
	}
}

func TestIntervalJoinWindow_MultipleMatches(t *testing.T) {
	def := &JoinDef{
		Name:       "test_join",
		LeftAlias:  "a",
		RightAlias: "b",
		LeftKey:    "order_id",
		RightKey:   "id",
		Within:     60 * time.Second,
		LeftChan:   "pgcdc:orders",
		RightChan:  "pgcdc:payments",
	}
	jw := NewIntervalJoinWindow(def)

	now := time.Now()
	key := "order-multi"

	// Two left events with the same key.
	jw.AddLeft(now, map[string]any{"order_id": key, "v": 1})
	jw.AddLeft(now.Add(2*time.Second), map[string]any{"order_id": key, "v": 2})

	// One right event — should match both left events.
	matches := jw.AddRight(now.Add(5*time.Second), map[string]any{"id": key, "status": "paid"})
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches for multiple left events, got %d", len(matches))
	}
}
