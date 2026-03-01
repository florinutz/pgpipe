//go:build !no_duckdb

package duckdb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/go-chi/chi/v5"
)

func TestAdapter_IngestAndQuery(t *testing.T) {
	a := New(":memory:", 1*time.Hour, 1*time.Second, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan event.Event, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx, events)
	}()

	// Wait for DB to initialize.
	time.Sleep(100 * time.Millisecond)

	// Send events.
	for i := 0; i < 3; i++ {
		ev, err := event.New("users", "INSERT", json.RawMessage(`{"id":1,"name":"alice"}`), "test")
		if err != nil {
			t.Fatal(err)
		}
		events <- ev
	}

	// Trigger flush via ticker (wait for flush interval).
	time.Sleep(1500 * time.Millisecond)

	// Query via handler.
	r := chi.NewRouter()
	a.MountHTTP(r)

	body := `{"sql": "SELECT count(*) as cnt FROM cdc_events"}`
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var results []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}

	cnt, ok := results[0]["cnt"]
	if !ok {
		t.Fatal("missing cnt column")
	}
	// DuckDB returns int64 via database/sql.
	cntFloat, ok := cnt.(float64)
	if !ok {
		t.Fatalf("unexpected type for cnt: %T", cnt)
	}
	if int(cntFloat) != 3 {
		t.Fatalf("expected 3 events, got %v", cnt)
	}

	cancel()
	<-errCh
}

func TestAdapter_TablesHandler(t *testing.T) {
	a := New(":memory:", 1*time.Hour, 1*time.Second, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan event.Event, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx, events)
	}()

	time.Sleep(100 * time.Millisecond)

	// Send events to different channels.
	for _, ch := range []string{"users", "users", "orders"} {
		ev, err := event.New(ch, "INSERT", json.RawMessage(`{}`), "test")
		if err != nil {
			t.Fatal(err)
		}
		events <- ev
	}

	time.Sleep(1500 * time.Millisecond)

	r := chi.NewRouter()
	a.MountHTTP(r)

	req := httptest.NewRequest(http.MethodGet, "/query/tables", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var tables []tableInfo
	if err := json.Unmarshal(w.Body.Bytes(), &tables); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 channels, got %d", len(tables))
	}
}

func TestAdapter_TTLCleanup(t *testing.T) {
	// Use very short retention so cleanup fires quickly.
	a := New(":memory:", 300*time.Millisecond, 100*time.Millisecond, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan event.Event, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx, events)
	}()

	time.Sleep(100 * time.Millisecond)

	ev, err := event.New("users", "INSERT", json.RawMessage(`{}`), "test")
	if err != nil {
		t.Fatal(err)
	}
	events <- ev

	// Wait for flush.
	time.Sleep(200 * time.Millisecond)

	// Wait for retention to expire + cleanup cycle to run.
	// Cleanup ticker fires at retention/10 = 30ms, so after 800ms the event
	// (300ms retention) should be well expired and cleaned up.
	time.Sleep(800 * time.Millisecond)

	r := chi.NewRouter()
	a.MountHTTP(r)

	body := `{"sql": "SELECT count(*) as cnt FROM cdc_events"}`
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var results []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}

	cnt, _ := results[0]["cnt"].(float64)
	if int(cnt) != 0 {
		t.Fatalf("expected 0 events after cleanup, got %v", cnt)
	}

	cancel()
	<-errCh
}

func TestAdapter_QueryHandler_InvalidSQL(t *testing.T) {
	a := New(":memory:", 1*time.Hour, 1*time.Second, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan event.Event, 1)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx, events)
	}()

	time.Sleep(100 * time.Millisecond)

	r := chi.NewRouter()
	a.MountHTTP(r)

	body := `{"sql": "SELECT * FROM nonexistent_table"}`
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid SQL, got %d", w.Code)
	}

	cancel()
	<-errCh
}

func TestAdapter_QueryHandler_EmptySQL(t *testing.T) {
	a := New(":memory:", 1*time.Hour, 1*time.Second, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan event.Event, 1)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx, events)
	}()

	time.Sleep(100 * time.Millisecond)

	r := chi.NewRouter()
	a.MountHTTP(r)

	body := `{"sql": ""}`
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty SQL, got %d", w.Code)
	}

	cancel()
	<-errCh
}

func TestAdapter_QueryHandler_WithParams(t *testing.T) {
	a := New(":memory:", 1*time.Hour, 1*time.Second, 100, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan event.Event, 10)

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.Start(ctx, events)
	}()

	time.Sleep(100 * time.Millisecond)

	ev, err := event.New("users", "INSERT", json.RawMessage(`{"id":1}`), "test")
	if err != nil {
		t.Fatal(err)
	}
	events <- ev

	time.Sleep(1500 * time.Millisecond)

	r := chi.NewRouter()
	a.MountHTTP(r)

	body := `{"sql": "SELECT count(*) as cnt FROM cdc_events WHERE channel = ?", "params": ["users"]}`
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var results []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cnt, _ := results[0]["cnt"].(float64)
	if int(cnt) != 1 {
		t.Fatalf("expected 1, got %v", cnt)
	}

	cancel()
	<-errCh
}
