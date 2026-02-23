package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChecker_AllUp(t *testing.T) {
	c := NewChecker()
	c.Register("detector")
	c.Register("bus")
	c.SetStatus("detector", StatusUp)
	c.SetStatus("bus", StatusUp)

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != StatusUp {
		t.Fatalf("expected status %q, got %q", StatusUp, resp.Status)
	}
}

func TestChecker_AnyDown(t *testing.T) {
	c := NewChecker()
	c.Register("detector")
	c.Register("bus")
	c.SetStatus("detector", StatusUp)
	c.SetStatus("bus", StatusDown)

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != StatusDown {
		t.Fatalf("expected status %q, got %q", StatusDown, resp.Status)
	}
	if resp.Components["bus"] != StatusDown {
		t.Fatalf("expected bus component %q, got %q", StatusDown, resp.Components["bus"])
	}
}

func TestChecker_RegisterStartsDown(t *testing.T) {
	c := NewChecker()
	c.Register("detector")

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var resp response
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != StatusDown {
		t.Fatalf("expected status %q, got %q", StatusDown, resp.Status)
	}
}
