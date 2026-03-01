package webhookgw

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/florinutz/pgcdc/event"
)

func TestValidateSignature_Generic(t *testing.T) {
	secret := "test-secret"
	body := `{"hello":"world"}`
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	sig := hex.EncodeToString(mac.Sum(nil))

	if err := ValidateSignature(sig, body, secret, "X-Signature"); err != nil {
		t.Fatalf("expected valid signature, got: %v", err)
	}
}

func TestValidateSignature_GenericInvalid(t *testing.T) {
	if err := ValidateSignature("deadbeef", "body", "secret", "X-Signature"); err == nil {
		t.Fatal("expected error for invalid signature")
	}
}

func TestValidateSignature_GitHub(t *testing.T) {
	secret := "gh-secret"
	body := `{"action":"push"}`
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if err := ValidateSignature(sig, body, secret, "X-Hub-Signature-256"); err != nil {
		t.Fatalf("expected valid GitHub signature, got: %v", err)
	}
}

func TestValidateSignature_GitHubInvalid(t *testing.T) {
	if err := ValidateSignature("sha256=0000", `{}`, "secret", "X-Hub-Signature-256"); err == nil {
		t.Fatal("expected error for invalid GitHub signature")
	}
}

func TestValidateSignature_Stripe(t *testing.T) {
	secret := "whsec_test"
	body := `{"type":"invoice.paid"}`
	ts := fmt.Sprintf("%d", time.Now().Unix())
	signed := ts + "." + body
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signed))
	sig := fmt.Sprintf("t=%s,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))

	if err := ValidateSignature(sig, body, secret, "Stripe-Signature"); err != nil {
		t.Fatalf("expected valid Stripe signature, got: %v", err)
	}
}

func TestValidateSignature_StripeReplay(t *testing.T) {
	secret := "whsec_test"
	body := `{"type":"invoice.paid"}`
	ts := "1234567890" // old timestamp from 2009
	signed := ts + "." + body
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signed))
	sig := fmt.Sprintf("t=%s,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))

	err := ValidateSignature(sig, body, secret, "Stripe-Signature")
	if err == nil {
		t.Fatal("expected error for replayed Stripe signature with old timestamp")
	}
	if !strings.Contains(err.Error(), "timestamp too old") {
		t.Fatalf("expected 'too old' error, got: %v", err)
	}
}

func TestValidateSignature_StripeInvalid(t *testing.T) {
	sig := "t=123,v1=0000"
	if err := ValidateSignature(sig, `{}`, "secret", "Stripe-Signature"); err == nil {
		t.Fatal("expected error for invalid Stripe signature")
	}
}

func TestValidateSignature_MissingHeader(t *testing.T) {
	if err := ValidateSignature("", "body", "secret", "X-Sig"); err == nil {
		t.Fatal("expected error for missing signature header")
	}
}

func TestValidateSignature_NoSecret(t *testing.T) {
	// When no secret is configured, validation should pass.
	if err := ValidateSignature("", "body", "", "X-Sig"); err != nil {
		t.Fatalf("expected nil when no secret, got: %v", err)
	}
}

// newTestDetector creates a detector with test sources and starts it in the background.
func newTestDetector(sources []*Source) (*Detector, <-chan event.Event) {
	events := make(chan event.Event, 100)
	det := New(sources, 1024, nil) // 1KB limit for tests

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_ = det.Start(ctx, events)
	}()

	// Wait for ready.
	select {
	case <-det.ready:
	case <-time.After(time.Second):
		panic("detector did not become ready")
	}

	return det, events
}

func TestHandleIngest_HappyPath(t *testing.T) {
	det, events := newTestDetector([]*Source{
		{Name: "github", ChannelPrefix: "pgcdc:github"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	body := `{"action":"push","ref":"refs/heads/main"}`
	req := httptest.NewRequest(http.MethodPost, "/ingest/github", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("expected status ok, got %q", resp["status"])
	}
	if resp["event_id"] == "" {
		t.Fatal("expected non-empty event_id")
	}

	select {
	case ev := <-events:
		if ev.Channel != "pgcdc:github" {
			t.Fatalf("expected channel pgcdc:github, got %q", ev.Channel)
		}
		if ev.Operation != "INSERT" {
			t.Fatalf("expected operation INSERT, got %q", ev.Operation)
		}
		if ev.Source != detectorName {
			t.Fatalf("expected source %q, got %q", detectorName, ev.Source)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHandleIngest_ExplicitChannel(t *testing.T) {
	det, events := newTestDetector([]*Source{
		{Name: "stripe", ChannelPrefix: "pgcdc:stripe"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	req := httptest.NewRequest(http.MethodPost, "/ingest/stripe/invoice", strings.NewReader(`{}`))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	select {
	case ev := <-events:
		if ev.Channel != "pgcdc:stripe.invoice" {
			t.Fatalf("expected channel pgcdc:stripe.invoice, got %q", ev.Channel)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHandleIngest_TypeFromPayload(t *testing.T) {
	det, events := newTestDetector([]*Source{
		{Name: "webhook", ChannelPrefix: "pgcdc:webhook"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	body := `{"type":"order.created","data":{}}`
	req := httptest.NewRequest(http.MethodPost, "/ingest/webhook", strings.NewReader(body))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	select {
	case ev := <-events:
		if ev.Channel != "pgcdc:webhook.order.created" {
			t.Fatalf("expected channel pgcdc:webhook.order.created, got %q", ev.Channel)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHandleIngest_UnknownSource(t *testing.T) {
	det, _ := newTestDetector([]*Source{
		{Name: "github", ChannelPrefix: "pgcdc:github"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	req := httptest.NewRequest(http.MethodPost, "/ingest/unknown", strings.NewReader(`{}`))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleIngest_BodyTooLarge(t *testing.T) {
	det, _ := newTestDetector([]*Source{
		{Name: "test", ChannelPrefix: "pgcdc:test"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	// Create body larger than 1KB limit.
	largeBody := strings.Repeat("x", 2048)
	req := httptest.NewRequest(http.MethodPost, "/ingest/test", strings.NewReader(largeBody))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleIngest_SignatureValidation(t *testing.T) {
	secret := "test-secret"
	det, events := newTestDetector([]*Source{
		{Name: "secure", Secret: secret, SignatureHeader: "X-Signature", ChannelPrefix: "pgcdc:secure"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	body := `{"data":"value"}`
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	sig := hex.EncodeToString(mac.Sum(nil))

	// Valid signature.
	req := httptest.NewRequest(http.MethodPost, "/ingest/secure", strings.NewReader(body))
	req.Header.Set("X-Signature", sig)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHandleIngest_SignatureRejection(t *testing.T) {
	det, _ := newTestDetector([]*Source{
		{Name: "secure", Secret: "real-secret", SignatureHeader: "X-Signature", ChannelPrefix: "pgcdc:secure"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	req := httptest.NewRequest(http.MethodPost, "/ingest/secure", strings.NewReader(`{}`))
	req.Header.Set("X-Signature", "invalid")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandleIngest_MissingSignatureHeader(t *testing.T) {
	det, _ := newTestDetector([]*Source{
		{Name: "secure", Secret: "secret", SignatureHeader: "X-Signature", ChannelPrefix: "pgcdc:secure"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	req := httptest.NewRequest(http.MethodPost, "/ingest/secure", strings.NewReader(`{}`))
	// No signature header set.
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandleIngest_NonJSONPayload(t *testing.T) {
	det, events := newTestDetector([]*Source{
		{Name: "plain", ChannelPrefix: "pgcdc:plain"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	req := httptest.NewRequest(http.MethodPost, "/ingest/plain", strings.NewReader("hello world"))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	select {
	case ev := <-events:
		// Non-JSON should be wrapped.
		var parsed map[string]string
		if err := json.Unmarshal(ev.Payload, &parsed); err != nil {
			t.Fatalf("unmarshal wrapped payload: %v", err)
		}
		if parsed["raw"] != "hello world" {
			t.Fatalf("expected raw=hello world, got %q", parsed["raw"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestHandleIngest_ResponseBody(t *testing.T) {
	det, _ := newTestDetector([]*Source{
		{Name: "test", ChannelPrefix: "pgcdc:test"},
	})

	r := chi.NewRouter()
	det.MountHTTP(r)

	req := httptest.NewRequest(http.MethodPost, "/ingest/test", strings.NewReader(`{}`))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	respBody, _ := io.ReadAll(w.Body)
	var resp map[string]string
	if err := json.Unmarshal(respBody, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", resp["status"])
	}
	if resp["event_id"] == "" {
		t.Fatal("expected non-empty event_id in response")
	}
}
