package pgcdcerr_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/florinutz/pgcdc/pgcdcerr"
)

func TestErrBusClosed(t *testing.T) {
	err := fmt.Errorf("subscribe: %w", pgcdcerr.ErrBusClosed)
	if !errors.Is(err, pgcdcerr.ErrBusClosed) {
		t.Error("errors.Is should match ErrBusClosed")
	}
}

func TestWebhookDeliveryError(t *testing.T) {
	cause := fmt.Errorf("connection refused")
	err := &pgcdcerr.WebhookDeliveryError{
		EventID: "evt-1",
		URL:     "https://example.com/hook",
		Retries: 5,
		Err:     cause,
	}

	// errors.As
	var target *pgcdcerr.WebhookDeliveryError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should match WebhookDeliveryError")
	}
	if target.EventID != "evt-1" {
		t.Errorf("EventID = %q, want evt-1", target.EventID)
	}

	// Unwrap
	if !errors.Is(err, cause) {
		t.Error("errors.Is should match underlying cause via Unwrap")
	}

	// Wrapped
	wrapped := fmt.Errorf("deliver: %w", err)
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should match through wrapping")
	}
}

func TestWebhookDeliveryError_StatusCode(t *testing.T) {
	err := &pgcdcerr.WebhookDeliveryError{
		EventID:    "evt-2",
		URL:        "https://example.com/hook",
		StatusCode: 500,
		Retries:    3,
	}
	msg := err.Error()
	if msg == "" {
		t.Error("Error() should return non-empty string")
	}
}

func TestDetectorDisconnectedError(t *testing.T) {
	cause := fmt.Errorf("connection reset")
	err := &pgcdcerr.DetectorDisconnectedError{
		Source: "listen_notify",
		Err:    cause,
	}

	var target *pgcdcerr.DetectorDisconnectedError
	if !errors.As(err, &target) {
		t.Fatal("errors.As should match DetectorDisconnectedError")
	}
	if target.Source != "listen_notify" {
		t.Errorf("Source = %q, want listen_notify", target.Source)
	}

	if !errors.Is(err, cause) {
		t.Error("errors.Is should match underlying cause via Unwrap")
	}
}
