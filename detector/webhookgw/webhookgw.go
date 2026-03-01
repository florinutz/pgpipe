package webhookgw

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const detectorName = "webhook_gateway"

// Detector receives inbound HTTP webhooks from external services,
// validates signatures, and emits them as CDC events.
type Detector struct {
	sources     map[string]*Source
	events      chan<- event.Event
	maxBodySize int64
	logger      *slog.Logger
	ready       chan struct{} // closed when events chan is set
}

// New creates a webhook gateway detector.
func New(sources []*Source, maxBodySize int64, logger *slog.Logger) *Detector {
	if logger == nil {
		logger = slog.Default()
	}
	if maxBodySize <= 0 {
		maxBodySize = 1024 * 1024 // 1MB default
	}

	srcMap := make(map[string]*Source, len(sources))
	for _, s := range sources {
		srcMap[s.Name] = s
	}

	return &Detector{
		sources:     srcMap,
		maxBodySize: maxBodySize,
		logger:      logger.With("component", detectorName),
		ready:       make(chan struct{}),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string { return detectorName }

// Start stores the events channel, signals readiness, and blocks until ctx is cancelled.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	d.events = events
	close(d.ready)
	d.logger.Info("webhook gateway detector started", "sources", len(d.sources))
	<-ctx.Done()
	return nil
}

// MountHTTP registers the webhook ingest routes on the given router.
func (d *Detector) MountHTTP(r chi.Router) {
	r.Post("/ingest/{source}", d.handleIngest)
	r.Post("/ingest/{source}/{channel}", d.handleIngest)
}

func (d *Detector) handleIngest(w http.ResponseWriter, r *http.Request) {
	sourceName := chi.URLParam(r, "source")
	explicitChannel := chi.URLParam(r, "channel")

	src, ok := d.sources[sourceName]
	if !ok {
		metrics.WebhookGatewayErrors.WithLabelValues(sourceName).Inc()
		http.Error(w, `{"error":"unknown source"}`, http.StatusNotFound)
		return
	}

	// Read body with size limit.
	body, err := io.ReadAll(io.LimitReader(r.Body, d.maxBodySize+1))
	if err != nil {
		metrics.WebhookGatewayErrors.WithLabelValues(sourceName).Inc()
		http.Error(w, `{"error":"read body failed"}`, http.StatusInternalServerError)
		return
	}
	if int64(len(body)) > d.maxBodySize {
		metrics.WebhookGatewayErrors.WithLabelValues(sourceName).Inc()
		http.Error(w, `{"error":"request body too large"}`, http.StatusRequestEntityTooLarge)
		return
	}

	// Validate signature if source has a secret.
	if src.Secret != "" {
		headerName := src.SignatureHeader
		if headerName == "" {
			headerName = "X-Signature"
		}
		sigValue := r.Header.Get(headerName)
		if err := ValidateSignature(sigValue, string(body), src.Secret, headerName); err != nil {
			metrics.WebhookGatewaySignatureFailures.WithLabelValues(sourceName).Inc()
			http.Error(w, `{"error":"signature validation failed"}`, http.StatusUnauthorized)
			return
		}
	}

	// Determine channel name.
	channel := d.buildChannel(src, explicitChannel, body)

	// Ensure payload is valid JSON; wrap raw payloads if needed.
	var payload json.RawMessage
	if json.Valid(body) {
		payload = body
	} else {
		wrapped, _ := json.Marshal(map[string]string{"raw": string(body)})
		payload = wrapped
	}

	// Wait for the events channel to be ready.
	select {
	case <-d.ready:
	case <-r.Context().Done():
		http.Error(w, `{"error":"request cancelled"}`, http.StatusServiceUnavailable)
		return
	}

	ev, err := event.New(channel, "INSERT", payload, detectorName)
	if err != nil {
		metrics.WebhookGatewayErrors.WithLabelValues(sourceName).Inc()
		http.Error(w, `{"error":"create event failed"}`, http.StatusInternalServerError)
		return
	}

	// Send to events channel (non-blocking would drop; blocking ensures delivery).
	select {
	case d.events <- ev:
	case <-r.Context().Done():
		http.Error(w, `{"error":"request cancelled"}`, http.StatusServiceUnavailable)
		return
	}

	metrics.WebhookGatewayReceived.WithLabelValues(sourceName).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","event_id":%q}`, ev.ID)
}

// buildChannel determines the event channel from the source config, explicit URL
// parameter, or the payload's "type" field.
func (d *Detector) buildChannel(src *Source, explicitChannel string, body []byte) string {
	prefix := src.ChannelPrefix
	if prefix == "" {
		prefix = "pgcdc:" + src.Name
	}

	if explicitChannel != "" {
		return prefix + "." + explicitChannel
	}

	// Try to extract "type" from the JSON payload for sub-channel routing.
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(body, &parsed); err == nil {
		if raw, ok := parsed["type"]; ok {
			var eventType string
			if json.Unmarshal(raw, &eventType) == nil && eventType != "" {
				// Sanitize: replace spaces and special chars.
				eventType = strings.ReplaceAll(eventType, " ", "_")
				return prefix + "." + eventType
			}
		}
	}

	return prefix
}
