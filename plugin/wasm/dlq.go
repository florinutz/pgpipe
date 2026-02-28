package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// dlqRecord is the JSON structure passed to the plugin's record function.
type dlqRecord struct {
	Event     event.Event `json:"event"`
	Adapter   string      `json:"adapter"`
	Error     string      `json:"error"`
	Timestamp time.Time   `json:"timestamp"`
}

// WasmDLQ wraps a Wasm plugin as a dlq.DLQ.
type WasmDLQ struct {
	basePlugin
}

// NewDLQ compiles, instantiates, and calls init on a Wasm DLQ plugin.
func NewDLQ(ctx context.Context, rt *Runtime, name string, spec *config.PluginSpec, logger *slog.Logger) (*WasmDLQ, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_dlq", "plugin", name)

	bp, err := newBase(ctx, rt, name, spec.Path, spec.Config, "", logger)
	if err != nil {
		return nil, fmt.Errorf("load dlq module %s: %w", name, err)
	}

	if err := bp.initPlugin(ctx, "dlq", spec.Config); err != nil {
		return nil, err
	}

	logger.Info("wasm dlq loaded", "path", spec.Path)
	return &WasmDLQ{basePlugin: *bp}, nil
}

func (d *WasmDLQ) Record(ctx context.Context, ev event.Event, adapterName string, err error) error {
	rec := dlqRecord{
		Event:     ev,
		Adapter:   adapterName,
		Error:     err.Error(),
		Timestamp: time.Now().UTC(),
	}

	data, marshalErr := json.Marshal(rec)
	if marshalErr != nil {
		metrics.DLQErrors.Inc()
		return fmt.Errorf("marshal dlq record: %w", marshalErr)
	}

	result, callErr := d.call("record", "dlq", data)
	if callErr != nil {
		metrics.DLQErrors.Inc()
		return callErr
	}
	if len(result) > 0 {
		metrics.PluginErrors.WithLabelValues(d.name, "dlq").Inc()
		metrics.DLQErrors.Inc()
		return &pgcdcerr.PluginError{Plugin: d.name, Type: "dlq", Err: fmt.Errorf("%s", string(result))}
	}

	metrics.DLQRecords.WithLabelValues(adapterName).Inc()
	return nil
}

func (d *WasmDLQ) Close() error {
	d.closePlugin(context.Background())
	return nil
}
