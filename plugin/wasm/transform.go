package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/transform"
)

// WasmTransform wraps a Wasm plugin as a transform.TransformFunc.
type WasmTransform struct {
	basePlugin
}

// NewTransform compiles and instantiates a Wasm transform plugin.
func NewTransform(ctx context.Context, rt *Runtime, spec config.PluginTransformSpec, logger *slog.Logger) (*WasmTransform, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_transform", "plugin", spec.Name)

	bp, err := newBase(ctx, rt, spec.Name, spec.Path, spec.Config, spec.Encoding, logger)
	if err != nil {
		return nil, fmt.Errorf("load transform module %s: %w", spec.Name, err)
	}

	logger.Info("wasm transform loaded", "path", spec.Path, "encoding", bp.encoding)
	return &WasmTransform{basePlugin: *bp}, nil
}

// TransformFunc returns a transform.TransformFunc closure backed by the Wasm plugin.
func (wt *WasmTransform) TransformFunc() transform.TransformFunc {
	return func(ev event.Event) (event.Event, error) {
		// Preserve LSN across the Wasm boundary (json:"-" tag means it won't serialize).
		savedLSN := ev.LSN

		data, err := json.Marshal(ev)
		if err != nil {
			metrics.PluginErrors.WithLabelValues(wt.name, "transform").Inc()
			return ev, &pgcdcerr.PluginError{Plugin: wt.name, Type: "transform", Err: fmt.Errorf("marshal: %w", err)}
		}

		result, callErr := wt.call("transform", "transform", data)
		if callErr != nil {
			wt.logger.Warn("transform plugin error, skipping event",
				slog.String("event_id", ev.ID),
				slog.String("error", callErr.Error()),
			)
			return ev, callErr
		}

		// Empty result = drop the event.
		if len(result) == 0 {
			return ev, transform.ErrDropEvent
		}

		var transformed event.Event
		if err := json.Unmarshal(result, &transformed); err != nil {
			metrics.PluginErrors.WithLabelValues(wt.name, "transform").Inc()
			return ev, &pgcdcerr.PluginError{Plugin: wt.name, Type: "transform", Err: fmt.Errorf("unmarshal result: %w", err)}
		}

		// Re-inject LSN that was lost during serialization.
		transformed.LSN = savedLSN
		return transformed, nil
	}
}

// Close releases the plugin instance.
func (wt *WasmTransform) Close(ctx context.Context) {
	_ = wt.plugin.Close(ctx)
}
