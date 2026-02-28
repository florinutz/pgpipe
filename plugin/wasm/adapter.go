package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// WasmAdapter wraps a Wasm plugin as an adapter.Adapter.
// It also implements adapter.Acknowledger and adapter.DLQAware.
type WasmAdapter struct {
	basePlugin
	dlq   dlq.DLQ
	ackFn adapter.AckFunc
}

// NewAdapter compiles, instantiates, and calls init on a Wasm adapter plugin.
func NewAdapter(ctx context.Context, rt *Runtime, spec config.PluginAdapterSpec, logger *slog.Logger) (*WasmAdapter, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_adapter", "plugin", spec.Name)

	bp, err := newBase(ctx, rt, spec.Name, spec.Path, spec.Config, "", logger)
	if err != nil {
		return nil, fmt.Errorf("load adapter module %s: %w", spec.Name, err)
	}

	if err := bp.initPlugin(ctx, "adapter", spec.Config); err != nil {
		return nil, err
	}

	logger.Info("wasm adapter loaded", "path", spec.Path)
	return &WasmAdapter{basePlugin: *bp}, nil
}

func (a *WasmAdapter) Name() string { return a.name }

func (a *WasmAdapter) SetDLQ(d dlq.DLQ) { a.dlq = d }

func (a *WasmAdapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

func (a *WasmAdapter) Start(ctx context.Context, events <-chan event.Event) error {
	defer a.closePlugin(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}

			data, err := json.Marshal(ev)
			if err != nil {
				a.logger.Error("marshal event", "error", err, "event_id", ev.ID)
				a.ack(ev)
				continue
			}

			result, callErr := a.call("handle", "adapter", data)
			if callErr != nil || len(result) > 0 {
				metrics.PluginErrors.WithLabelValues(a.name, "adapter").Inc()
				var errMsg string
				if callErr != nil {
					errMsg = callErr.Error()
				} else {
					errMsg = string(result)
				}

				a.logger.Warn("adapter plugin handle error",
					slog.String("event_id", ev.ID),
					slog.String("error", errMsg),
				)

				if a.dlq != nil {
					_ = a.dlq.Record(ctx, ev, a.name, &pgcdcerr.PluginError{
						Plugin: a.name,
						Type:   "adapter",
						Err:    fmt.Errorf("%s", errMsg),
					})
				}
				a.ack(ev)
				continue
			}

			metrics.EventsDelivered.WithLabelValues(a.name).Inc()
			a.ack(ev)
		}
	}
}

func (a *WasmAdapter) ack(ev event.Event) {
	if a.ackFn != nil && ev.LSN > 0 {
		a.ackFn(ev.LSN)
	}
}
