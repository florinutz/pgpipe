package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// checkpointLoadResponse is the JSON response from the plugin's load function.
type checkpointLoadResponse struct {
	LSN uint64 `json:"lsn"`
}

// checkpointSaveRequest is the JSON input to the plugin's save function.
type checkpointSaveRequest struct {
	SlotName string `json:"slot_name"`
	LSN      uint64 `json:"lsn"`
}

// WasmCheckpointStore wraps a Wasm plugin as a checkpoint.Store.
type WasmCheckpointStore struct {
	basePlugin
}

// NewCheckpointStore compiles, instantiates, and calls init on a Wasm checkpoint plugin.
func NewCheckpointStore(ctx context.Context, rt *Runtime, name string, spec *config.PluginSpec, logger *slog.Logger) (*WasmCheckpointStore, error) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "wasm_checkpoint", "plugin", name)

	bp, err := newBase(ctx, rt, name, spec.Path, spec.Config, "", logger)
	if err != nil {
		return nil, fmt.Errorf("load checkpoint module %s: %w", name, err)
	}

	if err := bp.initPlugin(ctx, "checkpoint", spec.Config); err != nil {
		return nil, err
	}

	logger.Info("wasm checkpoint store loaded", "path", spec.Path)
	return &WasmCheckpointStore{basePlugin: *bp}, nil
}

func (s *WasmCheckpointStore) Load(ctx context.Context, slotName string) (uint64, error) {
	result, err := s.call("load", "checkpoint", []byte(slotName))
	if err != nil {
		return 0, err
	}

	if len(result) == 0 {
		return 0, nil
	}

	var resp checkpointLoadResponse
	if err := json.Unmarshal(result, &resp); err != nil {
		return 0, &pgcdcerr.PluginError{Plugin: s.name, Type: "checkpoint", Err: fmt.Errorf("unmarshal load response: %w", err)}
	}

	return resp.LSN, nil
}

func (s *WasmCheckpointStore) Save(ctx context.Context, slotName string, lsn uint64) error {
	input, _ := json.Marshal(checkpointSaveRequest{SlotName: slotName, LSN: lsn})

	result, err := s.call("save", "checkpoint", input)
	if err != nil {
		return err
	}
	if len(result) > 0 {
		metrics.PluginErrors.WithLabelValues(s.name, "checkpoint").Inc()
		return &pgcdcerr.PluginError{Plugin: s.name, Type: "checkpoint", Err: fmt.Errorf("%s", string(result))}
	}

	return nil
}

func (s *WasmCheckpointStore) Close() error {
	s.closePlugin(context.Background())
	return nil
}
