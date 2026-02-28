package wasm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

// basePlugin holds common state and methods shared by all Wasm plugin wrappers
// (transform, adapter, DLQ, checkpoint store).
type basePlugin struct {
	name     string
	plugin   *extism.Plugin
	encoding string // "json" or "protobuf"
	logger   *slog.Logger
}

// newBase compiles a Wasm module and creates a plugin instance with the given config.
// It does NOT call init — callers that need init should call initPlugin after construction.
func newBase(ctx context.Context, rt *Runtime, name, path string, cfg map[string]any, encoding string, logger *slog.Logger) (*basePlugin, error) {
	if logger == nil {
		logger = slog.Default()
	}

	hostFns := HostFunctions(logger, name)
	compiled, err := rt.LoadModule(ctx, path, hostFns)
	if err != nil {
		return nil, fmt.Errorf("load module %s: %w", name, err)
	}

	plugin, err := rt.NewInstance(ctx, compiled, cfg)
	if err != nil {
		return nil, fmt.Errorf("create instance %s: %w", name, err)
	}

	if encoding == "" {
		encoding = "json"
	}

	return &basePlugin{
		name:     name,
		plugin:   plugin,
		encoding: encoding,
		logger:   logger,
	}, nil
}

// initPlugin calls the plugin's "init" function if exported, passing cfg as JSON.
// On failure, it closes the plugin and returns an error.
func (bp *basePlugin) initPlugin(ctx context.Context, pluginType string, cfg map[string]any) error {
	if !bp.plugin.FunctionExists("init") {
		return nil
	}

	cfgBytes, _ := json.Marshal(cfg)
	_, result, err := bp.plugin.Call("init", cfgBytes)
	if err != nil {
		_ = bp.plugin.Close(ctx)
		return fmt.Errorf("init %s plugin %s: %w", pluginType, bp.name, err)
	}
	if len(result) > 0 {
		_ = bp.plugin.Close(ctx)
		return fmt.Errorf("init %s plugin %s: %s", pluginType, bp.name, string(result))
	}
	return nil
}

// call invokes a named function on the plugin, records metrics, and wraps errors
// as pgcdcerr.PluginError.
func (bp *basePlugin) call(fnName, pluginType string, input []byte) ([]byte, error) {
	start := time.Now()
	_, result, err := bp.plugin.Call(fnName, input)
	duration := time.Since(start)

	metrics.PluginCalls.WithLabelValues(bp.name, pluginType).Inc()
	metrics.PluginDuration.WithLabelValues(bp.name, pluginType).Observe(duration.Seconds())

	if err != nil {
		metrics.PluginErrors.WithLabelValues(bp.name, pluginType).Inc()
		return nil, &pgcdcerr.PluginError{Plugin: bp.name, Type: pluginType, Err: err}
	}
	return result, nil
}

// closePlugin calls the plugin's "close" function (if exported) then releases the
// underlying Extism plugin instance.
func (bp *basePlugin) closePlugin(ctx context.Context) {
	if bp.plugin.FunctionExists("close") {
		_, _, _ = bp.plugin.Call("close", nil)
	}
	_ = bp.plugin.Close(ctx)
}
