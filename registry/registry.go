// Package registry provides a self-registering component registry for adapters,
// detectors, and transforms. Components register via init() in their respective
// cmd/register_*.go files. The registry enables:
//   - Replacing hardcoded switch statements in cmd/listen.go with registry lookups
//   - The `pgcdc list adapters|detectors|transforms` CLI command
//   - Build-tag support via separate register/stub files
package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/transform"
	"github.com/spf13/pflag"
)

// AdapterContext carries shared resources needed by adapter factories.
type AdapterContext struct {
	Cfg          config.Config
	Logger       *slog.Logger
	Ctx          context.Context
	KafkaEncoder encoding.Encoder
	NatsEncoder  encoding.Encoder
}

// AdapterResult is returned by adapter factories.
type AdapterResult struct {
	Adapter adapter.Adapter
	// Extra holds arbitrary per-adapter data (e.g. SSE/WS brokers that need
	// to be wired into the HTTP server). Callers inspect these with type assertions.
	Extra map[string]any
}

// AdapterEntry describes a registered adapter.
type AdapterEntry struct {
	Name        string
	Description string
	Create      func(ctx AdapterContext) (AdapterResult, error)
	BindFlags   func(f *pflag.FlagSet)
}

// DetectorContext carries shared resources for detector factories.
type DetectorContext struct {
	Cfg    config.Config
	Logger *slog.Logger
	Ctx    context.Context
}

// DetectorEntry describes a registered detector.
type DetectorEntry struct {
	Name        string
	Description string
	Create      func(ctx DetectorContext) (detector.Detector, error)
	BindFlags   func(f *pflag.FlagSet)
}

// TransformEntry describes a registered transform type.
type TransformEntry struct {
	Name        string
	Description string
	Create      func(spec config.TransformSpec) transform.TransformFunc
}

var (
	mu         sync.RWMutex
	adapters   = make(map[string]AdapterEntry)
	detectors  = make(map[string]DetectorEntry)
	transforms = make(map[string]TransformEntry)
)

// RegisterAdapter adds an adapter factory to the registry.
func RegisterAdapter(e AdapterEntry) {
	mu.Lock()
	defer mu.Unlock()
	adapters[e.Name] = e
}

// RegisterDetector adds a detector factory to the registry.
func RegisterDetector(e DetectorEntry) {
	mu.Lock()
	defer mu.Unlock()
	detectors[e.Name] = e
}

// RegisterTransform adds a transform factory to the registry.
func RegisterTransform(e TransformEntry) {
	mu.Lock()
	defer mu.Unlock()
	transforms[e.Name] = e
}

// GetAdapter returns the adapter entry by name and whether it was found.
func GetAdapter(name string) (AdapterEntry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := adapters[name]
	return e, ok
}

// GetDetector returns the detector entry by name and whether it was found.
func GetDetector(name string) (DetectorEntry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := detectors[name]
	return e, ok
}

// GetTransform returns the transform entry by name and whether it was found.
func GetTransform(name string) (TransformEntry, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := transforms[name]
	return e, ok
}

// CreateAdapter creates an adapter by name using the registry.
func CreateAdapter(name string, ctx AdapterContext) (AdapterResult, error) {
	e, ok := GetAdapter(name)
	if !ok {
		return AdapterResult{}, fmt.Errorf("unknown adapter %q", name)
	}
	return e.Create(ctx)
}

// Adapters returns all registered adapter entries, sorted by name.
func Adapters() []AdapterEntry {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]AdapterEntry, 0, len(adapters))
	for _, e := range adapters {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Detectors returns all registered detector entries, sorted by name.
func Detectors() []DetectorEntry {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]DetectorEntry, 0, len(detectors))
	for _, e := range detectors {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Transforms returns all registered transform entries, sorted by name.
func Transforms() []TransformEntry {
	mu.RLock()
	defer mu.RUnlock()
	result := make([]TransformEntry, 0, len(transforms))
	for _, e := range transforms {
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// AdapterNames returns sorted names of all registered adapters.
func AdapterNames() []string {
	entries := Adapters()
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// DetectorNames returns sorted names of all registered detectors.
func DetectorNames() []string {
	entries := Detectors()
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// BindAdapterFlags calls BindFlags on all registered adapters that provide it.
func BindAdapterFlags(f *pflag.FlagSet) {
	mu.RLock()
	defer mu.RUnlock()
	for _, e := range adapters {
		if e.BindFlags != nil {
			e.BindFlags(f)
		}
	}
}

// BindDetectorFlags calls BindFlags on all registered detectors that provide it.
func BindDetectorFlags(f *pflag.FlagSet) {
	mu.RLock()
	defer mu.RUnlock()
	for _, e := range detectors {
		if e.BindFlags != nil {
			e.BindFlags(f)
		}
	}
}

// SpecToTransform converts a config TransformSpec using registered transform factories.
func SpecToTransform(spec config.TransformSpec) transform.TransformFunc {
	e, ok := GetTransform(spec.Type)
	if !ok {
		return nil
	}
	return e.Create(spec)
}
