package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/middleware"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/encoding"
	encregistry "github.com/florinutz/pgcdc/encoding/registry"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/transform"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/trace"
)

// setupDetector creates and configures the change data capture detector via the registry.
// Returns the detector, additional pipeline options (checkpoint store), and a cleanup function.
func setupDetector(ctx context.Context, cfg config.Config, cmd *cobra.Command, tp trace.TracerProvider, wasmRT any, logger *slog.Logger) (registry.DetectorResult, []pgcdc.Option, error) {
	// Pre-process checkpoint plugin config from CLI flag (JSON string → map) before
	// calling the factory, since the factory only reads from cfg.
	cpPluginPath, _ := cmd.Flags().GetString("checkpoint-plugin")
	cpPluginCfgStr, _ := cmd.Flags().GetString("checkpoint-plugin-config")
	if cpPluginPath != "" || cpPluginCfgStr != "" {
		if cfg.Plugins.Checkpoint == nil {
			cfg.Plugins.Checkpoint = &config.PluginSpec{}
		}
		if cpPluginPath != "" {
			cfg.Plugins.Checkpoint.Path = cpPluginPath
		}
		if cpPluginCfgStr != "" {
			cfg.Plugins.Checkpoint.Config = map[string]any{}
			if err := json.Unmarshal([]byte(cpPluginCfgStr), &cfg.Plugins.Checkpoint.Config); err != nil {
				return registry.DetectorResult{}, nil, fmt.Errorf("parse --checkpoint-plugin-config: %w", err)
			}
		}
	}

	detType := cfg.Detector.Type
	if detType == "" {
		detType = "listen_notify"
	}

	detCtx := registry.DetectorContext{
		Cfg:            cfg,
		Logger:         logger,
		Ctx:            ctx,
		TracerProvider: tp,
		WasmRT:         wasmRT,
	}

	result, err := registry.CreateDetector(detType, detCtx)
	if err != nil {
		return registry.DetectorResult{}, nil, err
	}

	var opts []pgcdc.Option
	if result.CheckpointStore != nil {
		opts = append(opts, pgcdc.WithCheckpointStore(result.CheckpointStore))
	}
	return result, opts, nil
}

// setupAdapters creates all configured adapters via the registry, plus view and plugin adapters.
// Returns pipeline options, SSE/WS brokers, HTTP mount functions, and a cleanup function.
func setupAdapters(ctx context.Context, cfg config.Config, cmd *cobra.Command, wasmRT any, kafkaEncoder, natsEncoder encoding.Encoder, logger *slog.Logger) ([]pgcdc.Option, *sse.Broker, *ws.Broker, []func(chi.Router), func(), error) {
	adapterCtx := registry.AdapterContext{
		Cfg:          cfg,
		Logger:       logger,
		Ctx:          ctx,
		KafkaEncoder: kafkaEncoder,
		NatsEncoder:  natsEncoder,
	}

	var opts []pgcdc.Option
	var sseBroker *sse.Broker
	var wsBroker *ws.Broker
	var httpMountFns []func(chi.Router)

	for _, name := range cfg.Adapters {
		result, aErr := registry.CreateAdapter(name, adapterCtx)
		if aErr != nil {
			return nil, nil, nil, nil, func() {}, fmt.Errorf("create adapter %s: %w", name, aErr)
		}
		opts = append(opts, pgcdc.WithAdapter(result.Adapter))

		// Register reinjector adapters (e.g. view) explicitly.
		if name == "view" {
			opts = append(opts, pgcdc.WithReinjector(name))
		}

		if result.MiddlewareConfig != nil {
			opts = append(opts, pgcdc.WithMiddlewareConfig(name, *result.MiddlewareConfig))
		}
		if result.SSEBroker != nil {
			sseBroker = result.SSEBroker
		}
		if result.WSBroker != nil {
			wsBroker = result.WSBroker
		}
		if m, ok := result.Adapter.(adapter.HTTPMountable); ok {
			httpMountFns = append(httpMountFns, m.MountHTTP)
		}
	}

	// Parse --view-query CLI flags and merge with YAML views (CLI wins on name conflict).
	viewQueryFlags, _ := cmd.Flags().GetStringSlice("view-query")
	for _, vq := range viewQueryFlags {
		parts := strings.SplitN(vq, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, nil, nil, nil, func() {}, fmt.Errorf("invalid --view-query format %q: expected name:query", vq)
		}
		replaced := false
		for i, vc := range cfg.Views {
			if vc.Name == parts[0] {
				cfg.Views[i].Query = parts[1]
				replaced = true
				break
			}
		}
		if !replaced {
			cfg.Views = append(cfg.Views, config.ViewConfig{
				Name:  parts[0],
				Query: parts[1],
			})
		}
	}

	// Auto-create view adapter from YAML views or --view-query (if not already added via --adapter view).
	if len(cfg.Views) > 0 {
		hasViewAdapter := false
		for _, name := range cfg.Adapters {
			if name == "view" {
				hasViewAdapter = true
				break
			}
		}
		if !hasViewAdapter {
			adapterCtx.Cfg = cfg // reflect updated views
			result, aErr := registry.CreateAdapter("view", adapterCtx)
			if aErr != nil {
				return nil, nil, nil, nil, func() {}, fmt.Errorf("create view adapter: %w", aErr)
			}
			opts = append(opts, pgcdc.WithAdapter(result.Adapter))
			opts = append(opts, pgcdc.WithReinjector("view"))
		}
	}

	// Create plugin adapters.
	pluginAdapterOpts, pluginAdapterCleanup, paErr := wirePluginAdapters(ctx, wasmRT, cmd, cfg, logger)
	if paErr != nil {
		return nil, nil, nil, nil, func() {}, paErr
	}
	opts = append(opts, pluginAdapterOpts...)

	return opts, sseBroker, wsBroker, httpMountFns, pluginAdapterCleanup, nil
}

// setupTransforms builds the complete transform chain from CLI flags, plugins, and YAML config.
// Returns the immutable CLI transforms (for SIGHUP reload), plugin transforms, all pipeline options, and a cleanup function.
func setupTransforms(ctx context.Context, cfg config.Config, cmd *cobra.Command, wasmRT any, logger *slog.Logger) ([]transform.TransformFunc, pluginTransforms, []pgcdc.Option, func()) {
	globalTransforms, adapterTransforms := buildTransformOpts(cfg, cmd)
	pluginTransformOpts, plugTfx, pluginTransformCleanup := buildPluginTransformOpts(ctx, wasmRT, cmd, cfg, logger)
	immutableCLI := buildCLITransforms(cmd)

	var opts []pgcdc.Option
	for _, fn := range globalTransforms {
		opts = append(opts, pgcdc.WithTransform(fn))
	}
	for adapterName, fns := range adapterTransforms {
		for _, fn := range fns {
			opts = append(opts, pgcdc.WithAdapterTransform(adapterName, fn))
		}
	}
	opts = append(opts, pluginTransformOpts...)

	return immutableCLI, plugTfx, opts, pluginTransformCleanup
}

// setupDLQ creates the dead letter queue backend.
func setupDLQ(ctx context.Context, cfg config.Config, cmd *cobra.Command, wasmRT any, logger *slog.Logger) (dlq.DLQ, error) {
	switch cfg.DLQ.Type {
	case "stderr", "":
		return dlq.NewStderrDLQ(logger), nil
	case "pg_table":
		dlqDB := cfg.DLQ.DBURL
		if dlqDB == "" {
			dlqDB = cfg.DatabaseURL
		}
		return dlq.NewPGTableDLQ(dlqDB, cfg.DLQ.Table, logger), nil
	case "none":
		return dlq.NopDLQ{}, nil
	case "plugin":
		dlqPluginPath, _ := cmd.Flags().GetString("dlq-plugin-path")
		if dlqPluginPath == "" && cfg.Plugins.DLQ != nil {
			dlqPluginPath = cfg.Plugins.DLQ.Path
		}
		if dlqPluginPath == "" {
			return nil, fmt.Errorf("--dlq plugin requires --dlq-plugin-path or plugins.dlq.path in config")
		}
		dlqPluginCfgStr, _ := cmd.Flags().GetString("dlq-plugin-config")
		dlqPluginCfg := map[string]any{}
		if dlqPluginCfgStr != "" {
			if err := json.Unmarshal([]byte(dlqPluginCfgStr), &dlqPluginCfg); err != nil {
				return nil, fmt.Errorf("parse --dlq-plugin-config: %w", err)
			}
		} else if cfg.Plugins.DLQ != nil && cfg.Plugins.DLQ.Config != nil {
			dlqPluginCfg = cfg.Plugins.DLQ.Config
		}
		dlqInst, err := makePluginDLQ(ctx, wasmRT, dlqPluginPath, dlqPluginCfg, logger)
		if err != nil {
			return nil, fmt.Errorf("create wasm dlq: %w", err)
		}
		return dlqInst, nil
	default:
		return nil, fmt.Errorf("unknown DLQ type: %q (expected stderr, pg_table, plugin, or none)", cfg.DLQ.Type)
	}
}

// setupBackpressure creates the backpressure controller and parses adapter priorities.
// Returns nil if backpressure is disabled.
func setupBackpressure(cfg config.Config, cmd *cobra.Command, logger *slog.Logger) (*backpressure.Controller, error) {
	if !cfg.Backpressure.Enabled {
		return nil, nil
	}
	bpCtrl := backpressure.New(
		cfg.Backpressure.WarnThreshold,
		cfg.Backpressure.CriticalThreshold,
		cfg.Backpressure.MaxThrottle,
		cfg.Backpressure.PollInterval,
		nil, // health checker injected by pipeline
		logger,
	)
	adapterPriorityFlags, _ := cmd.Flags().GetStringSlice("adapter-priority")
	for _, ap := range adapterPriorityFlags {
		parts := strings.SplitN(ap, "=", 2)
		if len(parts) != 2 {
			logger.Warn("ignoring malformed adapter-priority (expected name=level)", "value", ap)
			continue
		}
		var prio backpressure.AdapterPriority
		switch parts[1] {
		case "critical":
			prio = backpressure.PriorityCritical
		case "normal":
			prio = backpressure.PriorityNormal
		case "best-effort":
			prio = backpressure.PriorityBestEffort
		default:
			return nil, fmt.Errorf("unknown adapter priority %q for %q (expected critical, normal, or best-effort)", parts[1], parts[0])
		}
		bpCtrl.SetAdapterPriority(parts[0], prio)
	}
	return bpCtrl, nil
}

// buildEncoders creates encoders for Kafka and NATS based on config.
// Returns nil encoders for JSON encoding (default, no-op passthrough).
func buildEncoders(cfg config.Config, logger *slog.Logger) (kafkaEnc, natsEnc encoding.Encoder, err error) {
	// Parse encoding types.
	kafkaEncType, err := encoding.ParseEncodingType(cfg.Kafka.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka encoding: %w", err)
	}
	natsEncType, err := encoding.ParseEncodingType(cfg.Nats.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("nats encoding: %w", err)
	}

	// If both are JSON, no encoders needed.
	if kafkaEncType == encoding.EncodingJSON && natsEncType == encoding.EncodingJSON {
		return nil, nil, nil
	}

	// Build schema registry client if any non-JSON encoding is configured.
	var regClient *encregistry.Client
	if cfg.Encoding.SchemaRegistryURL != "" {
		regClient = encregistry.New(
			cfg.Encoding.SchemaRegistryURL,
			cfg.Encoding.SchemaRegistryUsername,
			cfg.Encoding.SchemaRegistryPassword,
		)
	}

	kafkaEnc = makeEncoder(kafkaEncType, regClient, logger)
	natsEnc = makeEncoder(natsEncType, regClient, logger)

	return kafkaEnc, natsEnc, nil
}

func makeEncoder(encType encoding.EncodingType, regClient *encregistry.Client, logger *slog.Logger) encoding.Encoder {
	switch encType {
	case encoding.EncodingAvro:
		opts := []encoding.AvroOption{}
		if regClient != nil {
			opts = append(opts, encoding.WithRegistry(regClient))
		}
		return encoding.NewAvroEncoder(logger, opts...)
	case encoding.EncodingProtobuf:
		return encoding.NewProtobufEncoder(regClient, logger)
	default:
		return nil // JSON = nil encoder, adapters use raw bytes
	}
}

// middlewareConfigFrom builds a middleware.Config from per-adapter config fields.
// Used for backward compatibility: existing --webhook-cb-failures etc. flags
// populate the middleware config that wraps the Deliverer.

func webhookMiddlewareConfig(cfg config.Config) middleware.Config {
	return middleware.Config{
		CircuitBreaker: cbConfig(cfg.Webhook.CBMaxFailures, cfg.Webhook.CBResetTimeout),
		RateLimit:      rlConfig(cfg.Webhook.RateLimit, cfg.Webhook.RateLimitBurst),
	}
}

func cbConfig(maxFailures int, resetTimeout time.Duration) *middleware.CircuitBreakerConfig {
	if maxFailures <= 0 {
		return nil
	}
	return &middleware.CircuitBreakerConfig{
		MaxFailures:  maxFailures,
		ResetTimeout: resetTimeout,
	}
}

func rlConfig(eps float64, burst int) *middleware.RateLimitConfig {
	if eps <= 0 {
		return nil
	}
	return &middleware.RateLimitConfig{
		EventsPerSecond: eps,
		Burst:           burst,
	}
}

// ensureAllTablesPublication creates a FOR ALL TABLES publication if it doesn't already exist.
func ensureAllTablesPublication(ctx context.Context, dbURL, publication string, logger *slog.Logger) error {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES",
		pgx.Identifier{publication}.Sanitize(),
	))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" { // duplicate_object
			logger.Info("all-tables publication already exists, reusing", "publication", publication)
			return nil
		}
		return fmt.Errorf("create publication: %w", err)
	}
	logger.Info("created all-tables publication", "publication", publication)
	return nil
}
