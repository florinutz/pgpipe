package cmd

import (
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/florinutz/pgcdc/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Configuration management commands",
}

var configInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate a pgcdc.yaml configuration template",
	Long:  `Generates a commented pgcdc.yaml configuration file with only the sections relevant to your chosen detector and adapters.`,
	RunE:  runConfigInit,
}

var configValidateCmd = &cobra.Command{
	Use:   "validate [config-file]",
	Short: "Validate a pgcdc.yaml configuration file offline",
	Long:  `Parses and validates a YAML configuration file without connecting to any external services. Checks structural correctness, valid enum values, and configuration invariants.`,
	Args:  cobra.MaximumNArgs(1),
	RunE:  runConfigValidate,
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configInitCmd)
	configCmd.AddCommand(configValidateCmd)

	f := configInitCmd.Flags()
	f.String("detector", "wal", "detector type (listen_notify, wal, outbox, mysql, mongodb)")
	f.StringSlice("adapter", []string{"stdout"}, "adapters to configure (repeatable)")
	f.StringP("output", "o", "pgcdc.yaml", "output file path (- for stdout)")
}

type configTemplateData struct {
	Detector string
	Adapters map[string]bool
}

func runConfigInit(cmd *cobra.Command, args []string) error {
	detector, _ := cmd.Flags().GetString("detector")
	adapters, _ := cmd.Flags().GetStringSlice("adapter")
	output, _ := cmd.Flags().GetString("output")

	adapterMap := make(map[string]bool, len(adapters))
	for _, a := range adapters {
		adapterMap[strings.TrimSpace(a)] = true
	}

	data := configTemplateData{
		Detector: detector,
		Adapters: adapterMap,
	}

	funcMap := template.FuncMap{
		"has": func(m map[string]bool, key string) bool { return m[key] },
		"join": func(items []string, sep string) string {
			return strings.Join(items, sep)
		},
		"adapterList": func(m map[string]bool) string {
			names := make([]string, 0, len(m))
			for k := range m {
				names = append(names, k)
			}
			return strings.Join(names, ", ")
		},
	}

	tmpl, err := template.New("config").Funcs(funcMap).Parse(configTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	var w *os.File
	if output == "-" {
		w = os.Stdout
	} else {
		w, err = os.Create(output)
		if err != nil {
			return fmt.Errorf("create %s: %w", output, err)
		}
		defer func() { _ = w.Close() }()
	}

	if err := tmpl.Execute(w, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	if output != "-" {
		fmt.Fprintf(os.Stderr, "Wrote %s (detector: %s, adapters: %s)\n", output, detector, strings.Join(adapters, ", "))
	}
	return nil
}

func runConfigValidate(cmd *cobra.Command, args []string) error {
	if len(args) == 1 {
		viper.SetConfigFile(args[0])
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("read config: %w", err)
		}
	} else if viper.ConfigFileUsed() == "" {
		return fmt.Errorf("no config file found; specify a path or ensure pgcdc.yaml exists in the current directory")
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	var errors []string

	// Structural validation.
	if cfg.Bus.BufferSize <= 0 {
		errors = append(errors, fmt.Sprintf("bus.buffer_size must be > 0, got %d", cfg.Bus.BufferSize))
	}
	if cfg.ShutdownTimeout <= 0 {
		errors = append(errors, "shutdown_timeout must be > 0")
	}
	// Validate route references.
	adapterSet := make(map[string]bool, len(cfg.Adapters))
	for _, a := range cfg.Adapters {
		adapterSet[a] = true
	}
	for name := range cfg.Routes {
		if !adapterSet[name] {
			errors = append(errors, fmt.Sprintf("route references unknown adapter %q", name))
		}
	}

	// Detector type validation.
	validDetectors := map[string]bool{
		"listen_notify": true, "wal": true, "outbox": true, "mysql": true, "mongodb": true,
	}
	if cfg.Detector.Type != "" && !validDetectors[cfg.Detector.Type] {
		errors = append(errors, fmt.Sprintf("unknown detector type %q (expected: listen_notify, wal, outbox, mysql, mongodb)", cfg.Detector.Type))
	}

	// Adapter name validation.
	validAdapters := map[string]bool{
		"stdout": true, "webhook": true, "sse": true, "file": true, "exec": true,
		"pg_table": true, "ws": true, "embedding": true, "nats": true, "kafka": true,
		"search": true, "redis": true, "grpc": true, "s3": true, "iceberg": true,
		"kafkaserver": true, "view": true,
	}
	for _, a := range cfg.Adapters {
		if !validAdapters[a] {
			errors = append(errors, fmt.Sprintf("unknown adapter %q", a))
		}
	}

	// Bus mode validation.
	if cfg.Bus.Mode != "" && cfg.Bus.Mode != "fast" && cfg.Bus.Mode != "reliable" {
		errors = append(errors, fmt.Sprintf("unknown bus mode %q (expected: fast, reliable)", cfg.Bus.Mode))
	}

	// DLQ type validation.
	if cfg.DLQ.Type != "" {
		validDLQ := map[string]bool{"stderr": true, "pg_table": true, "none": true, "plugin": true}
		if !validDLQ[cfg.DLQ.Type] {
			errors = append(errors, fmt.Sprintf("unknown DLQ type %q (expected: stderr, pg_table, none)", cfg.DLQ.Type))
		}
	}

	// OTel exporter validation.
	if cfg.OTel.Exporter != "" {
		validExporters := map[string]bool{"none": true, "stdout": true, "otlp": true}
		if !validExporters[cfg.OTel.Exporter] {
			errors = append(errors, fmt.Sprintf("unknown otel exporter %q (expected: none, stdout, otlp)", cfg.OTel.Exporter))
		}
	}

	if len(errors) > 0 {
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", e)
		}
		return fmt.Errorf("config validation failed with %d error(s)", len(errors))
	}

	fmt.Fprintf(os.Stdout, "Config %s is valid (detector: %s, adapters: %s)\n",
		viper.ConfigFileUsed(), cfg.Detector.Type, strings.Join(cfg.Adapters, ", "))
	return nil
}

const configTemplate = `# pgcdc configuration
# Generated for detector: {{ .Detector }}, adapters: {{ adapterList .Adapters }}
# Docs: https://github.com/florinutz/pgcdc

# ─── Database ───────────────────────────────────────────────────────────────
{{- if or (eq .Detector "listen_notify") (eq .Detector "wal") (eq .Detector "outbox") }}

# PostgreSQL connection string.
database_url: "postgres://user:password@localhost:5432/mydb"
{{- end }}
{{- if eq .Detector "mysql" }}

# MySQL is configured under the mysql: section below.
{{- end }}
{{- if eq .Detector "mongodb" }}

# MongoDB is configured under the mongodb: section below.
{{- end }}

# ─── Channels ──────────────────────────────────────────────────────────────

{{- if eq .Detector "listen_notify" }}

# Channels to LISTEN on (LISTEN/NOTIFY detector).
channels:
  - orders
  - users
{{- else }}

# Channels are auto-detected from WAL/binlog/change stream events.
# channels: []
{{- end }}

# ─── Adapters ──────────────────────────────────────────────────────────────

# Active output adapters.
adapters:
{{- range $name, $_ := .Adapters }}
  - {{ $name }}
{{- end }}

# ─── Detector ──────────────────────────────────────────────────────────────

detector:
  # Type: listen_notify, wal, outbox, mysql, mongodb
  type: {{ .Detector }}
{{- if or (eq .Detector "listen_notify") (eq .Detector "wal") }}

  # Reconnect backoff (all PG-based detectors).
  backoff_base: 5s
  backoff_cap: 60s
{{- end }}
{{- if eq .Detector "wal" }}

  # WAL logical replication settings.
  # publication: pgcdc_pub   # publication name (--all-tables creates "all")
  persistent_slot: false      # true for crash-resumable replication
  # slot_name: pgcdc_slot    # custom slot name (default: auto-generated)
  # checkpoint_db: ""        # separate DB for checkpoint storage

  # Transaction metadata (adds xid, commit_time, seq to events).
  tx_metadata: false
  tx_markers: false           # synthetic BEGIN/COMMIT events (implies tx_metadata)

  # Column type metadata in events.
  include_schema: false
  schema_events: false        # emit SCHEMA_CHANGE events on DDL

  # Heartbeat keeps the replication slot advancing on idle databases.
  # heartbeat_interval: 30s
  # heartbeat_table: pgcdc_heartbeat

  # Slot lag warning threshold in bytes (default: 100MB).
  # slot_lag_warn: 104857600
{{- end }}
{{- if eq .Detector "listen_notify" }}

  # LISTEN/NOTIFY has no extra settings beyond backoff.
{{- end }}
{{- if eq .Detector "outbox" }}

# ─── Outbox ────────────────────────────────────────────────────────────────

outbox:
  # Outbox table name.
  table: pgcdc_outbox

  # Poll interval for new rows.
  poll_interval: 500ms

  # Max rows per poll batch.
  batch_size: 100

  # Keep processed rows (mark processed_at) instead of DELETE.
  keep_processed: false

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if eq .Detector "mysql" }}

# ─── MySQL ─────────────────────────────────────────────────────────────────

mysql:
  # MySQL server address (host:port).
  addr: "127.0.0.1:3306"

  # Replication credentials.
  user: replicator
  password: ""

  # Unique server ID for this replication slave (must be > 0).
  server_id: 1001

  # Filter to specific tables (schema.table format).
  # tables:
  #   - mydb.orders
  #   - mydb.users

  # Use GTID-based replication (recommended).
  use_gtid: false

  # MySQL flavor: mysql or mariadb
  flavor: mysql

  backoff_base: 5s
  backoff_cap: 60s
{{- end }}
{{- if eq .Detector "mongodb" }}

# ─── MongoDB ───────────────────────────────────────────────────────────────

mongodb:
  # MongoDB connection URI (requires replica set or sharded cluster).
  uri: "mongodb://localhost:27017"

  # Watch scope: collection, database, or cluster.
  scope: database

  # Database to watch (required unless scope is cluster).
  database: mydb

  # Filter to specific collections (empty = all in database).
  # collections:
  #   - orders
  #   - users

  # Full document mode: default or updateLookup.
  full_document: updateLookup

  # Resume token storage.
  # metadata_db: ""            # defaults to watched database
  # metadata_coll: pgcdc_resume_tokens

  backoff_base: 5s
  backoff_cap: 60s
{{- end }}

# ─── Bus ───────────────────────────────────────────────────────────────────

bus:
  # Subscriber channel buffer size.
  buffer_size: 1024

  # Mode: "fast" drops events on full buffers, "reliable" blocks the detector.
  mode: fast

# ─── General ───────────────────────────────────────────────────────────────

# Graceful shutdown timeout.
shutdown_timeout: 5s

# Log level: debug, info, warn, error
log_level: info

# Log format: text, json
log_format: text

# Standalone Prometheus metrics server address (empty = disabled).
# metrics_addr: ":9091"

# Skip adapter pre-flight validation.
# skip_validation: false

# Skip database migrations at startup.
# skip_migrations: false

# ─── Dead Letter Queue ────────────────────────────────────────────────────

dlq:
  # Type: stderr, pg_table, none
  type: stderr
  # table: pgcdc_dead_letters  # table name for pg_table DLQ
  # db_url: ""                  # separate DB for DLQ (default: database_url)
{{- if has .Adapters "webhook" }}

# ─── Webhook Adapter ──────────────────────────────────────────────────────

webhook:
  # Target URL for HTTP POST delivery.
  url: "https://example.com/webhook"

  # Custom headers added to every request.
  # headers:
  #   Authorization: "Bearer token"

  # HMAC-SHA256 signing key (sets X-PGCDC-Signature header).
  # signing_key: ""

  # Retry settings.
  max_retries: 5
  timeout: 10s
  backoff_base: 1s
  backoff_cap: 32s
{{- end }}
{{- if has .Adapters "sse" }}

# ─── SSE Adapter ──────────────────────────────────────────────────────────

sse:
  # Listen address for the SSE HTTP server.
  addr: ":8080"

  # Allowed CORS origins.
  cors_origins:
    - "*"

  heartbeat_interval: 15s
  read_timeout: 5s
  idle_timeout: 120s
{{- end }}
{{- if has .Adapters "file" }}

# ─── File Adapter ─────────────────────────────────────────────────────────

file:
  # Output file path (JSON Lines format).
  path: "/var/log/pgcdc/events.jsonl"

  # Max file size before rotation (bytes, default: 100MB).
  max_size: 104857600

  # Max rotated files to keep.
  max_files: 5
{{- end }}
{{- if has .Adapters "exec" }}

# ─── Exec Adapter ─────────────────────────────────────────────────────────

exec:
  # Command to execute (events piped to stdin as JSON Lines).
  command: "my-processor --flag"

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if has .Adapters "pg_table" }}

# ─── PG Table Adapter ────────────────────────────────────────────────────

pg_table:
  # Target PostgreSQL URL (default: database_url).
  # url: ""

  # Target table name.
  table: pgcdc_events

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if has .Adapters "embedding" }}

# ─── Embedding Adapter ───────────────────────────────────────────────────

embedding:
  # OpenAI-compatible embedding API endpoint.
  api_url: "https://api.openai.com/v1/embeddings"
  api_key: ""

  # Model name.
  model: text-embedding-3-small

  # Columns to concatenate for embedding input.
  columns:
    - title
    - body

  # Primary key column for UPSERT.
  id_column: id

  # pgvector target table and database.
  table: pgcdc_embeddings
  # db_url: ""              # default: database_url
  dimension: 1536

  max_retries: 3
  timeout: 30s
  backoff_base: 2s
  backoff_cap: 60s

  # Skip embedding when only non-watched columns change.
  # skip_unchanged: false
{{- end }}
{{- if has .Adapters "kafka" }}

# ─── Kafka Adapter ───────────────────────────────────────────────────────

kafka:
  # Broker addresses.
  brokers:
    - "localhost:9092"

  # Fixed topic (empty = channel-derived: pgcdc:orders -> pgcdc.orders).
  # topic: ""

  # SASL authentication.
  # sasl_mechanism: ""        # plain, SCRAM-SHA-256, SCRAM-SHA-512
  # sasl_username: ""
  # sasl_password: ""

  # TLS settings.
  # tls: false
  # tls_ca_file: ""

  # Encoding: json, avro, protobuf (requires schema_registry_url in encoding section).
  # encoding: json

  # Exactly-once delivery via Kafka transactions (set a unique ID to enable).
  # transactional_id: ""

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if has .Adapters "nats" }}

# ─── NATS JetStream Adapter ──────────────────────────────────────────────

nats:
  # NATS server URL.
  url: "nats://localhost:4222"

  # Subject prefix (channels mapped: pgcdc:orders -> pgcdc.orders).
  subject: pgcdc

  # JetStream stream name.
  stream: pgcdc

  # Credentials file for NATS authentication.
  # cred_file: ""

  # Stream max age.
  max_age: 24h

  # Encoding: json, avro, protobuf
  # encoding: json

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if has .Adapters "s3" }}

# ─── S3 Adapter ──────────────────────────────────────────────────────────

s3:
  # S3 bucket name (required).
  bucket: "my-cdc-bucket"

  # Key prefix for partitioned objects.
  # prefix: ""

  # S3-compatible endpoint (for MinIO, R2, etc.).
  # endpoint: ""

  region: us-east-1

  # Credentials (also reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env).
  # access_key_id: ""
  # secret_access_key: ""

  # Output format: jsonl or parquet.
  format: jsonl

  # Flush triggers.
  flush_interval: 1m
  flush_size: 10000

  # Drain timeout for graceful shutdown.
  drain_timeout: 30s

  backoff_base: 5s
  backoff_cap: 60s
{{- end }}
{{- if has .Adapters "search" }}

# ─── Search Adapter ──────────────────────────────────────────────────────

search:
  # Engine: typesense or meilisearch.
  engine: typesense

  # Search engine URL.
  url: "http://localhost:8108"

  # API key.
  api_key: ""

  # Target index/collection name.
  index: events

  # Primary key column.
  id_column: id

  # Batch settings.
  batch_size: 100
  batch_interval: 1s

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if has .Adapters "redis" }}

# ─── Redis Adapter ───────────────────────────────────────────────────────

redis:
  # Redis URL.
  url: "redis://localhost:6379"

  # Mode: invalidate (DEL on change) or sync (SET/DEL).
  mode: invalidate

  # Key prefix for Redis keys.
  # key_prefix: "pgcdc:"

  # Primary key column.
  id_column: id

  backoff_base: 1s
  backoff_cap: 30s
{{- end }}
{{- if has .Adapters "grpc" }}

# ─── gRPC Adapter ────────────────────────────────────────────────────────

grpc:
  # gRPC server listen address.
  addr: ":9090"
{{- end }}
{{- if has .Adapters "kafkaserver" }}

# ─── Kafka Server Adapter ────────────────────────────────────────────────

kafkaserver:
  # TCP listen address for Kafka wire protocol.
  addr: ":9092"

  # Number of partitions per topic.
  partition_count: 8

  # Ring buffer size per partition.
  buffer_size: 10000

  # Consumer group session timeout.
  session_timeout: 30s

  # Column used for partition key hashing.
  key_column: id
{{- end }}
{{- if has .Adapters "view" }}

# ─── Streaming SQL Views ─────────────────────────────────────────────────

# Define streaming SQL views over CDC events.
# views:
#   - name: orders_per_minute
#     query: >
#       SELECT channel, COUNT(*) as cnt
#       FROM pgcdc_events
#       TUMBLING WINDOW 1m
#       GROUP BY channel
#     emit: row          # row (one event per group) or batch (single event)
#     max_groups: 100000
{{- end }}

# ─── Transforms ──────────────────────────────────────────────────────────

# Transform pipeline applied to events before delivery.
# transforms:
#   global:
#     - type: drop_columns
#       columns: [internal_id, secret]
#     - type: filter
#       filter:
#         operations: [INSERT, UPDATE]
#   adapter:
#     webhook:
#       - type: mask
#         fields:
#           - field: email
#             mode: hash
#       - type: rename_fields
#         mapping:
#           user_id: userId

# ─── Routes ──────────────────────────────────────────────────────────────

# Route specific channels to specific adapters.
# Adapters without routes receive all events.
# routes:
#   webhook: [orders, payments]
#   stdout: [orders, users, payments]
{{- if or (has .Adapters "kafka") (has .Adapters "nats") }}

# ─── Encoding ────────────────────────────────────────────────────────────

# Schema Registry for Avro/Protobuf encoding (used by kafka/nats adapters).
# encoding:
#   schema_registry_url: "http://localhost:8081"
#   schema_registry_username: ""
#   schema_registry_password: ""
{{- end }}
{{- if eq .Detector "wal" }}

# ─── Backpressure ────────────────────────────────────────────────────────

# Source-aware backpressure (requires detector: wal + persistent_slot).
# backpressure:
#   enabled: false
#   warn_threshold: 524288000      # 500MB - yellow zone
#   critical_threshold: 2147483648 # 2GB - red zone
#   max_throttle: 500ms
#   poll_interval: 10s
#   adapter_priorities:
#     webhook: critical
#     stdout: best-effort

# ─── TOAST Cache ─────────────────────────────────────────────────────────

# Resolve unchanged TOAST columns without REPLICA IDENTITY FULL.
# toast_cache: false
# toast_cache_max_entries: 100000
{{- end }}

# ─── Plugins ─────────────────────────────────────────────────────────────

# Wasm plugin system (Extism-based, any language with PDK support).
# plugins:
#   transforms:
#     - name: add-timestamp
#       path: ./plugins/add-timestamp.wasm
#       encoding: json    # json (default) or protobuf
#       scope: global     # global or { adapter: "webhook" }
#   adapters:
#     - name: custom-sink
#       path: ./plugins/custom-sink.wasm
#   dlq:
#     path: ./plugins/custom-dlq.wasm
#   checkpoint:
#     path: ./plugins/custom-checkpoint.wasm

# ─── OpenTelemetry ───────────────────────────────────────────────────────

# otel:
#   exporter: none       # none, stdout, otlp
#   endpoint: ""         # OTLP gRPC endpoint
#   sample_ratio: 1.0
`
