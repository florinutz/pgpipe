package cmd

import (
	"time"

	"github.com/florinutz/pgcdc/registry"
	"github.com/spf13/cobra"
)

func init() {
	f := listenCmd.Flags()

	f.StringSliceP("channel", "c", nil, "PG channels to listen on (repeatable)")
	f.StringSliceP("adapter", "a", []string{"stdout"}, "adapters: webhook, sse, stdout (repeatable)")
	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	f.String("metrics-addr", "", "standalone metrics/health server address (e.g. :9090)")
	f.String("detector", "listen_notify", "detector type: listen_notify, wal, outbox, mysql, or mongodb")
	f.String("publication", "", "PostgreSQL publication name (required for --detector wal)")
	f.Bool("all-tables", false, "auto-create a FOR ALL TABLES publication and start WAL streaming (zero-config)")
	f.Bool("tx-metadata", false, "include transaction metadata in WAL events (xid, commit_time, seq)")
	f.Bool("tx-markers", false, "emit BEGIN/COMMIT marker events (implies --tx-metadata)")

	// Persistent slot flags.
	f.Bool("persistent-slot", false, "use a named persistent replication slot (survives disconnects)")
	f.String("slot-name", "", "replication slot name (default: pgcdc_<publication>)")
	f.String("checkpoint-db", "", "PostgreSQL URL for checkpoint storage (default: same as --db)")

	// Schema flags.
	f.Bool("include-schema", false, "include column type metadata in WAL events")
	f.Bool("schema-events", false, "emit SCHEMA_CHANGE events when table columns change")

	// Heartbeat flags.
	f.Duration("heartbeat-interval", 30*time.Second, "heartbeat interval for WAL slot advancement (0 to disable)")
	f.String("heartbeat-table", "pgcdc_heartbeat", "heartbeat table name")
	f.Int64("slot-lag-warn", 100*1024*1024, "slot lag warning threshold in bytes (default 100MB)")

	// Snapshot-first flags.
	f.Bool("snapshot-first", false, "run a table snapshot before live WAL streaming (requires --detector wal)")
	f.String("snapshot-table", "", "table to snapshot (required with --snapshot-first)")
	f.String("snapshot-where", "", "optional WHERE clause for snapshot")

	// TOAST cache flags.
	f.Bool("toast-cache", false, "enable in-memory TOAST column cache to resolve unchanged columns without REPLICA IDENTITY FULL (requires --detector wal)")
	f.Int("toast-cache-max-entries", 100000, "maximum number of rows in the TOAST cache")

	// Incremental snapshot flags.
	f.Bool("incremental-snapshot", false, "enable incremental snapshots via signal table (requires --detector wal)")
	f.String("snapshot-signal-table", "pgcdc_signals", "signal table name")
	f.Int("snapshot-chunk-size", 1000, "rows per chunk for incremental snapshots")
	f.Duration("snapshot-chunk-delay", 0, "delay between chunks for incremental snapshots")
	f.String("snapshot-progress-db", "", "PostgreSQL URL for progress storage (default: same as --db)")

	// Outbox detector flags.
	f.String("outbox-table", "pgcdc_outbox", "outbox table name")
	f.Duration("outbox-poll-interval", 500*time.Millisecond, "outbox polling interval")
	f.Int("outbox-batch-size", 100, "outbox batch size per poll")
	f.Bool("outbox-keep-processed", false, "keep processed outbox rows (set processed_at instead of DELETE)")

	// MySQL detector flags.
	f.String("mysql-addr", "", "MySQL address (host:port)")
	f.String("mysql-user", "", "MySQL user")
	f.String("mysql-password", "", "MySQL password")
	f.Uint32("mysql-server-id", 0, "MySQL server ID for replication (must be > 0)")
	f.StringSlice("mysql-tables", nil, "MySQL tables to replicate (schema.table format, repeatable)")
	f.Bool("mysql-gtid", false, "use GTID-based replication instead of file+position")
	f.String("mysql-flavor", "mysql", "MySQL flavor: mysql or mariadb")
	f.String("mysql-binlog-prefix", "mysql-bin", "binlog filename prefix for position decoding")

	// MongoDB detector flags.
	f.String("mongodb-uri", "", "MongoDB connection URI")
	f.String("mongodb-scope", "collection", "MongoDB watch scope: collection, database, or cluster")
	f.String("mongodb-database", "", "MongoDB database name")
	f.StringSlice("mongodb-collections", nil, "MongoDB collections to watch (repeatable)")
	f.String("mongodb-full-document", "updateLookup", "MongoDB fullDocument option: updateLookup, default, whenAvailable, required")
	f.String("mongodb-metadata-db", "", "MongoDB database for resume token storage (default: same as --mongodb-database)")
	f.String("mongodb-metadata-coll", "pgcdc_resume_tokens", "MongoDB collection for resume token storage")

	// Bus mode flag.
	f.String("bus-mode", "fast", "bus fan-out mode: fast (drop on full) or reliable (block on full)")

	// Cooperative checkpoint flag.
	f.Bool("cooperative-checkpoint", false, "checkpoint advances only after all adapters acknowledge (requires --persistent-slot and --detector wal)")

	// Transform flags.
	f.StringSlice("drop-columns", nil, "global: drop these columns from event payloads (repeatable)")
	f.StringSlice("filter-operations", nil, "global: only pass events with these operations (e.g. INSERT,UPDATE)")
	f.Bool("debezium-envelope", false, "global: rewrite payloads into Debezium-compatible envelope format")
	f.String("debezium-connector-name", "pgcdc", "Debezium source.name field (requires --debezium-envelope)")
	f.String("debezium-database", "", "Debezium source.db field (requires --debezium-envelope)")
	f.Bool("cloudevents-envelope", false, "global: rewrite payloads into CloudEvents structured-mode JSON")
	f.String("cloudevents-source", "/pgcdc", "CloudEvents source URI-reference (requires --cloudevents-envelope)")
	f.String("cloudevents-type-prefix", "io.pgcdc.change", "CloudEvents type prefix (requires --cloudevents-envelope)")

	// Backpressure flags.
	f.Bool("backpressure", false, "enable source-aware backpressure (requires --detector wal and --persistent-slot)")
	f.Int64("bp-warn-threshold", 500*1024*1024, "yellow zone threshold in bytes (default 500MB)")
	f.Int64("bp-critical-threshold", 2*1024*1024*1024, "red zone threshold in bytes (default 2GB)")
	f.Duration("bp-max-throttle", 500*time.Millisecond, "max sleep between WAL reads in yellow zone")
	f.Duration("bp-poll-interval", 10*time.Second, "backpressure lag polling interval")
	f.StringSlice("adapter-priority", nil, "adapter priority: name=critical|normal|best-effort (repeatable)")

	// Plugin flags.
	f.StringSlice("plugin-transform", nil, "wasm transform plugin paths (repeatable)")
	f.StringSlice("plugin-transform-config", nil, "JSON config for each plugin-transform (parallel order)")
	f.StringSlice("plugin-adapter", nil, "wasm adapter plugin paths (repeatable)")
	f.StringSlice("plugin-adapter-name", nil, "names for each plugin-adapter (parallel order)")
	f.StringSlice("plugin-adapter-config", nil, "JSON config for each plugin-adapter (parallel order)")
	f.String("dlq-plugin-path", "", "wasm DLQ plugin path (use with --dlq plugin)")
	f.String("dlq-plugin-config", "", "JSON config for DLQ plugin")
	f.String("checkpoint-plugin", "", "wasm checkpoint store plugin path")
	f.String("checkpoint-plugin-config", "", "JSON config for checkpoint plugin")

	// DLQ flags.
	f.String("dlq", "stderr", "dead letter queue backend: stderr, pg_table, or none")
	f.String("dlq-table", "pgcdc_dead_letters", "DLQ table name (for pg_table backend)")
	f.String("dlq-db", "", "PostgreSQL URL for DLQ table (default: same as --db)")

	// Route flags.
	f.StringSlice("route", nil, "route events to adapter: adapter=channel1,channel2 (repeatable)")

	// Startup validation flag.
	f.Bool("skip-validation", false, "skip adapter startup validation")

	// Schema migrations flag.
	f.Bool("skip-migrations", false, "skip internal schema migrations (for read-only DB users)")

	// View query flag (repeatable, name:query format).
	f.StringSlice("view-query", nil, "streaming SQL view query: name:query (repeatable, e.g. --view-query 'counts:SELECT COUNT(*) FROM pgcdc_events GROUP BY channel TUMBLING WINDOW 1m')")

	// Shared encoding flags (used by kafka and nats; kept here so they are always
	// registered regardless of which adapters are built in or out).
	f.String("schema-registry-url", "", "Confluent Schema Registry URL")
	f.String("schema-registry-username", "", "Schema Registry basic auth username")
	f.String("schema-registry-password", "", "Schema Registry basic auth password")

	// OpenTelemetry flags.
	f.String("otel-exporter", "none", "OTel trace exporter: none, stdout, or otlp")
	f.String("otel-endpoint", "", "OTLP gRPC endpoint (overrides OTEL_EXPORTER_OTLP_ENDPOINT)")
	f.Float64("otel-sample-ratio", 1.0, "trace sampling ratio (0.0-1.0)")

	// Inspector flags.
	f.Int("inspect-buffer", 100, "inspector ring buffer size per tap point (0 to disable)")

	// Schema store flags.
	f.Bool("schema-store", false, "enable schema versioning (track column changes)")
	f.String("schema-store-type", "memory", "schema store backend: memory or pg")
	f.String("schema-db", "", "PostgreSQL URL for schema store (default: same as --db)")

	// Nack window flags.
	f.Int("dlq-window-size", 0, "nack window size for adaptive DLQ (0 to disable)")
	f.Int("dlq-window-threshold", 0, "nack count to trigger adapter pause (default: window_size/2)")

	// Multi-detector flags.
	f.String("detector-mode", "sequential", "multi-detector mode: sequential, parallel, or failover")

	// CEL filter CLI shortcut.
	f.String("filter-cel", "", "global CEL filter expression (e.g. 'operation == \"INSERT\"')")

	// Dedup CLI shortcut.
	f.String("dedup-key", "", "dedup key path (e.g. payload.id; empty = event ID)")
	f.Duration("dedup-window", 0, "dedup time window (e.g. 1h)")
	f.Int("dedup-max-keys", 100000, "dedup LRU cache max keys")

	// TLS flags.
	f.String("sse-tls-cert", "", "path to TLS certificate file for HTTP server")
	f.String("sse-tls-key", "", "path to TLS private key file for HTTP server")
	f.String("metrics-tls-cert", "", "path to TLS certificate file for metrics server")
	f.String("metrics-tls-key", "", "path to TLS private key file for metrics server")
}

// bindListenFlags binds all listen command flags to viper keys. Called in PreRunE
// so only the active command's bindings are established (avoids global viper key
// collisions between listen and snapshot commands).
func bindListenFlags(cmd *cobra.Command, _ []string) error {
	if err := bindFlagsToViper(cmd.Flags(), [][2]string{
		// Core.
		{"channel", "channels"},
		{"adapter", "adapters"},
		{"db", "database_url"},
		{"bus-mode", "bus.mode"},
		{"metrics-addr", "metrics_addr"},
		{"skip-validation", "skip_validation"},
		{"skip-migrations", "skip_migrations"},

		// Detector.
		{"detector", "detector.type"},
		{"publication", "detector.publication"},
		{"all-tables", "detector.all_tables"},
		{"tx-metadata", "detector.tx_metadata"},
		{"tx-markers", "detector.tx_markers"},
		{"persistent-slot", "detector.persistent_slot"},
		{"slot-name", "detector.slot_name"},
		{"checkpoint-db", "detector.checkpoint_db"},
		{"include-schema", "detector.include_schema"},
		{"schema-events", "detector.schema_events"},
		{"heartbeat-interval", "detector.heartbeat_interval"},
		{"heartbeat-table", "detector.heartbeat_table"},
		{"slot-lag-warn", "detector.slot_lag_warn"},
		{"snapshot-first", "detector.snapshot_first"},
		{"toast-cache", "detector.toast_cache"},
		{"toast-cache-max-entries", "detector.toast_cache_max_entries"},
		{"cooperative-checkpoint", "detector.cooperative_checkpoint"},

		// Snapshot.
		{"snapshot-table", "snapshot.table"},
		{"snapshot-where", "snapshot.where"},

		// Incremental snapshot.
		{"incremental-snapshot", "incremental_snapshot.enabled"},
		{"snapshot-signal-table", "incremental_snapshot.signal_table"},
		{"snapshot-chunk-size", "incremental_snapshot.chunk_size"},
		{"snapshot-chunk-delay", "incremental_snapshot.chunk_delay"},
		{"snapshot-progress-db", "incremental_snapshot.progress_db"},

		// Backpressure.
		{"backpressure", "backpressure.enabled"},
		{"bp-warn-threshold", "backpressure.warn_threshold"},
		{"bp-critical-threshold", "backpressure.critical_threshold"},
		{"bp-max-throttle", "backpressure.max_throttle"},
		{"bp-poll-interval", "backpressure.poll_interval"},

		// DLQ.
		{"dlq", "dlq.type"},
		{"dlq-table", "dlq.table"},
		{"dlq-db", "dlq.db_url"},

		// Outbox.
		{"outbox-table", "outbox.table"},
		{"outbox-poll-interval", "outbox.poll_interval"},
		{"outbox-batch-size", "outbox.batch_size"},
		{"outbox-keep-processed", "outbox.keep_processed"},

		// MySQL.
		{"mysql-addr", "mysql.addr"},
		{"mysql-user", "mysql.user"},
		{"mysql-password", "mysql.password"},
		{"mysql-server-id", "mysql.server_id"},
		{"mysql-tables", "mysql.tables"},
		{"mysql-gtid", "mysql.use_gtid"},
		{"mysql-flavor", "mysql.flavor"},
		{"mysql-binlog-prefix", "mysql.binlog_prefix"},

		// MongoDB.
		{"mongodb-uri", "mongodb.uri"},
		{"mongodb-scope", "mongodb.scope"},
		{"mongodb-database", "mongodb.database"},
		{"mongodb-collections", "mongodb.collections"},
		{"mongodb-full-document", "mongodb.full_document"},
		{"mongodb-metadata-db", "mongodb.metadata_db"},
		{"mongodb-metadata-coll", "mongodb.metadata_coll"},

		// SQLite.
		{"sqlite-db", "sqlite.db_path"},
		{"sqlite-poll-interval", "sqlite.poll_interval"},
		{"sqlite-batch-size", "sqlite.batch_size"},
		{"sqlite-keep-processed", "sqlite.keep_processed"},

		// NATS consumer.
		{"nats-consumer-stream", "nats_consumer.stream"},
		{"nats-consumer-subjects", "nats_consumer.subjects"},
		{"nats-consumer-durable", "nats_consumer.durable"},

		// Shared encoding / Schema Registry.
		{"schema-registry-url", "encoding.schema_registry_url"},
		{"schema-registry-username", "encoding.schema_registry_username"},
		{"schema-registry-password", "encoding.schema_registry_password"},

		// OpenTelemetry.
		{"otel-exporter", "otel.exporter"},
		{"otel-endpoint", "otel.endpoint"},
		{"otel-sample-ratio", "otel.sample_ratio"},

		// Inspector.
		{"inspect-buffer", "inspector.buffer_size"},

		// Schema store.
		{"schema-store", "schema.enabled"},
		{"schema-store-type", "schema.store"},
		{"schema-db", "schema.db_url"},

		// Nack window.
		{"dlq-window-size", "dlq.window_size"},
		{"dlq-window-threshold", "dlq.window_threshold"},

		// Kafka consumer detector.
		{"kafka-consumer-topics", "kafka_consumer.topics"},
		{"kafka-consumer-group", "kafka_consumer.group"},
		{"kafka-consumer-offset", "kafka_consumer.offset"},

		// Webhook gateway.
		{"webhookgw-max-body", "webhook_gateway.max_body_size"},
		{"webhookgw-source", "webhook_gateway.cli_sources"},

		// Multi-detector.
		{"detector-mode", "detector_mode"},

		// TLS.
		{"sse-tls-cert", "sse.tls_cert_file"},
		{"sse-tls-key", "sse.tls_key_file"},
		{"metrics-tls-cert", "metrics_tls_cert_file"},
		{"metrics-tls-key", "metrics_tls_key_file"},
	}); err != nil {
		return err
	}

	// Adapter-specific viper bindings from registry entries.
	return registry.BindAdapterViperKeys(cmd.Flags(), bindFlagsToViper)
}
