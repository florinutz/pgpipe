package config

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL         string                      `mapstructure:"database_url"`
	Channels            []string                    `mapstructure:"channels"`
	Adapters            []string                    `mapstructure:"adapters"`
	LogLevel            string                      `mapstructure:"log_level"`
	LogFormat           string                      `mapstructure:"log_format"`
	ShutdownTimeout     time.Duration               `mapstructure:"shutdown_timeout"`
	MetricsAddr         string                      `mapstructure:"metrics_addr"`
	SkipValidation      bool                        `mapstructure:"skip_validation"`
	SkipMigrations      bool                        `mapstructure:"skip_migrations"`
	Bus                 BusConfig                   `mapstructure:"bus"`
	Webhook             WebhookConfig               `mapstructure:"webhook"`
	SSE                 SSEConfig                   `mapstructure:"sse"`
	File                FileConfig                  `mapstructure:"file"`
	Exec                ExecConfig                  `mapstructure:"exec"`
	PGTable             PGTableConfig               `mapstructure:"pg_table"`
	WebSocket           WebSocketConfig             `mapstructure:"websocket"`
	Detector            DetectorConfig              `mapstructure:"detector"`
	Snapshot            SnapshotConfig              `mapstructure:"snapshot"`
	Embedding           EmbeddingConfig             `mapstructure:"embedding"`
	Iceberg             IcebergConfig               `mapstructure:"iceberg"`
	Nats                NatsConfig                  `mapstructure:"nats"`
	Outbox              OutboxConfig                `mapstructure:"outbox"`
	DLQ                 DLQConfig                   `mapstructure:"dlq"`
	Search              SearchConfig                `mapstructure:"search"`
	Redis               RedisConfig                 `mapstructure:"redis"`
	GRPC                GRPCConfig                  `mapstructure:"grpc"`
	Kafka               KafkaConfig                 `mapstructure:"kafka"`
	S3                  S3Config                    `mapstructure:"s3"`
	IncrementalSnapshot IncrementalSnapshotConfig   `mapstructure:"incremental_snapshot"`
	Transforms          TransformConfig             `mapstructure:"transforms"`
	Routes              map[string][]string         `mapstructure:"routes"` // adapter -> channels
	Backpressure        BackpressureConfig          `mapstructure:"backpressure"`
	Plugins             PluginConfig                `mapstructure:"plugins"`
	Encoding            EncodingConfig              `mapstructure:"encoding"`
	MySQL               MySQLConfig                 `mapstructure:"mysql"`
	MongoDB             MongoDBConfig               `mapstructure:"mongodb"`
	OTel                OTelConfig                  `mapstructure:"otel"`
	KafkaServer         KafkaServerConfig           `mapstructure:"kafkaserver"`
	Views               []ViewConfig                `mapstructure:"views"`
	Middleware          map[string]MiddlewareConfig `mapstructure:"middleware"` // adapter name -> middleware config
}

type OTelConfig struct {
	Exporter    string  `mapstructure:"exporter"`
	Endpoint    string  `mapstructure:"endpoint"`
	SampleRatio float64 `mapstructure:"sample_ratio"`
}

type BusConfig struct {
	BufferSize int    `mapstructure:"buffer_size"`
	Mode       string `mapstructure:"mode"` // "fast" (default) or "reliable"
}

type WebhookConfig struct {
	URL            string            `mapstructure:"url"`
	Headers        map[string]string `mapstructure:"headers"`
	SigningKey     string            `mapstructure:"signing_key"`
	MaxRetries     int               `mapstructure:"max_retries"`
	Timeout        time.Duration     `mapstructure:"timeout"`
	BackoffBase    time.Duration     `mapstructure:"backoff_base"`
	BackoffCap     time.Duration     `mapstructure:"backoff_cap"`
	CBMaxFailures  int               `mapstructure:"cb_max_failures"`
	CBResetTimeout time.Duration     `mapstructure:"cb_reset_timeout"`
	RateLimit      float64           `mapstructure:"rate_limit"`
	RateLimitBurst int               `mapstructure:"rate_limit_burst"`
}

type SSEConfig struct {
	Addr              string        `mapstructure:"addr"`
	CORSOrigins       []string      `mapstructure:"cors_origins"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"`
}

type FileConfig struct {
	Path     string `mapstructure:"path"`
	MaxSize  int64  `mapstructure:"max_size"`
	MaxFiles int    `mapstructure:"max_files"`
}

type ExecConfig struct {
	Command     string        `mapstructure:"command"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type PGTableConfig struct {
	URL         string        `mapstructure:"url"`
	Table       string        `mapstructure:"table"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type WebSocketConfig struct {
	PingInterval time.Duration `mapstructure:"ping_interval"`
}

type SnapshotConfig struct {
	Table     string `mapstructure:"table"`
	Where     string `mapstructure:"where"`
	BatchSize int    `mapstructure:"batch_size"`
}

type EmbeddingConfig struct {
	APIURL         string        `mapstructure:"api_url"`
	APIKey         string        `mapstructure:"api_key"`
	Model          string        `mapstructure:"model"`
	Columns        []string      `mapstructure:"columns"`
	IDColumn       string        `mapstructure:"id_column"`
	Table          string        `mapstructure:"table"`
	DBURL          string        `mapstructure:"db_url"`
	Dimension      int           `mapstructure:"dimension"`
	MaxRetries     int           `mapstructure:"max_retries"`
	Timeout        time.Duration `mapstructure:"timeout"`
	BackoffBase    time.Duration `mapstructure:"backoff_base"`
	BackoffCap     time.Duration `mapstructure:"backoff_cap"`
	CBMaxFailures  int           `mapstructure:"cb_max_failures"`
	CBResetTimeout time.Duration `mapstructure:"cb_reset_timeout"`
	RateLimit      float64       `mapstructure:"rate_limit"`
	RateLimitBurst int           `mapstructure:"rate_limit_burst"`
	SkipUnchanged  bool          `mapstructure:"skip_unchanged"`
}

type IcebergConfig struct {
	CatalogType   string        `mapstructure:"catalog_type"`
	CatalogURI    string        `mapstructure:"catalog_uri"`
	Warehouse     string        `mapstructure:"warehouse"`
	Namespace     string        `mapstructure:"namespace"`
	Table         string        `mapstructure:"table"`
	Mode          string        `mapstructure:"mode"`
	SchemaMode    string        `mapstructure:"schema_mode"`
	PrimaryKeys   []string      `mapstructure:"primary_keys"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	FlushSize     int           `mapstructure:"flush_size"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
}

type DetectorConfig struct {
	Type              string        `mapstructure:"type"`
	BackoffBase       time.Duration `mapstructure:"backoff_base"`
	BackoffCap        time.Duration `mapstructure:"backoff_cap"`
	Publication       string        `mapstructure:"publication"`
	TxMetadata        bool          `mapstructure:"tx_metadata"`
	TxMarkers         bool          `mapstructure:"tx_markers"`
	PersistentSlot    bool          `mapstructure:"persistent_slot"`
	SlotName          string        `mapstructure:"slot_name"`
	CheckpointDB      string        `mapstructure:"checkpoint_db"`
	IncludeSchema     bool          `mapstructure:"include_schema"`
	SchemaEvents      bool          `mapstructure:"schema_events"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	HeartbeatTable    string        `mapstructure:"heartbeat_table"`
	SlotLagWarn       int64         `mapstructure:"slot_lag_warn"`
	// Feature flags (used for cross-cutting validation).
	ToastCache            bool `mapstructure:"toast_cache"`
	CooperativeCheckpoint bool `mapstructure:"cooperative_checkpoint"`
	BackpressureEnabled   bool `mapstructure:"backpressure_enabled"`
	IncrementalSnapshot   bool `mapstructure:"incremental_snapshot"`
	SnapshotFirst         bool `mapstructure:"snapshot_first"`
}

type NatsConfig struct {
	URL         string        `mapstructure:"url"`
	Subject     string        `mapstructure:"subject"`
	Stream      string        `mapstructure:"stream"`
	CredFile    string        `mapstructure:"cred_file"`
	MaxAge      time.Duration `mapstructure:"max_age"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
	Encoding    string        `mapstructure:"encoding"` // json, avro, protobuf
}

type OutboxConfig struct {
	Table         string        `mapstructure:"table"`
	PollInterval  time.Duration `mapstructure:"poll_interval"`
	BatchSize     int           `mapstructure:"batch_size"`
	KeepProcessed bool          `mapstructure:"keep_processed"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
}

type DLQConfig struct {
	Type  string `mapstructure:"type"`
	Table string `mapstructure:"table"`
	DBURL string `mapstructure:"db_url"`
}

type SearchConfig struct {
	Engine        string        `mapstructure:"engine"`
	URL           string        `mapstructure:"url"`
	APIKey        string        `mapstructure:"api_key"`
	Index         string        `mapstructure:"index"`
	IDColumn      string        `mapstructure:"id_column"`
	BatchSize     int           `mapstructure:"batch_size"`
	BatchInterval time.Duration `mapstructure:"batch_interval"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
}

type RedisConfig struct {
	URL         string        `mapstructure:"url"`
	Mode        string        `mapstructure:"mode"`
	KeyPrefix   string        `mapstructure:"key_prefix"`
	IDColumn    string        `mapstructure:"id_column"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type GRPCConfig struct {
	Addr string `mapstructure:"addr"`
}

type KafkaConfig struct {
	Brokers         []string      `mapstructure:"brokers"`
	Topic           string        `mapstructure:"topic"`
	SASLMechanism   string        `mapstructure:"sasl_mechanism"`
	SASLUsername    string        `mapstructure:"sasl_username"`
	SASLPassword    string        `mapstructure:"sasl_password"`
	TLS             bool          `mapstructure:"tls"`
	TLSCAFile       string        `mapstructure:"tls_ca_file"`
	BackoffBase     time.Duration `mapstructure:"backoff_base"`
	BackoffCap      time.Duration `mapstructure:"backoff_cap"`
	Encoding        string        `mapstructure:"encoding"`         // json, avro, protobuf
	TransactionalID string        `mapstructure:"transactional_id"` // empty = idempotent only
	CBMaxFailures   int           `mapstructure:"cb_max_failures"`
	CBResetTimeout  time.Duration `mapstructure:"cb_reset_timeout"`
	RateLimit       float64       `mapstructure:"rate_limit"`
	RateLimitBurst  int           `mapstructure:"rate_limit_burst"`
}

type S3Config struct {
	Bucket          string        `mapstructure:"bucket"`
	Prefix          string        `mapstructure:"prefix"`
	Endpoint        string        `mapstructure:"endpoint"`
	Region          string        `mapstructure:"region"`
	AccessKeyID     string        `mapstructure:"access_key_id"`
	SecretAccessKey string        `mapstructure:"secret_access_key"`
	Format          string        `mapstructure:"format"` // "jsonl" or "parquet"
	FlushInterval   time.Duration `mapstructure:"flush_interval"`
	FlushSize       int           `mapstructure:"flush_size"`
	DrainTimeout    time.Duration `mapstructure:"drain_timeout"`
	BackoffBase     time.Duration `mapstructure:"backoff_base"`
	BackoffCap      time.Duration `mapstructure:"backoff_cap"`
}

type IncrementalSnapshotConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	SignalTable string        `mapstructure:"signal_table"`
	ChunkSize   int           `mapstructure:"chunk_size"`
	ChunkDelay  time.Duration `mapstructure:"chunk_delay"`
	ProgressDB  string        `mapstructure:"progress_db"`
}

type TransformConfig struct {
	Global  []TransformSpec            `mapstructure:"global"`
	Adapter map[string][]TransformSpec `mapstructure:"adapter"`
}

type TransformSpec struct {
	Type        string            `mapstructure:"type"`        // drop_columns, rename_fields, mask, filter, debezium, cloudevents
	Columns     []string          `mapstructure:"columns"`     // for drop_columns
	Mapping     map[string]string `mapstructure:"mapping"`     // for rename_fields
	Fields      []MaskFieldSpec   `mapstructure:"fields"`      // for mask
	Filter      FilterSpec        `mapstructure:"filter"`      // for filter
	Debezium    DebeziumSpec      `mapstructure:"debezium"`    // for debezium
	CloudEvents CloudEventsSpec   `mapstructure:"cloudevents"` // for cloudevents
}

type CloudEventsSpec struct {
	Source     string `mapstructure:"source"`
	TypePrefix string `mapstructure:"type_prefix"`
}

type DebeziumSpec struct {
	ConnectorName string `mapstructure:"connector_name"`
	Database      string `mapstructure:"database"`
}

type MaskFieldSpec struct {
	Field string `mapstructure:"field"`
	Mode  string `mapstructure:"mode"`
}

type FilterSpec struct {
	Field      string   `mapstructure:"field"`
	Equals     string   `mapstructure:"equals"`
	In         []string `mapstructure:"in"`
	Operations []string `mapstructure:"operations"`
}

type BackpressureConfig struct {
	Enabled           bool              `mapstructure:"enabled"`
	WarnThreshold     int64             `mapstructure:"warn_threshold"`
	CriticalThreshold int64             `mapstructure:"critical_threshold"`
	MaxThrottle       time.Duration     `mapstructure:"max_throttle"`
	PollInterval      time.Duration     `mapstructure:"poll_interval"`
	AdapterPriorities map[string]string `mapstructure:"adapter_priorities"` // name -> "critical"|"normal"|"best-effort"
}

type PluginConfig struct {
	Transforms []PluginTransformSpec `mapstructure:"transforms"`
	Adapters   []PluginAdapterSpec   `mapstructure:"adapters"`
	DLQ        *PluginSpec           `mapstructure:"dlq"`
	Checkpoint *PluginSpec           `mapstructure:"checkpoint"`
}

type PluginTransformSpec struct {
	Name     string         `mapstructure:"name"`
	Path     string         `mapstructure:"path"`
	Config   map[string]any `mapstructure:"config"`
	Encoding string         `mapstructure:"encoding"` // "json" (default) or "protobuf"
	Scope    any            `mapstructure:"scope"`    // "global" or map with "adapter" key
}

type PluginAdapterSpec struct {
	Name   string         `mapstructure:"name"`
	Path   string         `mapstructure:"path"`
	Config map[string]any `mapstructure:"config"`
}

type PluginSpec struct {
	Path   string         `mapstructure:"path"`
	Config map[string]any `mapstructure:"config"`
}

type MySQLConfig struct {
	Addr         string        `mapstructure:"addr"` // host:port
	User         string        `mapstructure:"user"`
	Password     string        `mapstructure:"password"`
	ServerID     uint32        `mapstructure:"server_id"`
	Tables       []string      `mapstructure:"tables"` // schema.table filter
	UseGTID      bool          `mapstructure:"use_gtid"`
	Flavor       string        `mapstructure:"flavor"`        // "mysql" or "mariadb"
	BinlogPrefix string        `mapstructure:"binlog_prefix"` // default: "mysql-bin"
	BackoffBase  time.Duration `mapstructure:"backoff_base"`
	BackoffCap   time.Duration `mapstructure:"backoff_cap"`
}

type MongoDBConfig struct {
	URI          string        `mapstructure:"uri"`
	Scope        string        `mapstructure:"scope"` // "collection", "database", or "cluster"
	Database     string        `mapstructure:"database"`
	Collections  []string      `mapstructure:"collections"`
	FullDocument string        `mapstructure:"full_document"` // "updateLookup", "default", etc.
	MetadataDB   string        `mapstructure:"metadata_db"`
	MetadataColl string        `mapstructure:"metadata_coll"`
	BackoffBase  time.Duration `mapstructure:"backoff_base"`
	BackoffCap   time.Duration `mapstructure:"backoff_cap"`
}

type KafkaServerConfig struct {
	Addr           string        `mapstructure:"addr"`
	PartitionCount int           `mapstructure:"partition_count"`
	BufferSize     int           `mapstructure:"buffer_size"`
	SessionTimeout time.Duration `mapstructure:"session_timeout"`
	CheckpointDB   string        `mapstructure:"checkpoint_db"`
	KeyColumn      string        `mapstructure:"key_column"`
}

type ViewConfig struct {
	Name      string `mapstructure:"name"`
	Query     string `mapstructure:"query"`
	Emit      string `mapstructure:"emit"`       // "row" (default) or "batch"
	MaxGroups int    `mapstructure:"max_groups"` // default 100000
}

type MiddlewareConfig struct {
	Retry          *MiddlewareRetryConfig          `mapstructure:"retry"`
	CircuitBreaker *MiddlewareCircuitBreakerConfig `mapstructure:"circuit_breaker"`
	RateLimit      *MiddlewareRateLimitConfig      `mapstructure:"rate_limit"`
}

type MiddlewareRetryConfig struct {
	MaxRetries  int           `mapstructure:"max_retries"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type MiddlewareCircuitBreakerConfig struct {
	MaxFailures  int           `mapstructure:"max_failures"`
	ResetTimeout time.Duration `mapstructure:"reset_timeout"`
}

type MiddlewareRateLimitConfig struct {
	EventsPerSecond float64 `mapstructure:"events_per_second"`
	Burst           int     `mapstructure:"burst"`
}

type EncodingConfig struct {
	SchemaRegistryURL      string `mapstructure:"schema_registry_url"`
	SchemaRegistryUsername string `mapstructure:"schema_registry_username"`
	SchemaRegistryPassword string `mapstructure:"schema_registry_password"`
}

// knownAdapters is the set of valid adapter names.
var knownAdapters = map[string]bool{
	"stdout": true, "webhook": true, "sse": true, "file": true,
	"exec": true, "pg_table": true, "ws": true, "embedding": true,
	"iceberg": true, "nats": true, "search": true, "redis": true,
	"kafka": true, "kafkaserver": true, "s3": true, "grpc": true,
	"view": true,
}

// Validate performs structural validation on the config.
func (c Config) Validate() error {
	var errs []string

	// --- Top-level ---
	if c.DatabaseURL == "" && c.Detector.Type != "mysql" && c.Detector.Type != "mongodb" {
		errs = append(errs, "database_url is required")
	}
	if c.Bus.BufferSize <= 0 {
		errs = append(errs, fmt.Sprintf("bus.buffer_size must be > 0, got %d", c.Bus.BufferSize))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, "shutdown_timeout must be > 0")
	}
	if c.Detector.Type == "listen_notify" && len(c.Channels) == 0 {
		errs = append(errs, "no channels specified for listen_notify detector")
	}

	adapterSet := make(map[string]bool, len(c.Adapters))
	for _, a := range c.Adapters {
		if !knownAdapters[a] {
			errs = append(errs, fmt.Sprintf("unknown adapter %q", a))
		}
		adapterSet[a] = true
	}
	for name := range c.Routes {
		if !adapterSet[name] {
			errs = append(errs, fmt.Sprintf("route references unknown adapter %q", name))
		}
	}

	// --- Helper for duration checks ---
	checkDur := func(path string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be > 0", path))
		}
	}

	// --- Detector durations (always validated) ---
	checkDur("detector.backoff_base", c.Detector.BackoffBase)
	checkDur("detector.backoff_cap", c.Detector.BackoffCap)

	// --- Adapter-specific: required fields + durations ---
	if adapterSet["webhook"] {
		if c.Webhook.URL == "" {
			errs = append(errs, "webhook.url is required")
		}
		checkDur("webhook.timeout", c.Webhook.Timeout)
		checkDur("webhook.backoff_base", c.Webhook.BackoffBase)
		checkDur("webhook.backoff_cap", c.Webhook.BackoffCap)
	}

	if adapterSet["sse"] {
		checkDur("sse.heartbeat_interval", c.SSE.HeartbeatInterval)
		checkDur("sse.read_timeout", c.SSE.ReadTimeout)
		checkDur("sse.idle_timeout", c.SSE.IdleTimeout)
	}

	if adapterSet["file"] {
		if c.File.Path == "" {
			errs = append(errs, "file.path is required")
		}
	}

	if adapterSet["exec"] {
		if c.Exec.Command == "" {
			errs = append(errs, "exec.command is required")
		}
		checkDur("exec.backoff_base", c.Exec.BackoffBase)
		checkDur("exec.backoff_cap", c.Exec.BackoffCap)
	}

	if adapterSet["pg_table"] {
		if c.PGTable.URL == "" && c.DatabaseURL == "" {
			errs = append(errs, "pg_table requires a database URL (pg_table.url or database_url)")
		}
		checkDur("pg_table.backoff_base", c.PGTable.BackoffBase)
		checkDur("pg_table.backoff_cap", c.PGTable.BackoffCap)
	}

	if adapterSet["ws"] {
		checkDur("websocket.ping_interval", c.WebSocket.PingInterval)
	}

	if adapterSet["embedding"] {
		if c.Embedding.APIURL == "" {
			errs = append(errs, "embedding.api_url is required")
		}
		if len(c.Embedding.Columns) == 0 {
			errs = append(errs, "embedding.columns must not be empty")
		}
		checkDur("embedding.timeout", c.Embedding.Timeout)
		checkDur("embedding.backoff_base", c.Embedding.BackoffBase)
		checkDur("embedding.backoff_cap", c.Embedding.BackoffCap)
	}

	if adapterSet["nats"] {
		if c.Nats.URL == "" {
			errs = append(errs, "nats.url is required")
		}
		checkDur("nats.backoff_base", c.Nats.BackoffBase)
		checkDur("nats.backoff_cap", c.Nats.BackoffCap)
	}

	if adapterSet["kafka"] {
		if len(c.Kafka.Brokers) == 0 {
			errs = append(errs, "kafka.brokers must not be empty")
		}
		checkDur("kafka.backoff_base", c.Kafka.BackoffBase)
		checkDur("kafka.backoff_cap", c.Kafka.BackoffCap)
	}

	if adapterSet["search"] {
		if c.Search.URL == "" {
			errs = append(errs, "search.url is required")
		}
		if c.Search.Index == "" {
			errs = append(errs, "search.index is required")
		}
		if c.Search.BatchSize <= 0 {
			errs = append(errs, fmt.Sprintf("search.batch_size must be > 0, got %d", c.Search.BatchSize))
		}
		checkDur("search.batch_interval", c.Search.BatchInterval)
		checkDur("search.backoff_base", c.Search.BackoffBase)
		checkDur("search.backoff_cap", c.Search.BackoffCap)
	}

	if adapterSet["redis"] {
		if c.Redis.URL == "" {
			errs = append(errs, "redis.url is required")
		}
		checkDur("redis.backoff_base", c.Redis.BackoffBase)
		checkDur("redis.backoff_cap", c.Redis.BackoffCap)
	}

	if adapterSet["s3"] {
		if c.S3.Bucket == "" {
			errs = append(errs, "s3.bucket is required")
		}
		if c.S3.FlushSize <= 0 {
			errs = append(errs, fmt.Sprintf("s3.flush_size must be > 0, got %d", c.S3.FlushSize))
		}
		checkDur("s3.flush_interval", c.S3.FlushInterval)
		checkDur("s3.drain_timeout", c.S3.DrainTimeout)
		checkDur("s3.backoff_base", c.S3.BackoffBase)
		checkDur("s3.backoff_cap", c.S3.BackoffCap)
	}

	if adapterSet["iceberg"] {
		if c.Iceberg.Warehouse == "" {
			errs = append(errs, "iceberg.warehouse is required")
		}
		if c.Iceberg.Table == "" {
			errs = append(errs, "iceberg.table is required")
		}
		if c.Iceberg.Mode == "upsert" && len(c.Iceberg.PrimaryKeys) == 0 {
			errs = append(errs, "iceberg upsert mode requires primary_keys")
		}
		checkDur("iceberg.flush_interval", c.Iceberg.FlushInterval)
		checkDur("iceberg.backoff_base", c.Iceberg.BackoffBase)
		checkDur("iceberg.backoff_cap", c.Iceberg.BackoffCap)
	}

	if adapterSet["kafkaserver"] {
		if c.KafkaServer.PartitionCount <= 0 {
			errs = append(errs, fmt.Sprintf("kafkaserver.partition_count must be > 0, got %d", c.KafkaServer.PartitionCount))
		}
		if c.KafkaServer.BufferSize <= 0 {
			errs = append(errs, fmt.Sprintf("kafkaserver.buffer_size must be > 0, got %d", c.KafkaServer.BufferSize))
		}
		checkDur("kafkaserver.session_timeout", c.KafkaServer.SessionTimeout)
	}

	// --- Outbox detector durations ---
	if c.Detector.Type == "outbox" {
		checkDur("outbox.poll_interval", c.Outbox.PollInterval)
		checkDur("outbox.backoff_base", c.Outbox.BackoffBase)
		checkDur("outbox.backoff_cap", c.Outbox.BackoffCap)
	}

	// --- MySQL detector ---
	if c.Detector.Type == "mysql" {
		if c.MySQL.Addr == "" {
			errs = append(errs, "mysql.addr is required")
		}
		if c.MySQL.ServerID == 0 {
			errs = append(errs, "mysql.server_id must be > 0")
		}
		checkDur("mysql.backoff_base", c.MySQL.BackoffBase)
		checkDur("mysql.backoff_cap", c.MySQL.BackoffCap)
	}

	// --- MongoDB detector ---
	if c.Detector.Type == "mongodb" {
		if c.MongoDB.URI == "" {
			errs = append(errs, "mongodb.uri is required")
		}
		if c.MongoDB.Scope != "cluster" && c.MongoDB.Database == "" {
			errs = append(errs, "mongodb.database is required (unless scope is \"cluster\")")
		}
		checkDur("mongodb.backoff_base", c.MongoDB.BackoffBase)
		checkDur("mongodb.backoff_cap", c.MongoDB.BackoffCap)
	}

	// --- Backpressure (when enabled) ---
	if c.Backpressure.Enabled {
		checkDur("backpressure.max_throttle", c.Backpressure.MaxThrottle)
		checkDur("backpressure.poll_interval", c.Backpressure.PollInterval)
		if c.Backpressure.WarnThreshold <= 0 {
			errs = append(errs, "backpressure.warn_threshold must be > 0")
		}
		if c.Backpressure.CriticalThreshold <= 0 {
			errs = append(errs, "backpressure.critical_threshold must be > 0")
		}
		if c.Backpressure.CriticalThreshold > 0 && c.Backpressure.WarnThreshold > 0 &&
			c.Backpressure.CriticalThreshold <= c.Backpressure.WarnThreshold {
			errs = append(errs, fmt.Sprintf(
				"backpressure.critical_threshold (%d) must be > warn_threshold (%d)",
				c.Backpressure.CriticalThreshold, c.Backpressure.WarnThreshold))
		}
	}

	// --- WAL publication ---
	if c.Detector.Type == "wal" && c.Detector.Publication == "" {
		errs = append(errs, "WAL detector requires a publication")
	}

	// --- Snapshot-first requires snapshot table ---
	if c.Detector.SnapshotFirst && c.Snapshot.Table == "" {
		errs = append(errs, "snapshot-first requires a snapshot table")
	}

	// --- Detector-feature compatibility ---
	if c.Detector.Type != "wal" {
		walOnly := []struct {
			name    string
			enabled bool
		}{
			{"tx-metadata", c.Detector.TxMetadata},
			{"tx-markers", c.Detector.TxMarkers},
			{"include-schema", c.Detector.IncludeSchema},
			{"schema-events", c.Detector.SchemaEvents},
			{"toast-cache", c.Detector.ToastCache},
			{"snapshot-first", c.Detector.SnapshotFirst},
			{"persistent-slot", c.Detector.PersistentSlot},
			{"incremental-snapshot", c.Detector.IncrementalSnapshot},
		}
		for _, f := range walOnly {
			if f.enabled {
				errs = append(errs, fmt.Sprintf("%s requires --detector wal", f.name))
			}
		}
	}
	if c.Detector.CooperativeCheckpoint && !c.Detector.PersistentSlot {
		errs = append(errs, "cooperative-checkpoint requires --persistent-slot")
	}
	if c.Detector.CooperativeCheckpoint && c.Detector.Type != "wal" {
		errs = append(errs, "cooperative-checkpoint requires --detector wal")
	}
	if c.Detector.BackpressureEnabled && !c.Detector.PersistentSlot {
		errs = append(errs, "backpressure requires --persistent-slot")
	}
	if c.Detector.BackpressureEnabled && c.Detector.Type != "wal" {
		errs = append(errs, "backpressure requires --detector wal")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation: %s", strings.Join(errs, "; "))
	}
	return nil
}

func Default() Config {
	return Config{
		Channels:        []string{},
		Adapters:        []string{"stdout"},
		LogLevel:        "info",
		LogFormat:       "text",
		ShutdownTimeout: 5 * time.Second,
		Bus: BusConfig{
			BufferSize: 1024,
		},
		Webhook: WebhookConfig{
			MaxRetries:  5,
			Headers:     map[string]string{},
			Timeout:     10 * time.Second,
			BackoffBase: 1 * time.Second,
			BackoffCap:  32 * time.Second,
		},
		SSE: SSEConfig{
			Addr:              ":8080",
			CORSOrigins:       []string{"*"},
			HeartbeatInterval: 15 * time.Second,
			ReadTimeout:       5 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
		File: FileConfig{
			MaxSize:  100 * 1024 * 1024, // 100 MB
			MaxFiles: 5,
		},
		Exec: ExecConfig{
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		PGTable: PGTableConfig{
			Table:       "pgcdc_events",
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		WebSocket: WebSocketConfig{
			PingInterval: 15 * time.Second,
		},
		Snapshot: SnapshotConfig{
			BatchSize: 1000,
		},
		Detector: DetectorConfig{
			Type:              "listen_notify",
			BackoffBase:       5 * time.Second,
			BackoffCap:        60 * time.Second,
			HeartbeatInterval: 30 * time.Second,
			HeartbeatTable:    "pgcdc_heartbeat",
			SlotLagWarn:       100 * 1024 * 1024, // 100 MB
		},
		Iceberg: IcebergConfig{
			CatalogType:   "hadoop",
			Namespace:     "pgcdc",
			Mode:          "append",
			SchemaMode:    "raw",
			FlushInterval: 1 * time.Minute,
			FlushSize:     10000,
			BackoffBase:   5 * time.Second,
			BackoffCap:    60 * time.Second,
		},
		Nats: NatsConfig{
			Subject:     "pgcdc",
			Stream:      "pgcdc",
			MaxAge:      24 * time.Hour,
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		Outbox: OutboxConfig{
			Table:        "pgcdc_outbox",
			PollInterval: 500 * time.Millisecond,
			BatchSize:    100,
			BackoffBase:  1 * time.Second,
			BackoffCap:   30 * time.Second,
		},
		DLQ: DLQConfig{
			Type:  "stderr",
			Table: "pgcdc_dead_letters",
		},
		Search: SearchConfig{
			Engine:        "typesense",
			IDColumn:      "id",
			BatchSize:     100,
			BatchInterval: 1 * time.Second,
			BackoffBase:   1 * time.Second,
			BackoffCap:    30 * time.Second,
		},
		Redis: RedisConfig{
			Mode:        "invalidate",
			IDColumn:    "id",
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		GRPC: GRPCConfig{
			Addr: ":9090",
		},
		S3: S3Config{
			Region:        "us-east-1",
			Format:        "jsonl",
			FlushInterval: 1 * time.Minute,
			FlushSize:     10000,
			DrainTimeout:  30 * time.Second,
			BackoffBase:   5 * time.Second,
			BackoffCap:    60 * time.Second,
		},
		Kafka: KafkaConfig{
			Brokers:     []string{"localhost:9092"},
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		IncrementalSnapshot: IncrementalSnapshotConfig{
			SignalTable: "pgcdc_signals",
			ChunkSize:   1000,
		},
		Backpressure: BackpressureConfig{
			WarnThreshold:     500 * 1024 * 1024,      // 500 MB
			CriticalThreshold: 2 * 1024 * 1024 * 1024, // 2 GB
			MaxThrottle:       500 * time.Millisecond,
			PollInterval:      10 * time.Second,
		},
		MySQL: MySQLConfig{
			Flavor:       "mysql",
			BinlogPrefix: "mysql-bin",
			BackoffBase:  5 * time.Second,
			BackoffCap:   60 * time.Second,
		},
		MongoDB: MongoDBConfig{
			Scope:        "collection",
			FullDocument: "updateLookup",
			MetadataColl: "pgcdc_resume_tokens",
			BackoffBase:  5 * time.Second,
			BackoffCap:   60 * time.Second,
		},
		OTel: OTelConfig{
			Exporter:    "none",
			SampleRatio: 1.0,
		},
		KafkaServer: KafkaServerConfig{
			Addr:           ":9092",
			PartitionCount: 8,
			BufferSize:     10000,
			SessionTimeout: 30 * time.Second,
			KeyColumn:      "id",
		},
		Embedding: EmbeddingConfig{
			Model:       "text-embedding-3-small",
			IDColumn:    "id",
			Table:       "pgcdc_embeddings",
			Dimension:   1536,
			MaxRetries:  3,
			Timeout:     30 * time.Second,
			BackoffBase: 2 * time.Second,
			BackoffCap:  60 * time.Second,
		},
	}
}
