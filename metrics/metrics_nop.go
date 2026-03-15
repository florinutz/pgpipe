//go:build no_metrics

package metrics

import "github.com/prometheus/client_golang/prometheus"

// nopCounter returns an unregistered Counter.
func nopCounter(opts prometheus.CounterOpts) prometheus.Counter {
	return prometheus.NewCounter(opts)
}

// nopCounterVec returns an unregistered CounterVec.
func nopCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(opts, labels)
}

// nopGauge returns an unregistered Gauge.
func nopGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	return prometheus.NewGauge(opts)
}

// nopGaugeVec returns an unregistered GaugeVec.
func nopGaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(opts, labels)
}

// nopHistogram returns an unregistered Histogram.
func nopHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	return prometheus.NewHistogram(opts)
}

// nopHistogramVec returns an unregistered HistogramVec.
func nopHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(opts, labels)
}

var (
	EventsReceived = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_events_received_total",
		Help: "Total number of events received by the bus.",
	})

	EventsDelivered = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_events_delivered_total",
		Help: "Total number of events delivered by adapters.",
	}, []string{"adapter"})

	EventsDropped = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_events_dropped_total",
		Help: "Total number of events dropped due to full subscriber channels.",
	}, []string{"adapter"})

	WebhookRetries = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_webhook_retries_total",
		Help: "Total number of webhook delivery retries.",
	})

	WebhookDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_webhook_duration_seconds",
		Help: "Duration of webhook HTTP requests.",
	})

	SSEClientsActive = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_sse_clients_active",
		Help: "Number of active SSE client connections.",
	}, []string{"channel"})

	BusSubscribers = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_bus_subscribers",
		Help: "Number of active bus subscribers.",
	})

	FileRotations = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_file_rotations_total",
		Help: "Total number of file rotations.",
	})

	ExecRestarts = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_exec_restarts_total",
		Help: "Total number of exec subprocess restarts.",
	})

	WSClientsActive = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_ws_clients_active",
		Help: "Number of active WebSocket client connections.",
	}, []string{"channel"})

	SnapshotRowsExported = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_snapshot_rows_exported_total",
		Help: "Total number of rows exported during snapshots.",
	})

	SnapshotDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_snapshot_duration_seconds",
		Help: "Duration of snapshot operations.",
	})

	EmbeddingAPIRequests = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_api_requests_total",
		Help: "Total number of embedding API requests.",
	})

	EmbeddingAPIDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_embedding_api_duration_seconds",
		Help: "Duration of embedding API requests.",
	})

	EmbeddingAPIErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_api_errors_total",
		Help: "Total number of embedding API errors after retries exhausted.",
	})

	EmbeddingTokensUsed = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_tokens_total",
		Help: "Total number of tokens consumed by the embedding API.",
	})

	EmbeddingSkipped = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_skipped_total",
		Help: "Total number of embedding operations skipped due to unchanged columns.",
	})

	IcebergFlushes = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_iceberg_flushes_total",
		Help: "Total number of Iceberg flush operations.",
	}, []string{"mode"})

	IcebergFlushSize = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_iceberg_flush_size",
		Help: "Number of events per Iceberg flush.",
	})

	IcebergFlushDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_iceberg_flush_duration_seconds",
		Help: "Duration of Iceberg flush operations.",
	})

	IcebergBufferSize = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_iceberg_buffer_size",
		Help: "Current number of events buffered for Iceberg flush.",
	})

	IcebergDataFilesWritten = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_iceberg_data_files_written_total",
		Help: "Total number of Parquet data files written.",
	})

	IcebergBytesWritten = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_iceberg_bytes_written_total",
		Help: "Total bytes written to Iceberg storage.",
	})

	S3Flushes = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_s3_flushes_total",
		Help: "Total number of S3 flush operations.",
	}, []string{"status"})

	S3FlushSize = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_s3_flush_size",
		Help: "Number of events per S3 flush.",
	})

	S3FlushDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_s3_flush_duration_seconds",
		Help: "Duration of S3 flush operations.",
	})

	S3BufferSize = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_s3_buffer_size",
		Help: "Current number of events buffered for S3 flush.",
	})

	S3ObjectsUploaded = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_s3_objects_uploaded_total",
		Help: "Total number of objects uploaded to S3.",
	})

	S3BytesUploaded = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_s3_bytes_uploaded_total",
		Help: "Total bytes uploaded to S3.",
	})

	IncrementalSnapshotRowsExported = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_incremental_snapshot_rows_exported_total",
		Help: "Total number of rows exported during incremental snapshots.",
	})

	IncrementalSnapshotChunks = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_incremental_snapshot_chunks_total",
		Help: "Total number of chunks read during incremental snapshots.",
	})

	IncrementalSnapshotDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_incremental_snapshot_duration_seconds",
		Help: "Duration of incremental snapshot operations.",
	})

	IncrementalSnapshotsActive = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_incremental_snapshots_active",
		Help: "Number of currently active incremental snapshots.",
	})

	CheckpointLSN = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_checkpoint_lsn",
		Help: "Last persisted LSN value.",
	})

	CheckpointLagBytes = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_checkpoint_lag_bytes",
		Help: "WAL bytes between current position and last checkpoint.",
	})

	CheckpointTotal = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_checkpoint_total",
		Help: "Total number of checkpoints written.",
	})

	SlotLagBytes = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_slot_lag_bytes",
		Help: "Bytes of WAL retained by the replication slot.",
	})

	NatsPublished = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_nats_published_total",
		Help: "Total number of events published to NATS.",
	})

	NatsPublishDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_nats_publish_duration_seconds",
		Help: "Duration of NATS publish operations.",
	})

	NatsErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_nats_errors_total",
		Help: "Total number of NATS publish errors.",
	})

	NatsConsumerReceived = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_nats_consumer_received_total",
		Help: "Total number of events received from NATS consumer.",
	})

	NatsConsumerErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_nats_consumer_errors_total",
		Help: "Total number of NATS consumer errors.",
	})

	KafkaPublished = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_published_total",
		Help: "Total number of events published to Kafka.",
	})

	KafkaPublishDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_kafka_publish_duration_seconds",
		Help: "Duration of Kafka publish operations.",
	})

	KafkaErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_errors_total",
		Help: "Total number of Kafka publish errors.",
	})

	KafkaTransactions = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_transactions_total",
		Help: "Total number of Kafka transactions committed.",
	})

	KafkaTransactionErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_transaction_errors_total",
		Help: "Total number of Kafka transaction errors (aborts).",
	})

	KafkaConsumerReceived = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_consumer_received_total",
		Help: "Total number of events received from Kafka consumer.",
	})

	KafkaConsumerErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_consumer_errors_total",
		Help: "Total number of Kafka consumer errors.",
	})

	ListenNotifyErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_listennotify_errors_total",
		Help: "Total number of LISTEN/NOTIFY detector reconnect errors.",
	})

	WalReplicationErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_walreplication_errors_total",
		Help: "Total number of WAL replication detector reconnect errors.",
	})

	OutboxPolled = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_outbox_polled_total",
		Help: "Total number of outbox poll cycles.",
	})

	OutboxEventsProcessed = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_outbox_events_processed_total",
		Help: "Total number of outbox events processed.",
	})

	OutboxErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_outbox_errors_total",
		Help: "Total number of outbox processing errors.",
	})

	DLQRecords = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_dlq_records_total",
		Help: "Total number of events recorded to the dead letter queue.",
	}, []string{"adapter"})

	DLQErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_dlq_errors_total",
		Help: "Total number of errors writing to the dead letter queue.",
	})

	SearchUpserted = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_search_upserted_total",
		Help: "Total number of documents upserted to search engine.",
	})

	SearchDeleted = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_search_deleted_total",
		Help: "Total number of documents deleted from search engine.",
	})

	SearchBatchDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_search_batch_duration_seconds",
		Help: "Duration of search batch operations.",
	})

	SearchErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_search_errors_total",
		Help: "Total number of search engine errors.",
	})

	RedisOperations = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_redis_operations_total",
		Help: "Total number of Redis operations.",
	}, []string{"operation"})

	RedisErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_redis_errors_total",
		Help: "Total number of Redis errors.",
	})

	GRPCActiveClients = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_grpc_active_clients",
		Help: "Number of active gRPC streaming clients.",
	})

	GRPCEventsSent = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_grpc_events_sent_total",
		Help: "Total number of events sent to gRPC clients.",
	})

	BusBackpressure = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_bus_backpressure_total",
		Help: "Events that required blocking send in reliable bus mode.",
	}, []string{"adapter"})

	AckPosition = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_ack_position",
		Help: "Highest acknowledged LSN per adapter.",
	}, []string{"adapter"})

	CooperativeCheckpointLSN = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_cooperative_checkpoint_lsn",
		Help: "Cooperative checkpoint LSN (minimum across all adapters).",
	})

	TransformDropped = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_transform_dropped_total",
		Help: "Total number of events dropped by transforms.",
	}, []string{"adapter"})

	TransformErrors = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_transform_errors_total",
		Help: "Total number of transform errors.",
	}, []string{"adapter"})

	PluginCalls = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_plugin_calls_total",
		Help: "Total Wasm plugin invocations.",
	}, []string{"plugin", "type"})

	PluginDuration = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_plugin_duration_seconds",
		Help: "Wasm plugin call duration.",
	}, []string{"plugin", "type"})

	PluginErrors = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_plugin_errors_total",
		Help: "Total Wasm plugin errors.",
	}, []string{"plugin", "type"})

	EncodingEncoded = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_encoding_encoded_total",
		Help: "Total number of events encoded (Avro/Protobuf).",
	})

	EncodingErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_encoding_errors_total",
		Help: "Total number of encoding errors.",
	})

	SchemaRegistryRegistrations = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_schema_registry_registrations_total",
		Help: "Total number of schema registrations with Schema Registry.",
	})

	SchemaRegistryErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_schema_registry_errors_total",
		Help: "Total number of Schema Registry errors.",
	})

	BackpressureState = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_backpressure_state",
		Help: "Current backpressure zone: 0=green, 1=yellow, 2=red.",
	})

	BackpressureThrottleDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_backpressure_throttle_duration_seconds",
		Help: "Observed throttle sleep durations between WAL reads.",
	})

	BackpressureLoadShed = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_backpressure_load_shed_total",
		Help: "Events auto-acked due to backpressure load shedding.",
	}, []string{"adapter"})

	MySQLEventsReceived = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_mysql_events_received_total",
		Help: "Total number of events received from MySQL binlog.",
	})

	MySQLErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_mysql_errors_total",
		Help: "Total number of MySQL binlog replication errors.",
	})

	MongoDBEventsReceived = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_mongodb_events_received_total",
		Help: "Total number of events received from MongoDB change streams.",
	})

	MongoDBErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_mongodb_errors_total",
		Help: "Total number of MongoDB change stream errors.",
	})

	MongoDBResumeTokenSaves = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_mongodb_resume_token_saves_total",
		Help: "Total number of resume token saves to MongoDB.",
	})

	SQLitePolled = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_sqlite_polled_total",
		Help: "Total number of SQLite poll cycles with results.",
	})

	SQLiteEventsProcessed = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_sqlite_events_processed_total",
		Help: "Total number of events processed from SQLite changes table.",
	})

	SQLiteErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_sqlite_errors_total",
		Help: "Total number of SQLite detector errors.",
	})

	KafkaServerConnectionsActive = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_kafkaserver_connections_active",
		Help: "Number of active Kafka protocol server connections.",
	})

	KafkaServerFetchRequests = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_fetch_requests_total",
		Help: "Total number of Kafka Fetch requests received.",
	})

	KafkaServerRecordsFetched = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_records_fetched_total",
		Help: "Total number of records returned in Kafka Fetch responses.",
	})

	KafkaServerGroupRebalances = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_group_rebalances_total",
		Help: "Total number of consumer group rebalances.",
	})

	KafkaServerOffsetCommits = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_offset_commits_total",
		Help: "Total number of offset commits.",
	})

	KafkaServerProtocolErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_protocol_errors_total",
		Help: "Total number of Kafka protocol errors.",
	})

	KafkaServerBufferUsage = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_kafkaserver_buffer_usage",
		Help: "Current buffer usage per topic/partition.",
	}, []string{"topic", "partition"})

	ViewWindowsEmitted = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_windows_emitted_total",
		Help: "Total number of view result events emitted.",
	}, []string{"view"})

	ViewEventsProcessed = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_events_processed_total",
		Help: "Total number of events processed by views.",
	}, []string{"view"})

	ViewWindowDuration = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_view_window_duration_seconds",
		Help: "Duration of view window flush operations.",
	}, []string{"view"})

	ViewGroups = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_view_groups",
		Help: "Number of groups in the last window flush.",
	}, []string{"view"})

	ViewTypeErrors = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_type_errors_total",
		Help: "Total number of type coercion errors in view aggregation.",
	}, []string{"view"})

	ViewGroupsOverflow = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_groups_overflow_total",
		Help: "Total events dropped due to group cardinality exceeding max_groups.",
	}, []string{"view"})

	MiddlewareRetries = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_middleware_retries_total",
		Help: "Total number of delivery retries across all middleware-wrapped adapters.",
	})

	MiddlewareDeliveryDuration = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_middleware_delivery_duration_seconds",
		Help: "Duration of event delivery through the middleware chain.",
	}, []string{"adapter"})

	BatchFlushes = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_batch_flushes_total",
		Help: "Total number of batch flush operations.",
	}, []string{"adapter", "status"})

	BatchFlushSize = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_batch_flush_size",
		Help: "Number of events per batch flush.",
	}, []string{"adapter"})

	BatchFlushDuration = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_batch_flush_duration_seconds",
		Help: "Duration of batch flush operations.",
	}, []string{"adapter"})

	BatchBufferSize = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_batch_buffer_size",
		Help: "Current number of events buffered for batch flush.",
	}, []string{"adapter"})

	ConfigReloads = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_config_reloads_total",
		Help: "Total number of successful config reloads via SIGHUP.",
	})

	ConfigReloadErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_config_reload_errors_total",
		Help: "Total number of failed config reload attempts.",
	})

	PanicsRecovered = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_panics_recovered_total",
		Help: "Total number of panics recovered by safe goroutine wrapper.",
	}, []string{"component"})

	ChainEventsProcessed = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_chain_events_processed_total",
		Help: "Total number of events successfully processed through adapter chain links.",
	}, []string{"chain"})

	ChainLinkErrors = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_chain_link_errors_total",
		Help: "Total number of events skipped due to chain link errors.",
	}, []string{"chain"})

	AdapterPaused = nopGaugeVec(prometheus.GaugeOpts{
		Namespace: "pgcdc",
		Name:      "adapter_paused",
		Help:      "Whether an adapter is paused due to nack threshold (1=paused, 0=running).",
	}, []string{"adapter"})

	NackWindowExceeded = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_nack_window_exceeded_total",
		Help: "Total number of events skipped due to nack window threshold exceeded.",
	}, []string{"adapter"})

	WebhookGatewayReceived = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_webhook_gateway_received_total",
		Help: "Total number of webhook events received by source.",
	}, []string{"source"})

	WebhookGatewaySignatureFailures = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_webhook_gateway_signature_failures_total",
		Help: "Total number of webhook signature validation failures.",
	}, []string{"source"})

	WebhookGatewayErrors = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_webhook_gateway_errors_total",
		Help: "Total number of webhook gateway processing errors.",
	}, []string{"source"})

	CircuitBreakerState = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_circuit_breaker_state",
		Help: "Current circuit breaker state: 0=closed, 1=open, 2=half_open.",
	}, []string{"adapter"})

	CircuitBreakerTrips = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_circuit_breaker_trips_total",
		Help: "Total number of circuit breaker trips to open state.",
	}, []string{"adapter"})

	RateLimitWaits = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_rate_limit_waits_total",
		Help: "Total number of rate limit waits.",
	}, []string{"adapter"})

	RateLimitWaitDuration = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_rate_limit_wait_duration_seconds",
		Help: "Duration of rate limit waits.",
	}, []string{"adapter"})

	ValidationDuration = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_validation_duration_seconds",
		Help: "Duration of adapter startup validation.",
	}, []string{"adapter"})

	GraphQLSubscriptionsActive = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_graphql_subscriptions_active",
		Help: "Number of active GraphQL subscriptions.",
	})

	GraphQLEventsSent = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_graphql_events_sent_total",
		Help: "Total number of events sent to GraphQL subscribers.",
	})

	GraphQLClientsActive = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_graphql_clients_active",
		Help: "Number of active GraphQL WebSocket connections.",
	})

	ArrowFlightClients = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_arrow_flight_clients_active",
		Help: "Number of active Arrow Flight DoGet streams.",
	})

	ArrowFlightRecordsSent = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_arrow_flight_records_sent_total",
		Help: "Total number of Arrow records sent to Flight clients.",
	})

	ArrowFlightErrors = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_arrow_flight_errors_total",
		Help: "Total number of Arrow Flight errors.",
	})

	DuckDBQueries = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_duckdb_queries_total",
		Help: "Total number of DuckDB queries executed.",
	}, []string{"status"})

	DuckDBQueryDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_duckdb_query_duration_seconds",
		Help: "Duration of DuckDB query executions.",
	})

	DuckDBRows = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_duckdb_rows_total",
		Help: "Approximate number of rows in DuckDB cdc_events table.",
	})

	DuckDBFlushes = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_duckdb_flushes_total",
		Help: "Total number of DuckDB buffer flush operations.",
	}, []string{"status"})

	ToastCacheHits = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_toast_cache_hits_total",
		Help: "Total number of TOAST cache hits (unchanged columns resolved from cache).",
	})

	ToastCacheMisses = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_toast_cache_misses_total",
		Help: "Total number of TOAST cache misses (unchanged columns emitted as metadata).",
	})

	ToastCacheEvictions = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_toast_cache_evictions_total",
		Help: "Total number of TOAST cache LRU evictions.",
	})

	ToastCacheEntries = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_toast_cache_entries",
		Help: "Current number of entries in the TOAST cache.",
	})

	EventDeliveryLag = nopHistogramVec(prometheus.HistogramOpts{
		Name: "pgcdc_event_delivery_lag_seconds",
		Help: "Time from event creation to successful delivery.",
	}, []string{"adapter"})

	DLQDepth = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_dlq_depth",
		Help: "Current number of unprocessed DLQ records (PG-backed DLQ only).",
	})

	AdapterQueueDepth = nopGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_adapter_queue_depth",
		Help: "Current number of events in an adapter's subscriber channel.",
	}, []string{"adapter"})

	BatchEventsLost = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_batch_events_lost_total",
		Help: "Total events lost due to fatal batch flush error with no DLQ configured.",
	}, []string{"adapter"})

	ClickHouseFlushes = nopCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_clickhouse_flushes_total",
		Help: "Total number of ClickHouse flush operations.",
	}, []string{"status"})

	ClickHouseFlushSize = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_clickhouse_flush_size",
		Help: "Number of events per ClickHouse flush.",
	})

	ClickHouseFlushDuration = nopHistogram(prometheus.HistogramOpts{
		Name: "pgcdc_clickhouse_flush_duration_seconds",
		Help: "Duration of ClickHouse flush operations.",
	})

	ClickHouseRows = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_clickhouse_rows_inserted_total",
		Help: "Total number of rows inserted into ClickHouse.",
	})

	ClickHouseDedupSkipped = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_clickhouse_dedup_skipped_total",
		Help: "Total number of duplicate events skipped within a ClickHouse batch flush.",
	})

	DedupDropped = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_dedup_dropped_total",
		Help: "Total number of events dropped by dedup transform.",
	})

	DedupCacheSize = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_dedup_cache_size",
		Help: "Current number of entries in the dedup LRU cache.",
	})

	// PGWire adapter metrics.

	PGWireQueriesTotal = nopCounter(prometheus.CounterOpts{
		Name: "pgcdc_pgwire_queries_total",
		Help: "Total number of queries received by the PGWire adapter.",
	})

	PGWireClientsActive = nopGauge(prometheus.GaugeOpts{
		Name: "pgcdc_pgwire_clients_active",
		Help: "Number of active PGWire client connections.",
	})
)
