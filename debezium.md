# Debezium vs pgcdc: Complete Feature Analysis

Comprehensive comparison based on Debezium 3.4 (Dec 2025) documentation vs pgcdc current main branch.

---

## 1. Scope and Philosophy

| Dimension | Debezium | pgcdc |
|-----------|----------|-------|
| **Focus** | Multi-database CDC platform | PostgreSQL-only CDC |
| **Databases** | PostgreSQL, MySQL, MariaDB, MongoDB, SQL Server, Oracle, Db2, Cassandra, Vitess, Spanner, Informix | PostgreSQL only |
| **Language** | Java (JVM) | Go |
| **Deployment** | Kafka Connect cluster, Debezium Server (standalone), embedded JVM library | Single binary, Go library (`pgcdc.Pipeline`) |
| **External deps** | Kafka + ZooKeeper/KRaft (primary), or standalone server | None (single binary, zero infra) |
| **Binary size** | ~200MB+ (JVM + dependencies) | ~25MB Go binary |
| **Memory** | 512MB-2GB+ heap typical | 50-100MB typical |

**Key insight**: Debezium's multi-database support is its defining advantage. pgcdc's single-binary zero-infra model is its defining advantage. These are fundamentally different trade-offs.

---

## 2. Source Detection / Connectors

### Debezium source connectors (11)

| Connector | Status | Log mechanism |
|-----------|--------|---------------|
| PostgreSQL | Stable | WAL logical decoding (pgoutput, decoderbufs) |
| MySQL | Stable | Binlog |
| MariaDB | Stable | Binlog |
| MongoDB | Stable | Change streams |
| SQL Server | Stable | Transaction log (CT) |
| Oracle | Stable | Redo log mining (LogMiner, XStream) |
| Db2 | Stable | SQL Change Data Capture |
| Cassandra | Incubating | Commit log |
| Vitess | Incubating | VStream |
| Spanner | Incubating | Change streams |
| Informix | Incubating | CDC API |

### pgcdc detectors (3, PostgreSQL only)

| Detector | Mechanism |
|----------|-----------|
| `walreplication` | WAL logical decoding (pgoutput) |
| `listennotify` | PostgreSQL LISTEN/NOTIFY |
| `outbox` | Transactional outbox polling (SELECT FOR UPDATE SKIP LOCKED) |

**Gap**: pgcdc is PG-only by design. The LISTEN/NOTIFY and outbox detectors have no Debezium equivalent — Debezium only does WAL-based CDC for PostgreSQL. This is a pgcdc advantage for lightweight notification workflows and the outbox pattern without WAL setup.

---

## 3. Sink / Adapter Destinations

### Debezium sinks

**Via Kafka Connect sink connectors:**
- Any Kafka Connect sink (Elasticsearch, JDBC, S3, BigQuery, Snowflake, etc. — thousands available from Confluent Hub and community)

**Via Debezium Server (standalone, no Kafka):**
- Apache Kafka
- Amazon Kinesis
- Google Cloud Pub/Sub
- Google Cloud Pub/Sub Lite
- Apache Pulsar
- Azure Event Hubs
- Redis Streams
- NATS Streaming / NATS JetStream
- Apache RocketMQ
- RabbitMQ Stream
- Pravega
- Infinispan
- HTTP Client (webhooks)
- Milvus (vector DB)
- Qdrant (vector DB)
- Apache Iceberg
- InstructLab

**Debezium native sink connectors (Kafka Connect):**
- JDBC (any relational DB)
- MongoDB

### pgcdc adapters (16 built-in + Wasm custom)

| Adapter | Category |
|---------|----------|
| stdout | Debug/dev |
| file | Log archive (JSON-lines, rotation) |
| exec | Subprocess stdin |
| webhook | HTTP POST with retries |
| SSE | Server-Sent Events (browser) |
| WebSocket | Real-time browser/client |
| pg_table | INSERT into PostgreSQL table |
| Kafka | Topic publish (SASL/TLS, exactly-once txn) |
| NATS JetStream | Subject publish with dedup |
| Redis | Cache invalidation or sync (SET/DEL) |
| gRPC | Streaming server (clients subscribe) |
| Typesense/Meilisearch | Search engine sync (batched upsert/delete) |
| embedding/pgvector | AI vector pipeline (embed + UPSERT) |
| Iceberg | Apache Iceberg table writes |
| Wasm | Custom adapter in any language |

**Comparison**: Debezium Server reaches more messaging systems (Kinesis, Pub/Sub, Pulsar, Event Hubs, RocketMQ, RabbitMQ). pgcdc has native adapters that Debezium lacks: SSE, WebSocket, gRPC streaming, exec subprocess, file rotation, pgvector embedding, search engine sync, and multi-sink fan-out from a single binary.

**Gap — pgcdc missing:**
- Amazon Kinesis
- Google Cloud Pub/Sub
- Apache Pulsar
- Azure Event Hubs
- RabbitMQ
- Elasticsearch (pgcdc has Typesense/Meilisearch instead)
- Cloud object storage (S3, GCS, Azure Blob)

**Gap — Debezium missing:**
- SSE, WebSocket, gRPC streaming (browser/app real-time)
- Subprocess stdin (exec)
- File with rotation
- pgvector embedding pipeline
- Search engine sync (Typesense/Meilisearch)
- Multi-sink fan-out in single process

---

## 4. Snapshot Capabilities

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Initial full snapshot | Yes (multiple modes: `initial`, `always`, `initial_only`, `never`, `when_needed`, `custom`) | Yes (`pgcdc snapshot` command or `--snapshot-first`) |
| Snapshot-first then stream | Yes (`initial` mode — default) | Yes (`--snapshot-first --snapshot-table`) |
| Incremental snapshots | Yes (chunk-based, watermark dedup, parallel with streaming) | Yes (chunk-based PK-ordered, signal-table triggered, crash-resumable) |
| Blocking snapshots | Yes (pause streaming, snapshot, resume) | No |
| Ad-hoc snapshot via signal | Yes (`execute-snapshot` signal, incremental or blocking) | Yes (signal table `pgcdc_signals`) |
| Pause/resume snapshot | Yes (`pause-snapshot`, `resume-snapshot` signals) | Yes (crash recovery resumes from progress) |
| Stop snapshot | Yes (`stop-snapshot` signal) | No explicit stop signal |
| Snapshot WHERE filter | Yes (`additional-conditions` on ad-hoc) | No |
| Snapshot surrogate key | Yes (custom PK for ordering) | No (uses actual PK) |
| Custom snapshot SPI | Yes (Java `Snapshotter` interface) | No (but Wasm plugin could wrap) |
| Read-only incremental (PG 13+) | Yes (`read.only=true`, no signal table writes) | No |
| Parallel snapshot threads | Yes (`snapshot.max.threads`) | No |
| Progress persistence | Kafka offsets (built into Connect) | `pgcdc_snapshot_progress` table |

**Gap**: Debezium has more snapshot modes (blocking ad-hoc, WHERE filter, surrogate key, parallel threads, read-only mode). pgcdc's snapshot is simpler but covers the main use cases.

---

## 5. Change Event Format

### Debezium envelope

```json
{
  "before": { ... },
  "after": { ... },
  "source": {
    "version": "3.4.0.Final",
    "connector": "postgresql",
    "name": "my-server",
    "ts_ms": 1234567890,
    "ts_us": 1234567890000,
    "ts_ns": 1234567890000000,
    "snapshot": false,
    "db": "mydb",
    "schema": "public",
    "table": "orders",
    "txId": 1234,
    "lsn": 567890
  },
  "op": "c",
  "ts_ms": 1234567890,
  "transaction": {
    "id": "1234:567890",
    "total_order": 1,
    "data_collection_order": 1
  }
}
```

Operations: `c` (create), `u` (update), `d` (delete), `r` (read/snapshot), `t` (truncate), `m` (message)

### pgcdc native envelope

```json
{
  "id": "UUIDv7",
  "channel": "pgcdc:orders",
  "operation": "INSERT",
  "payload": { "op": "INSERT", "table": "orders", "row": {...}, "old": {...} },
  "source": "wal_replication",
  "created_at": "2026-02-25T...",
  "transaction": { "xid": 1234, "commit_time": "...", "seq": 1 }
}
```

pgcdc also supports **Debezium-compatible output** via `--debezium-envelope` transform, producing the standard Debezium envelope format with `before`/`after`/`op`/`source`/`transaction`.

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| before/after fields | Native | Via `--debezium-envelope` transform |
| Truncate events | Yes (`op: t`) | No |
| Logical decoding messages | Yes (`op: m`) | No |
| Tombstone events (null value for deletes) | Yes (for Kafka log compaction) | No |
| Primary key update handling | DELETE + CREATE event pair with `__debezium.newkey`/`__debezium.oldkey` headers | Emitted as UPDATE (old PK in `old`, new PK in `row`) |
| CloudEvents format | Yes (via CloudEventsConverter) | No |
| Timestamp precision | ms, us, ns variants | ms only |
| UUIDv7 event ID | No | Yes |
| Schema in event | Via Avro/Protobuf schema, or JSON Schema | `--include-schema` adds `columns` array |

**Gap**: pgcdc missing truncate events, logical decoding messages, tombstones, CloudEvents format, PK update special handling, nanosecond timestamps.

---

## 6. Serialization and Schema Registry

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| JSON | Yes | Yes |
| Avro | Yes (Confluent, Apicurio registries) | Yes (Confluent registry) |
| Protobuf | Yes | Yes |
| JSON Schema | Yes | No |
| CloudEvents | Yes | No |
| Binary (raw bytes) | Yes | No |
| Simple String | Yes | No |
| Schema Registry integration | Confluent, Apicurio, custom | Confluent-compatible |
| Auto schema registration | Yes | Yes |
| Custom converters (data type) | Yes (Java SPI) | No (but Wasm transform could reshape) |

**Gap**: pgcdc missing JSON Schema, CloudEvents, Apicurio registry support, custom data type converters.

---

## 7. Transform Pipeline

### Debezium SMTs (Single Message Transforms)

| SMT | Purpose |
|-----|---------|
| Topic Routing | Re-route to different topics by regex |
| Content-Based Routing | Route by message content |
| New Record State Extraction | Flatten envelope to just before/after |
| Outbox Event Router | Microservices outbox pattern routing |
| Message Filtering | Filter by content predicate |
| HeaderToValue | Move headers into record values |
| Partition Routing | Route to specific Kafka partitions |
| Timezone Converter | Convert timestamps to target timezone |
| Geometry Format Transformer | WKB/EWKB conversion |
| TimescaleDB Integration | Enrich events from hypertables |
| Decode Logical Decoding Message | Convert binary messages |
| SMT Predicates | Conditionally apply transforms |

### pgcdc transforms

| Transform | Purpose |
|-----------|---------|
| `drop_columns` | Remove fields from payload |
| `rename_fields` | Rename payload keys |
| `mask` (hash/redact/partial) | Mask sensitive data (SHA-256, redact, partial) |
| `filter` (field value) | Drop events by field value |
| `filter` (operation) | Drop events by operation type |
| `debezium` | Rewrite into Debezium envelope format |
| Wasm transforms | Any custom logic in Rust/Go/Python/TypeScript |
| Per-adapter scoping | Different transforms per adapter |
| Global + per-adapter | Compose global and adapter-specific chains |

**Comparison**: Debezium has more specialized SMTs (timezone, geometry, TimescaleDB, outbox router, partition routing). pgcdc has the mask transform (PII compliance) and Wasm extensibility that Debezium lacks. Debezium SMTs are Java classes requiring JVM compilation; pgcdc Wasm transforms accept any language.

**Gap — pgcdc missing:**
- Timezone conversion transform
- Partition routing (N/A for non-Kafka sinks)
- Outbox Event Router SMT equivalent (pgcdc has outbox detector instead — different approach)
- Record state extraction / flattening
- Content-based topic routing
- HeaderToValue equivalent
- SMT Predicates (conditional application)

**Gap — Debezium missing:**
- Field masking (hash, redact, partial) — Debezium has column mask but only asterisks, no hash/redact modes
- Wasm/polyglot custom transforms
- Per-adapter transform scoping

---

## 8. Signaling and Notifications

### Debezium signaling

| Channel | Mechanism |
|---------|-----------|
| Source (DB table) | INSERT into `debezium_signal` table |
| Kafka topic | Send JSON to signal topic |
| JMX | JMX `signal` operation |
| File | Write to `file-signals.txt` |
| Custom | Java SPI implementation |

Signal actions: `execute-snapshot`, `stop-snapshot`, `pause-snapshot`, `resume-snapshot`, `log`

### Debezium notifications

| Channel | Mechanism |
|---------|-----------|
| Kafka topic (sink) | Publish to notification topic |
| Log | Append to connector log |
| JMX | Expose as JMX bean attribute |
| Custom | Java SPI implementation |

Notification types: STARTED, IN_PROGRESS, TABLE_SCAN_COMPLETED, COMPLETED, ABORTED, SKIPPED, PAUSED, RESUMED (for both initial and incremental snapshots)

### pgcdc signaling

| Channel | Mechanism |
|---------|-----------|
| Signal table | INSERT into `pgcdc_signals` |
| SIGHUP | Config reload (transforms, routes) |

Signal actions: incremental snapshot trigger

pgcdc emits snapshot lifecycle events (`SNAPSHOT_STARTED`, `SNAPSHOT_COMPLETED`) on `pgcdc:_snapshot` channel, and schema changes on `pgcdc:_schema` channel. No formal notification system beyond these event channels.

**Gap**: pgcdc has no Kafka/JMX/file signal channels, no notification system (log, topic, JMX), no `stop-snapshot`/`pause-snapshot` signal actions. However, SIGHUP config reload is unique to pgcdc.

---

## 9. Delivery Guarantees

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| At-least-once | Yes (default) | Yes (default) |
| Exactly-once (Kafka) | Yes (via Kafka Connect transactions, KIP-618, PG/MySQL/Oracle/etc.) | Yes (`--kafka-transactional-id`) |
| Offset storage | Kafka internal topics (Connect) or custom | `pgcdc_checkpoints` PG table |
| Cooperative checkpointing | Via Kafka consumer groups | Native `--cooperative-checkpoint` (min across all adapters) |
| Deduplication | Kafka log compaction + consumer idempotency | NATS `Nats-Msg-Id`, Kafka event ID key, UUIDv7 for consumers |

**Note**: Debezium's exactly-once relies on Kafka Connect distributed mode 3.3+ with known open issues (KAFKA-17734, KAFKA-17754, KAFKA-17582). pgcdc's Kafka exactly-once uses per-event transactions via `franz-go`.

---

## 10. Filtering and Routing

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Schema include/exclude | Yes (regex: `schema.include.list`, `schema.exclude.list`) | No (single publication) |
| Table include/exclude | Yes (regex: `table.include.list`, `table.exclude.list`) | `--all-tables` or manual publication scope |
| Column include/exclude | Yes (regex: `column.include.list`, `column.exclude.list`) | `--drop-columns` transform (post-capture) |
| Column masking | Yes (asterisks, hash with salt) | Yes (hash/SHA-256, redact, partial) |
| Skip operations | Yes (`skipped.operations` = c,u,d,t) | Yes (`--filter-operations INSERT,UPDATE`) |
| Skip unchanged | Yes (`skip.messages.without.change`) | No |
| Event routing | Topic Routing SMT (regex), Content-Based Routing SMT | `--route adapter=channel1,channel2` (bus-level) |
| Message prefix filter | Yes (`message.prefix.include.list`) | No |

**Gap**: pgcdc filtering happens post-capture (transform layer), not at the WAL decoding level. Debezium filters at source, reducing WAL processing overhead. pgcdc's column filtering via `--drop-columns` still decodes all columns then strips them — Debezium never reads excluded columns.

---

## 11. Schema and Type Handling

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Schema in events | Full Avro/Protobuf/JSON Schema | `--include-schema` adds `columns` array |
| Schema evolution detection | Via RelationMessage (triggers refresh) | `--schema-events` emits SCHEMA_CHANGE events |
| Schema refresh modes | `columns_diff`, `columns_diff_exclude_unchanged_toast` | Automatic on RelationMessage change |
| DDL capture | No (logical decoding limitation) | No |
| Custom data type converters | Yes (Java SPI for custom type mapping) | No |
| REPLICA IDENTITY handling | Full support (DEFAULT, NOTHING, FULL, INDEX) | Uses whatever PG provides |
| TOAST column handling | Yes (`unavailable.value.placeholder`) | Raw (unchanged TOAST columns may be null) |
| hstore handling | `map` or `json` modes | Raw JSON |
| PostGIS types | Yes (native mapping) | No specific handling |
| pgvector types | Yes (native mapping) | Via embedding adapter (not in event schema) |
| Domain types | Yes | No specific handling |
| Generated columns | Limitation: pgoutput doesn't capture them | Same limitation (pgoutput) |

**Gap**: pgcdc lacks custom type converters, explicit TOAST handling, PostGIS/domain type mapping, hstore modes. These matter for enterprises with complex PG schemas.

---

## 12. Operational Features

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Heartbeat | Yes (`heartbeat.interval.ms` + `heartbeat.action.query`) | Yes (`--heartbeat-interval` + `pgcdc_heartbeat` table) |
| Slot lag monitoring | Via JMX metrics | Yes (`pgcdc_slot_lag_bytes` gauge + log warnings) |
| Source-aware backpressure | No | Yes (3-zone auto-throttle/pause/shed) |
| Config hot-reload | No (requires connector restart) | Yes (SIGHUP reloads transforms + routes) |
| Slot management | Via Kafka Connect REST API | `pgcdc slot list\|status\|drop` CLI |
| Error handling modes | `fail`, `warn`, `skip` | Typed errors + DLQ |
| Dead letter queue | Via Kafka Connect DLQ (error topic) | `--dlq stderr\|pg_table\|none` (also via Wasm plugin) |
| Graceful shutdown | Kafka Connect task stop | Signal → context cancel → drain → timeout |
| Health checks | JMX MBeans | `/healthz` HTTP endpoint (per-component) |
| Multiple tasks | Yes (`tasks.max`, but always 1 for PG) | Single process, multiple adapters |
| Publication auto-create | Yes (`publication.autocreate.mode`: all_tables, disabled, filtered) | Yes (`--all-tables` creates FOR ALL TABLES) |
| Replica identity auto-set | Yes (`replica.identity.autoset.values`) | No |
| Initial SQL statements | Yes (`database.initial.statements`) | No |
| LSN flush control | Yes (`lsn.flush.mode` — new in 3.4) | Via cooperative checkpointing |

**Gap — pgcdc missing:**
- Replica identity auto-set
- Initial SQL statements on connect
- Error handling modes (fail/warn/skip per event)
- LSN flush mode control

**Gap — Debezium missing:**
- Source-aware backpressure (throttle/pause/shed)
- Config hot-reload without restart
- HTTP health endpoint (uses JMX instead)
- Native DLQ to PG table
- Adapter-priority shedding

---

## 13. Monitoring and Observability

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Metrics system | JMX MBeans (+ Prometheus via JMX exporter) | Native Prometheus `/metrics` endpoint |
| Tracing | OpenTelemetry via `ActivateTracingSpan` SMT | Native OpenTelemetry (OTLP, stdout) |
| Trace propagation | W3C TraceContext in Kafka headers | W3C TraceContext in HTTP, Kafka, NATS headers |
| Span architecture | db-log-write → debezium-read → kafka-produce | detector → adapter (with span context on events) |
| Snapshot metrics | Rows scanned, duration, table counts | `pgcdc_incremental_snapshot_*` counters/histograms |
| Streaming metrics | LSN position, events processed, lag | `pgcdc_events_delivered`, `pgcdc_slot_lag_bytes`, etc. |
| Per-adapter metrics | No (single Kafka topic output) | Yes (`pgcdc_events_delivered{adapter="..."}`) |
| Custom MBean names | Yes | N/A (Prometheus labels instead) |
| Backpressure metrics | No | `pgcdc_backpressure_state`, `pgcdc_backpressure_throttle_*` |
| Transform metrics | No | `pgcdc_transform_dropped_total`, `pgcdc_transform_errors_total` |
| Plugin metrics | No | `pgcdc_plugin_calls_total`, `pgcdc_plugin_duration_seconds` |
| Grafana dashboard | Example in debezium-examples repo | Not yet |

**pgcdc advantage**: Native Prometheus (no JMX exporter needed), per-adapter granularity, backpressure/transform/plugin metrics. Debezium requires JMX → Prometheus bridge for modern observability stacks.

---

## 14. Deployment and Operations

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Kafka Connect (primary) | Native | N/A |
| Standalone server | Debezium Server (Quarkus-based) | Single Go binary |
| Embedded library | Yes (JVM only, `debezium-embedded`) | Yes (Go only, `pgcdc.Pipeline`) |
| Kubernetes operator | Yes (Debezium Operator via Strimzi) | No |
| Management platform | Yes (Debezium Platform — incubating, UI + API) | No |
| Web UI | Yes (connector wizard, status, restart) | No |
| Helm chart | Yes (via Strimzi) | No |
| Docker image | Yes (multiple variants) | Yes (`make docker-build`) |
| ARM64 support | Via JVM (depends on base image) | Native Go cross-compile |
| Configuration format | JSON (connector config) | CLI flags + YAML + env vars |
| Schema history storage | Kafka topic (for MySQL/Oracle DDL) | N/A (PG logical decoding doesn't need it) |

**Gap**: pgcdc has no Kubernetes operator, Helm chart, management platform, or web UI. Debezium has a mature Kubernetes story via Strimzi.

---

## 15. Extensibility

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Custom transforms | Java SMT classes (compile, package JAR, deploy) | Built-in + Wasm plugins (any language, hot-load `.wasm`) |
| Custom serializers | Java Converter SPI | Wasm transforms can reshape |
| Custom snapshot | Java Snapshotter SPI | No |
| Custom signal channels | Java SignalChannelReader SPI | No (signal table only) |
| Custom notification channels | Java NotificationChannel SPI | No |
| Custom sink (Server) | Java SPI | Wasm adapter plugin |
| Custom DLQ | No (Kafka Connect error topic only) | Wasm DLQ plugin |
| Custom checkpoint store | No (Kafka offsets only) | Wasm checkpoint plugin |
| Plugin languages | Java only | Any Extism PDK language (Rust, Go, Python, TypeScript, C, Zig) |
| Plugin hot-reload | No (requires restart) | Load on startup (SIGHUP reloads transforms) |

**pgcdc advantage**: Wasm plugin system is a generational leap. Debezium requires Java for all extensions. pgcdc accepts any language compiled to Wasm, with host functions for logging, metrics, and HTTP.

---

## 16. Outbox Pattern Support

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Approach | WAL capture + Outbox Event Router SMT | Native outbox detector (polling) |
| Table schema | Fixed: `id`, `aggregatetype`, `aggregateid`, `type`, `payload` | Configurable columns in `pgcdc_outbox` |
| Routing | By `aggregatetype` → Kafka topics | By channel name → adapter routing |
| Quarkus integration | Yes (Outbox Quarkus Extension — auto-persist) | No |
| Cleanup | Debezium captures then app deletes (or soft-delete) | Auto DELETE or UPDATE `processed_at` |
| Concurrency safety | WAL-based (no polling contention) | `FOR UPDATE SKIP LOCKED` (multi-instance safe) |

**Different approaches**: Debezium uses WAL to capture outbox writes (zero polling overhead). pgcdc polls the outbox table directly (simpler setup, no WAL/publication needed). Both are valid — Debezium's is lower-latency; pgcdc's requires no replication slot.

---

## 17. Transaction Metadata

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Transaction boundaries | BEGIN/END events with event counts | BEGIN/COMMIT markers on `pgcdc:_txn` channel |
| Transaction ID | `txId:lsn` composite | `xid` (uint32) |
| Per-event position | `total_order` + `data_collection_order` | `seq` (per-transaction sequence) |
| Commit timestamp | In `source.ts_ms` | In `transaction.commit_time` |
| Event counts in END | Yes (`event_count`, per-collection breakdown) | No |

**Gap**: pgcdc lacks per-collection event counts in transaction end markers.

---

## 18. PostgreSQL-Specific Features

| Feature | Debezium | pgcdc |
|---------|----------|-------|
| Logical decoding plugins | pgoutput (native), decoderbufs (protobuf) | pgoutput only |
| Slot drop on stop | Configurable (`slot.drop.on.stop`) | Configurable (`--persistent-slot` keeps, default drops) |
| Slot retry logic | `slot.max.retries` + `slot.retry.delay.ms` | Backoff with jitter |
| Publication modes | `all_tables`, `disabled`, `filtered` | `--all-tables` or manual |
| XMIN tracking | Yes (`xmin.fetch.interval.ms`) | No |
| Replication connection status | Configurable interval (`status.update.interval.ms`) | `standbyStatusInterval` (10s default) |
| SSL modes | Full: disable, require, verify-ca, verify-full | Via connection string |
| TCP keepalive | Configurable | Via pgx defaults |

---

## Summary: Where pgcdc Leads

1. **Zero infrastructure** — single binary, no Kafka/JVM/ZooKeeper
2. **Wasm plugin system** — polyglot extensibility (transforms, adapters, DLQ, checkpoint)
3. **Source-aware backpressure** — automatic throttle/pause/shed based on WAL lag
4. **Multi-sink fan-out** — one process feeds N adapters simultaneously
5. **Config hot-reload** — SIGHUP swaps transforms and routes without restart
6. **Native adapters** — SSE, WebSocket, gRPC streaming, exec, file, pgvector embedding, search engine sync
7. **Field masking** — SHA-256 hash, redact, partial masking (vs Debezium's asterisks-only)
8. **Go library** — import `pgcdc.Pipeline` into Go applications
9. **Native Prometheus** — no JMX exporter bridge needed
10. **Per-adapter metrics/transforms** — granular control per destination
11. **Outbox without WAL** — polling detector works without replication slot

## Summary: Where Debezium Leads

1. **Multi-database** — 11 source connectors vs PostgreSQL only
2. **Kafka ecosystem** — native Connect integration, exactly-once via KIP-618
3. **Notification system** — Kafka topic, log, JMX notifications for operational events
4. **Signal channels** — DB table, Kafka, JMX, file, custom SPI
5. **Snapshot sophistication** — blocking, read-only, parallel, WHERE filter, surrogate key, custom SPI
6. **Source-level filtering** — schema/table/column include/exclude at WAL decode level (pgcdc filters post-capture)
7. **Data type handling** — PostGIS, hstore modes, domain types, custom converters
8. **TOAST handling** — explicit placeholder for unchanged TOAST columns
9. **Truncate events** — captures table truncations
10. **CloudEvents format** — standard event envelope format
11. **Kubernetes operator** — Strimzi integration, Debezium Platform (incubating)
12. **Management UI** — web-based connector wizard and monitoring
13. **Outbox Event Router** — sophisticated SMT for microservices patterns
14. **Enterprise support** — Red Hat commercial backing

## Summary: Parity

Both tools offer: WAL logical decoding, incremental snapshots, signal table triggers, Avro/Protobuf/JSON encoding, Schema Registry, Kafka output, NATS output, Redis output, Iceberg output, OpenTelemetry tracing, heartbeat, persistent slots, cooperative checkpointing, transform pipeline, DLQ, exactly-once Kafka delivery, transaction metadata, schema evolution detection, outbox pattern support.
