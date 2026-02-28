# Detectors

pgcdc supports five event source types (detectors).

## LISTEN/NOTIFY

The simplest approach. Uses PostgreSQL's built-in LISTEN/NOTIFY mechanism.

**Pros**: No PG config changes needed. Works on any PG 12+. Sub-millisecond latency.
**Cons**: Requires triggers. 8KB payload limit. No transaction metadata. Events lost during disconnects.

```bash
pgcdc init --table orders | psql mydb
pgcdc listen -c pgcdc:orders --db postgres://...
```

## WAL Logical Replication

Captures changes directly from the write-ahead log. No triggers needed.

**Pros**: No triggers. No payload size limit. Full row data including old values. Transaction metadata. Persistent slots survive disconnects.
**Cons**: Requires `wal_level=logical`. One replication slot per pgcdc instance.

```bash
pgcdc init --table orders --detector wal | psql mydb
pgcdc listen --detector wal --publication pgcdc_orders --db postgres://...
```

### Features

- **Persistent slots**: `--persistent-slot` creates a named slot that survives disconnects
- **Checkpointing**: LSN position saved to `pgcdc_checkpoints` table
- **Transaction metadata**: `--tx-metadata` adds xid, commit_time, seq to events
- **Transaction markers**: `--tx-markers` wraps transactions in BEGIN/COMMIT events
- **Schema information**: `--include-schema` adds column type metadata
- **Schema evolution**: `--schema-events` emits SCHEMA_CHANGE events
- **Heartbeat**: `--heartbeat-interval 30s` prevents WAL bloat on idle databases
- **Slot lag monitoring**: `pgcdc_slot_lag_bytes` metric + `--slot-lag-warn` threshold
- **TOAST cache**: `--toast-cache` resolves unchanged TOAST columns without `REPLICA IDENTITY FULL` (see below)

### TOAST Column Handling

PostgreSQL stores large column values (>2KB) out-of-line via TOAST. With `REPLICA IDENTITY DEFAULT`, UPDATE events for unchanged TOAST columns arrive in the WAL with no data — only a marker saying the value is unchanged.

By default pgcdc emits these as:
```json
{"op":"UPDATE","row":{"id":"42","status":"shipped","description":null},"_unchanged_toast_columns":["description"]}
```

Enable the TOAST cache to resolve them automatically:

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  --toast-cache --toast-cache-max-entries 100000 \
  --db postgres://...
```

The cache stores the most recent full row per `(table, primary key)`. On UPDATE, unchanged columns are backfilled from the cached row. On cache hit the event looks like a normal full row with no `_unchanged_toast_columns` field.

**When the cache can't help** (cold start, evicted entry, no PK): the column is `null` and `_unchanged_toast_columns` lists the affected columns. Consumers should check for this field when `REPLICA IDENTITY FULL` is not set.

The alternative — `ALTER TABLE orders REPLICA IDENTITY FULL` — writes the full old row on every UPDATE, doubling WAL volume. The TOAST cache is a lower-cost option for read-heavy tables with large columns.

### Slot Management

```bash
pgcdc slot list --db postgres://...
pgcdc slot status --slot-name pgcdc_orders --db postgres://...
pgcdc slot drop --slot-name pgcdc_orders --db postgres://...
```

## Outbox Pattern

Polls a transactional outbox table. Best for applications that need transactional guarantees.

**Pros**: Events are part of the application transaction. No PG config changes. Works with any PG version.
**Cons**: Polling-based (configurable interval, default 500ms). Requires outbox table.

```bash
pgcdc init --table my_outbox --adapter outbox | psql mydb
pgcdc listen --detector outbox --outbox-table my_outbox --db postgres://...
```

Options:
- `--outbox-poll-interval 500ms` — polling frequency
- `--outbox-batch-size 100` — rows per poll
- `--outbox-keep-processed` — UPDATE instead of DELETE (set `processed_at`)

Uses `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent processing.

## MySQL Binlog

Connects to MySQL as a replication slave to capture changes from the binlog (ROW format required).

**Pros**: Native CDC from MySQL. No triggers. Captures INSERT/UPDATE/DELETE.
**Cons**: Requires `binlog_format=ROW`. Needs a unique `server_id`.

```bash
pgcdc listen --detector mysql \
  --mysql-addr localhost:3306 --mysql-user replicator --mysql-password secret \
  --mysql-server-id 100
```

Options:
- `--mysql-tables schema.table` — filter by schema.table pattern
- `--mysql-gtid` — use GTID-based replication
- `--mysql-flavor mysql|mariadb` — MySQL or MariaDB flavor

Events are emitted on `pgcdc:<schema>.<table>` channels.

## MongoDB Change Streams

Uses MongoDB's native Change Streams API (requires replica set or sharded cluster).

**Pros**: Native CDC from MongoDB. No polling. Captures insert/update/replace/delete.
**Cons**: Requires replica set or sharded cluster. No cooperative checkpoint or backpressure support.

```bash
pgcdc listen --detector mongodb \
  --mongodb-uri mongodb://localhost:27017 --mongodb-database mydb
```

Options:
- `--mongodb-scope collection|database|cluster` — watch scope
- `--mongodb-collections coll1,coll2` — collection filter
- `--mongodb-full-document updateLookup|default` — full document lookup on updates

Resume tokens are persisted to a MongoDB metadata collection for crash-resumable streaming.
