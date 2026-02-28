# Delivery Guarantees

This document maps each pgcdc adapter to its delivery guarantee, idempotency strategy, and cooperative checkpoint interaction.

## Summary Table

| Adapter | Guarantee | Retry Strategy | DLQ Support | Cooperative Ack | Idempotency |
|---------|-----------|----------------|-------------|-----------------|-------------|
| **stdout** | At-most-once | None | No | Yes (on write) | N/A (stream) |
| **webhook** | At-least-once | Exponential backoff (5 retries) | Yes | Yes (after terminal outcome) | Event ID header |
| **sse** | At-most-once | None (non-blocking send) | No | No | N/A (push) |
| **file** | At-most-once | None (fatal on write error) | No | No | N/A (append) |
| **exec** | At-least-once | Process restart with backoff, pending event re-delivery | No | No | N/A (stream) |
| **pg_table** | At-least-once | Reconnect with backoff | No | Yes (after insert or intentional skip) | Event ID primary key |
| **ws** | At-most-once | None (non-blocking send) | No | No | N/A (push) |
| **embedding** | At-least-once | API retry (3 attempts) + DB reconnect | Yes | Yes (implicit via Acknowledger) | UPSERT on source_id |
| **nats** | At-least-once | Reconnect with backoff | No | Yes (after JetStream ack) | `Nats-Msg-Id` header (dedup) |
| **kafka** | At-least-once | Reconnect with backoff | Yes | Yes (after produce ack or DLQ) | Event ID as record key |
| **kafka** (transactional) | Exactly-once | Per-event Kafka transactions | Yes | Yes | `TransactionalID` |
| **search** | At-least-once | 3-attempt retry per HTTP request | No | No | Upsert semantics |
| **redis** | At-least-once | Reconnect with backoff | Yes | No | SET/DEL are idempotent |
| **grpc** | At-most-once | None (non-blocking send to clients) | No | No | N/A (push) |
| **s3** | At-least-once | Flush retry (10 consecutive failures = reconnect) | Yes | Yes (batch ack after upload) | UUID object keys |
| **kafkaserver** | At-most-once | None (in-memory ring buffer) | No | Yes (via Acknowledger) | N/A (ring buffer) |
| **view** | At-most-once | None (in-memory window state) | No | No | N/A (aggregation) |

## Detailed Behavior

### stdout

The stdout adapter (`adapter/stdout/stdout.go`) writes JSON-lines to an `io.Writer`. There is no retry logic. If `json.Encoder.Encode` fails, the adapter returns a fatal error and shuts down. Events are acked immediately after successful write when cooperative checkpointing is enabled.

### webhook

The webhook adapter (`adapter/webhook/webhook.go`) delivers events via HTTP POST with exponential backoff and full jitter. It retries on network errors, HTTP 429, and 5xx responses up to `maxRetries` (default 5). Non-retryable 4xx responses are logged and skipped (the event is considered "handled"). After all retries are exhausted, the event is sent to the DLQ. The adapter acks after every terminal outcome: successful delivery, DLQ record, or non-retryable skip.

**Idempotency**: The `X-PGCDC-Event-ID` header carries the event's UUIDv7 ID on every request. Receivers can use this for deduplication across retries.

**Drain**: The adapter implements `adapter.Drainer` and waits for all in-flight deliveries (tracked via `sync.WaitGroup`) during graceful shutdown.

### sse

The SSE broker (`adapter/sse/sse.go`) fans out events to connected HTTP clients using non-blocking channel sends. If a client's per-connection buffer (default 256) is full, the event is dropped for that client with a `pgcdc_events_dropped_total{adapter="sse"}` metric increment. There is no retry or acknowledgment mechanism.

### file

The file adapter (`adapter/file/file.go`) writes JSON-lines to a file with optional rotation. If `json.Encoder.Encode` or `os.File.Sync` fails, the adapter returns a fatal error. There is no retry logic. Events are counted as delivered only after a successful `Sync` call.

### exec

The exec adapter (`adapter/exec/exec.go`) pipes JSON-lines to a long-running subprocess's stdin. If the subprocess exits or a write fails, the failed event is stored as "pending" and re-delivered to the next process instance. The process is restarted with exponential backoff. This provides at-least-once delivery for the pending event, but events already written to the subprocess stdin before the crash are not tracked.

### pg_table

The pg_table adapter (`adapter/pgtable/pgtable.go`) inserts events into a PostgreSQL table. It reconnects with exponential backoff on connection loss. Non-connection errors (e.g., constraint violations) cause the event to be skipped and acked. Connection-loss errors trigger reconnection without acking the event.

**Idempotency**: The event ID serves as the primary key. Duplicate inserts are rejected by the database constraint.

### ws

The WebSocket broker (`adapter/ws/ws.go`) mirrors the SSE pattern: non-blocking sends to per-client buffered channels (default 256). Slow clients have events dropped with metric tracking. No retry or acknowledgment.

### embedding

The embedding adapter (`adapter/embedding/embedding.go`) calls an OpenAI-compatible API to generate vectors and UPSERTs them into a pgvector table. The embedding API call retries up to 3 times with exponential backoff on network errors, 429, and 5xx. Non-retryable 4xx responses skip the event. If all retries are exhausted, the event is sent to the DLQ. The DB connection reconnects with backoff on connection loss.

**Idempotency**: The `source_id` column has a unique constraint. UPSERT (`INSERT ... ON CONFLICT DO UPDATE`) ensures repeated processing of the same event produces the same result.

**Change detection**: When `--embedding-skip-unchanged` is enabled, UPDATE events where none of the embedding columns changed are skipped, avoiding unnecessary API calls.

### nats

The NATS JetStream adapter (`adapter/nats/nats.go`) publishes events and waits for JetStream acknowledgment. On publish failure, the adapter returns an error that triggers reconnection with backoff. The `Nats-Msg-Id` header is set to the event ID for server-side deduplication.

**Idempotency**: JetStream's built-in deduplication uses the `Nats-Msg-Id` header. If pgcdc reconnects and re-publishes the same event, NATS will deduplicate it.

### kafka

The Kafka adapter (`adapter/kafka/kafka.go`) publishes events with `RequireAll` acks (all in-sync replicas must acknowledge). On non-terminal errors, the adapter triggers a reconnect loop with backoff. Terminal Kafka errors (non-retriable per `kerr.Error`) are sent to the DLQ and acked.

**Exactly-once mode**: When `--kafka-transactional-id` is set, each event is produced inside its own Kafka transaction (`BeginTransaction` / `ProduceSync` / `EndTransaction`). Transaction failures are aborted and the error propagated.

**Idempotency**: The event ID is used as the Kafka record key. Combined with `RequireAll` acks, this prevents silent data loss. With transactional mode, Kafka's transaction coordinator provides exactly-once semantics.

### search

The search adapter (`adapter/search/search.go`) syncs events to Typesense or Meilisearch. Upserts are batched; deletes are individual. Each HTTP request retries up to 3 times with backoff on 5xx and 429. Non-retryable 4xx errors are treated as terminal.

**Idempotency**: Both Typesense and Meilisearch use document ID-based upsert semantics. Repeated processing of the same event overwrites with identical data.

**TOAST handling**: When unchanged TOAST columns are detected (`_unchanged_toast_columns` in payload), the adapter uses partial update (PATCH for Typesense, POST merge for Meilisearch) to avoid overwriting stored fields with null.

### redis

The Redis adapter (`adapter/redis/redis.go`) operates in two modes: `invalidate` (DEL on any change) and `sync` (SET for INSERT/UPDATE, DEL for DELETE). It reconnects with backoff on connection loss. Non-connection errors are logged, and the event is sent to the DLQ if configured.

**Idempotency**: DEL and SET are naturally idempotent. In sync mode with unchanged TOAST columns, the adapter merges with the existing Redis value via GET before SET.

### grpc

The gRPC adapter (`adapter/grpc/grpc.go`) follows the broker pattern. Events are sent to per-client buffered channels (256 capacity) using non-blocking sends. Full channels cause drops. The gRPC server uses `GracefulStop` during shutdown.

### s3

The S3 adapter (`adapter/s3/s3.go`) buffers events in memory and periodically flushes partitioned objects to S3-compatible storage. Flush is triggered by time interval (default 1 minute) or buffer size (default 10,000 events). Failed flushes put events back into the buffer for retry. After 10 consecutive flush failures, the adapter triggers a reconnect cycle. All events in a successful flush batch are acked together.

**Drain**: The adapter implements `adapter.Drainer`. On shutdown, it flushes remaining buffered events with a configurable drain timeout (default 30 seconds).

**Idempotency**: Each S3 object has a UUID-based key, so retried flushes produce new objects rather than overwriting. Consumers should deduplicate by event ID.

### kafkaserver

The Kafka protocol server (`adapter/kafkaserver/server.go`) uses an in-memory ring buffer per partition (default 10,000 records). When the buffer is full, the oldest records are overwritten. Consumers that fall behind receive `OFFSET_OUT_OF_RANGE`, matching standard Kafka behavior. There is no DLQ because events are in-memory only.

**Offset persistence**: Consumer group offsets are persisted via `checkpoint.Store` with keys `kafka:{group}:{topic}:{partition}`. This survives restarts but not ring buffer overflow.

### view

The view adapter (`adapter/view/view.go`) processes events through streaming SQL windows and re-injects `VIEW_RESULT` events into the bus. All window state is in-memory. If pgcdc restarts, window state is lost. The view adapter does not implement cooperative checkpointing.

**Loop prevention**: Events on `pgcdc:_view:*` channels are skipped by the engine to prevent infinite re-processing.

## Cooperative Checkpoint Interaction

When `--cooperative-checkpoint` is enabled, the WAL detector only reports LSN positions that **all** adapters have acknowledged. This prevents PostgreSQL from recycling WAL segments that slower adapters haven't processed.

Adapters that implement `adapter.Acknowledger` (`SetAckFunc`) call the ack function after fully processing each event:
- **webhook**: acks after delivery, DLQ record, or non-retryable skip
- **kafka**: acks after produce ack or DLQ record
- **stdout**: acks after successful write
- **pg_table**: acks after successful insert or intentional skip (constraint violation)
- **nats**: acks after JetStream publish acknowledgment
- **embedding**: acked by the middleware Ack layer after successful `Deliver()` call
- **s3**: acks all events in a batch after successful S3 upload
- **kafkaserver**: implements Acknowledger interface

Adapters that do **not** implement `Acknowledger` are auto-acked when the event is sent to their subscriber channel. This means the checkpoint can advance before the adapter has actually delivered the event, which is acceptable for at-most-once adapters (SSE, WS, gRPC, file, view).
