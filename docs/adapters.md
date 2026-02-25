# Adapters

pgcdc ships with 15 built-in adapters. Use `-a <name>` to enable one or more.

## stdout

JSON lines to stdout. Default adapter.

```bash
pgcdc listen -c pgcdc:orders --db postgres://... | jq .payload
```

## webhook

HTTP POST with retries, HMAC signing, exponential backoff.

```bash
pgcdc listen -c pgcdc:orders -a webhook -u https://example.com/hook \
  --signing-key my-secret --retries 10 --db postgres://...
```

Headers: `Content-Type`, `User-Agent`, `X-PGCDC-Event-ID`, `X-PGCDC-Channel`, `X-PGCDC-Signature` (when signing key set).

Retries on 5xx and 429. No retry on other 4xx. Failed events go to DLQ.

## SSE (Server-Sent Events)

```bash
pgcdc listen -c pgcdc:orders -a sse --sse-addr :8080 --db postgres://...
# GET /events (all) or GET /events/pgcdc:orders (filtered)
```

## WebSocket

```bash
pgcdc listen -c pgcdc:orders -a ws --sse-addr :8080 --db postgres://...
# ws://localhost:8080/ws or ws://localhost:8080/ws/pgcdc:orders
```

## gRPC

Starts a gRPC streaming server.

```bash
pgcdc listen -c pgcdc:orders -a grpc --grpc-addr :9090 --db postgres://...
```

Proto definition: `adapter/grpc/proto/pgcdc.proto`. Clients call `Subscribe(SubscribeRequest)` with optional channel filter.

## file

JSON lines to file with rotation.

```bash
pgcdc listen -c pgcdc:orders -a file --file-path /var/log/events.jsonl \
  --file-max-size 104857600 --file-max-files 5 --db postgres://...
```

## exec

Pipe to subprocess stdin.

```bash
pgcdc listen -c pgcdc:orders -a exec --exec-command 'jq .payload >> /tmp/payloads.jsonl' --db postgres://...
```

## pg_table

INSERT into a PostgreSQL table.

```bash
pgcdc init --table audit_events --adapter pg_table | psql mydb
pgcdc listen -c pgcdc:orders -a pg_table --pg-table-name audit_events --db postgres://...
```

## NATS JetStream

```bash
pgcdc listen -c pgcdc:orders -a nats --nats-url nats://localhost:4222 \
  --nats-stream pgcdc --nats-subject pgcdc --db postgres://...
```

Subject mapping: `pgcdc:orders` -> `pgcdc.orders`. Dedup via `Nats-Msg-Id` header.

## search (Typesense / Meilisearch)

Sync documents to a search engine with batching.

```bash
pgcdc listen -c pgcdc:articles -a search \
  --search-engine typesense --search-url http://localhost:8108 \
  --search-api-key xyz --search-index articles --db postgres://...
```

INSERT/UPDATE → upsert document (full row as JSON). DELETE → delete document.

**TOAST-aware**: when an UPDATE event contains `_unchanged_toast_columns` (unchanged large columns without `REPLICA IDENTITY FULL`), those columns are stripped from the document and the update is sent as a partial update (Typesense `PATCH`, Meilisearch merge) rather than a full upsert. This prevents overwriting existing search-indexed content with null. Pair with `--toast-cache` on the WAL detector to eliminate the metadata entirely on cache hits.

Flags: `--search-engine`, `--search-url`, `--search-api-key`, `--search-index`, `--search-id-column`, `--search-batch-size`, `--search-batch-interval`.

## redis

Cache invalidation or sync.

```bash
# Invalidate mode (default): DEL key on any change
pgcdc listen -c pgcdc:orders -a redis --redis-url redis://localhost:6379 \
  --redis-key-prefix orders: --db postgres://...

# Sync mode: SET on INSERT/UPDATE, DEL on DELETE
pgcdc listen -c pgcdc:orders -a redis --redis-url redis://localhost:6379 \
  --redis-mode sync --redis-key-prefix orders: --db postgres://...
```

Key format: `<prefix><id_column_value>` (e.g., `orders:42`).

**TOAST-aware** (sync mode): when an UPDATE event contains `_unchanged_toast_columns`, the adapter performs `GET` → merge → `SET` instead of a plain `SET`, so unchanged large columns are not overwritten with null in Redis. Pair with `--toast-cache` on the WAL detector to resolve TOAST columns before they reach the adapter, eliminating the extra GET entirely.

## embedding

Sync to pgvector table via OpenAI-compatible API.

```bash
pgcdc listen -c pgcdc:articles -a embedding \
  --embedding-api-url https://api.openai.com/v1/embeddings \
  --embedding-api-key $OPENAI_API_KEY \
  --embedding-columns title,body --db postgres://...
```

Works with OpenAI, Azure OpenAI, Ollama, vLLM, LiteLLM.

## iceberg

Write events to Apache Iceberg tables (Parquet files).

```bash
pgcdc listen -c pgcdc:orders -a iceberg \
  --iceberg-warehouse /tmp/iceberg --iceberg-table orders --db postgres://...
```
