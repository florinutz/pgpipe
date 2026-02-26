# CDC-Powered RAG: Real-Time Embeddings Without the Infrastructure

Retrieval-Augmented Generation (RAG) pipelines typically require a separate vector database, an embedding service, a batch ETL process, and monitoring to keep everything in sync. pgcdc's embedding adapter reduces this to a single process: database changes are automatically embedded and stored alongside your data in pgvector.

This post covers how the CDC-to-embedding pipeline works, the architecture choices, and why "zero infrastructure" makes this practical for teams that do not want to operate Pinecone or Weaviate.

## The Sync Problem

Every RAG system has a freshness problem. Your application writes to PostgreSQL. Your search/chat system reads from a vector store. Something needs to keep them in sync.

The common approaches:

1. **Batch ETL**: A cron job periodically scans for changes and embeds them. Simple but introduces latency (minutes to hours) and misses deletes/updates between scans.
2. **Application-level dual write**: The application writes to both PostgreSQL and the vector store. Introduces coupling, consistency risks, and embedding API calls in the request path.
3. **CDC streaming**: Capture changes from the database log and process them asynchronously. Real-time, decoupled, and handles all operation types.

pgcdc's embedding adapter implements option 3 with minimal moving parts.

## Architecture

The pipeline flows through pgcdc's standard architecture:

```
PostgreSQL WAL
    |
    v
WAL Detector (pglogrepl)
    |
    v
Bus (fan-out)
    |
    v
Embedding Adapter
    |
    +---> OpenAI-compatible API (embed text)
    |
    +---> pgvector table (UPSERT/DELETE vector)
```

A single pgcdc process handles detection, embedding, and storage. The embedding adapter (`adapter/embedding/embedding.go`) processes events from its bus subscription:

- **INSERT/UPDATE**: Extract configured text columns, call the embedding API, UPSERT the vector into pgvector.
- **DELETE**: Delete the corresponding vector row.

## Configuration

```sh
pgcdc listen \
  --db postgres://localhost/myapp \
  --detector wal \
  --all-tables \
  --adapter embedding \
  --embedding-api-url https://api.openai.com/v1/embeddings \
  --embedding-api-key $OPENAI_API_KEY \
  --embedding-columns title,description,content \
  --embedding-db postgres://localhost/myapp \
  --embedding-table product_embeddings \
  --embedding-id-column id \
  --embedding-model text-embedding-3-small \
  --embedding-dimension 1536 \
  --route embedding=pgcdc:products
```

Key parameters:

- `--embedding-columns`: Which payload fields to concatenate for embedding. The adapter joins them with spaces.
- `--embedding-id-column`: Which payload field serves as the source row identifier for UPSERT/DELETE matching.
- `--route embedding=pgcdc:products`: Only embed events from the products table (skip unrelated tables).

## The Embedding Call

The adapter calls any OpenAI-compatible embedding endpoint. This includes OpenAI, Azure OpenAI, local models via Ollama or vLLM, or any service implementing the `/v1/embeddings` contract:

```json
POST /v1/embeddings
{
  "model": "text-embedding-3-small",
  "input": "Wireless Headphones Premium noise-cancelling headphones with 30-hour battery life"
}
```

The response contains the embedding vector:

```json
{
  "data": [{"embedding": [0.0023, -0.009, ...]}],
  "usage": {"total_tokens": 12}
}
```

The adapter retries on network errors, HTTP 429 (rate limit), and 5xx responses with exponential backoff (default 3 retries, 2s base, 60s cap). Non-retryable 4xx errors (e.g., 400 Bad Request for invalid input) are skipped. After retry exhaustion, the event goes to the dead letter queue.

## pgvector Storage

The vector is stored directly in PostgreSQL using the pgvector extension. No separate vector database needed. The adapter UPSERTs with a `::vector` cast:

```sql
INSERT INTO product_embeddings (source_id, source_table, content, embedding, event_id, updated_at)
VALUES ($1, $2, $3, $4::vector, $5, NOW())
ON CONFLICT (source_id) DO UPDATE SET
  content = EXCLUDED.content,
  embedding = EXCLUDED.embedding,
  event_id = EXCLUDED.event_id,
  updated_at = NOW()
```

The `source_id` column has a unique constraint, making UPSERT idempotent. If pgcdc processes the same event twice (e.g., after a restart), the result is identical.

For DELETE operations:

```sql
DELETE FROM product_embeddings WHERE source_id = $1
```

The vector format is pgvector's string representation: `[0.0023,-0.009,...]`. The adapter builds this without importing pgvector as a dependency -- it is just a string with a cast.

## Zero New Dependencies

A deliberate design choice: the embedding adapter adds zero new Go dependencies. It uses:

- `net/http` for the embedding API call.
- `pgx/v5` (already a pgcdc dependency) for the database connection.
- Standard `encoding/json` for payload parsing.

Vectors are formatted as strings and cast via `::vector` in SQL. This avoids pulling in a pgvector Go client or any vector-specific library.

## Change Significance Filtering

Not every UPDATE needs re-embedding. If a user changes their email address, re-embedding their profile description is wasted work and wasted API cost.

The adapter supports `--embedding-skip-unchanged` mode. When enabled, UPDATE events are checked: if none of the configured embedding columns (`--embedding-columns`) differ between the old and new row values, the embedding API call is skipped entirely.

```go
func (a *Adapter) embeddingColumnsChanged(p payload) bool {
    if !a.skipUnchanged {
        return true
    }
    if p.Op != "UPDATE" {
        return true
    }
    if p.Old == nil {
        return true // no old values to compare
    }
    for _, col := range a.columns {
        newVal := fmt.Sprintf("%v", p.Row[col])
        oldVal := fmt.Sprintf("%v", p.Old[col])
        if newVal != oldVal {
            return true
        }
    }
    return false
}
```

This requires REPLICA IDENTITY FULL on the source table (or at least the embedding columns in the replica identity) so that the old row values are available in the WAL event. With the default REPLICA IDENTITY, old values are only available for primary key columns.

## Connection Resilience

The adapter maintains a persistent connection to the pgvector database. If the connection drops:

1. The current event's DB operation fails.
2. The adapter detects the closed connection via `conn.IsClosed()`.
3. It returns from the inner `run` loop, triggering a reconnect with exponential backoff.
4. Once reconnected, event processing resumes.

The embedding API connection is per-request (standard HTTP), so API resilience is handled entirely by the per-event retry logic.

## DLQ Integration

When an embedding API call exhausts its retries, the event is recorded to the dead letter queue:

```go
if a.dlq != nil {
    if dlqErr := a.dlq.Record(ctx, ev, "embedding", err); dlqErr != nil {
        a.logger.Error("dlq record failed", "error", dlqErr)
    }
}
```

DLQ records can be inspected via `pgcdc dlq list --adapter embedding` and replayed via `pgcdc dlq replay`. This provides a safety net for transient API outages: events that failed during the outage can be re-processed once the API is healthy.

## Startup Validation

Before starting the pipeline, the adapter validates both external dependencies:

1. **API reachability**: HTTP GET to the base URL (without `/embeddings`) with the API key.
2. **DB connectivity**: `pgx.Connect` to the pgvector database URL.

If either check fails, the pipeline aborts with a `ValidationError` before processing any events. This catches misconfigured API keys, wrong database URLs, and network issues at startup rather than at the first event.

## Observability

The adapter exposes several Prometheus metrics:

- `pgcdc_events_delivered_total{adapter="embedding"}`: Successfully embedded events.
- `pgcdc_embedding_api_requests_total`: Total API calls (includes retries).
- `pgcdc_embedding_api_errors_total`: Failed API calls.
- `pgcdc_embedding_api_duration_seconds`: API call latency histogram.
- `pgcdc_embedding_tokens_used_total`: Total tokens consumed (from API response).
- `pgcdc_dlq_records_total{adapter="embedding"}`: Events sent to DLQ.

These metrics make it straightforward to monitor API cost (tokens), latency, and error rates.

## Practical Deployment

A minimal deployment for a product search RAG system:

```yaml
# pgcdc.yaml
adapters:
  - embedding

embedding:
  api_url: https://api.openai.com/v1/embeddings
  api_key: ${OPENAI_API_KEY}
  model: text-embedding-3-small
  columns: [name, description, features]
  id_column: id
  db: postgres://localhost/myapp
  table: product_vectors
  dimension: 1536
  skip_unchanged: true

routes:
  embedding: [pgcdc:products]
```

The pgvector table:

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE product_vectors (
  source_id TEXT PRIMARY KEY,
  source_table TEXT,
  content TEXT,
  embedding vector(1536),
  event_id TEXT,
  updated_at TIMESTAMPTZ
);

CREATE INDEX ON product_vectors USING ivfflat (embedding vector_cosine_ops);
```

Your RAG query:

```sql
SELECT p.id, p.name, p.description,
       1 - (pv.embedding <=> $1::vector) AS similarity
FROM products p
JOIN product_vectors pv ON pv.source_id = p.id::text
ORDER BY pv.embedding <=> $1::vector
LIMIT 10;
```

The entire stack is PostgreSQL + pgcdc + an embedding API. No Pinecone, no Weaviate, no Redis, no Elasticsearch. Your vectors live in the same database as your data, queryable with standard SQL joins.

## Trade-Offs

This approach works well when:

- Your dataset fits in pgvector (millions of rows with IVFFlat, hundreds of millions with HNSW).
- You want real-time sync without operating a separate vector database.
- Your query patterns involve joining vectors with relational data.
- You prefer PostgreSQL's operational model (backup, replication, monitoring).

It is less suitable when:

- You need specialized vector database features (multi-tenancy, automatic index tuning, managed sharding).
- Your embedding dimension or dataset size exceeds pgvector's practical limits.
- You need to query vectors without joining relational data.

For many teams building RAG features, the simplicity of keeping everything in PostgreSQL outweighs the advanced features of dedicated vector databases. pgcdc's embedding adapter makes the CDC-to-vector pipeline automatic and real-time.
