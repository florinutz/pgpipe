# Real-Time Search Sync from PostgreSQL

Keep Typesense or Meilisearch automatically synchronized with your PostgreSQL tables.

## Architecture

```
PostgreSQL (orders table)
  │
  ├── INSERT/UPDATE ──▶ pgcdc ──▶ Typesense (upsert document)
  └── DELETE ──────────▶ pgcdc ──▶ Typesense (delete document)
```

## Typesense Setup

### 1. Start Typesense

```bash
docker run -d -p 8108:8108 \
  -e TYPESENSE_API_KEY=xyz \
  typesense/typesense:latest
```

### 2. Create a collection

```bash
curl -X POST http://localhost:8108/collections -H "X-TYPESENSE-API-KEY: xyz" -d '{
  "name": "orders",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "data", "type": "object"}
  ]
}'
```

### 3. Stream changes

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  -a search --search-engine typesense \
  --search-url http://localhost:8108 \
  --search-api-key xyz \
  --search-index orders \
  --db postgres://localhost:5432/mydb
```

## Meilisearch Setup

```bash
docker run -d -p 7700:7700 \
  -e MEILI_MASTER_KEY=xyz \
  getmeili/meilisearch:latest

pgcdc listen --detector wal --publication pgcdc_orders \
  -a search --search-engine meilisearch \
  --search-url http://localhost:7700 \
  --search-api-key xyz \
  --search-index orders \
  --db postgres://localhost:5432/mydb
```

## TOAST Columns

PostgreSQL stores large values (>2KB) out-of-line via TOAST. With `REPLICA IDENTITY DEFAULT`, UPDATE events for unchanged TOAST columns carry no data. For search this matters: naively upserting a document with a null `body` field would overwrite a properly indexed value.

pgcdc handles this in two ways:

1. **TOAST cache** (recommended): resolves columns before they reach the adapter — the search adapter sees complete documents and batches them normally.

```bash
pgcdc listen --detector wal --publication pgcdc_articles \
  --toast-cache \
  -a search --search-engine typesense \
  --search-url http://localhost:8108 --search-api-key xyz \
  --search-index articles --db postgres://...
```

2. **Automatic partial update** (no cache): when `_unchanged_toast_columns` is present, the adapter strips those fields and sends the update via PATCH (Typesense) or merge-import (Meilisearch), preserving existing indexed content.

## Routing with Other Adapters

Only send specific channels to search:

```bash
pgcdc listen --detector wal --publication pgcdc_all \
  -a stdout -a search \
  --search-engine typesense --search-url http://localhost:8108 \
  --search-api-key xyz --search-index articles \
  --route search=pgcdc:articles \
  --db postgres://...
```
