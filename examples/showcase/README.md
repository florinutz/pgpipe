# pgcdc Showcase

A Docker Compose demo that shows pgcdc streaming PostgreSQL changes to multiple outputs simultaneously: stdout, SSE (live dashboard), S3 (MinIO), a Kafka-compatible server, and a streaming SQL view.

## Prerequisites

- Docker and Docker Compose (v2)

## Quick Start

```sh
docker compose up --build
```

Then open **http://localhost:3000** in your browser.

## Services

| Service | Description | Port |
|---|---|---|
| **postgres** | PostgreSQL 16 with `wal_level=logical` and sample tables | 5432 |
| **pgcdc** | CDC pipeline: WAL replication to stdout + SSE + S3 + kafkaserver + view | 8080 (SSE), 9092 (Kafka), 9090 (metrics) |
| **minio** | S3-compatible object storage | 9000 (API), 9001 (console) |
| **minio-init** | One-shot: creates the `pgcdc-events` bucket | -- |
| **dashboard** | Nginx serving the live SSE dashboard | 3000 |
| **datagen** | Inserts random orders and customers every 2 seconds | -- |
| **kafka-consumer** | Consumes from pgcdc's Kafka-compatible server using kcat | -- |

## Observing Events

### Live Dashboard (SSE)

Open **http://localhost:3000**. The left panel shows CDC events in real time. The right panel shows streaming SQL view results (order counts per channel, tumbling 30-second windows).

### Stdout Logs

```sh
docker compose logs -f pgcdc
```

### S3 / MinIO Console

Open **http://localhost:9001** (login: `minioadmin` / `minioadmin`). Browse the `pgcdc-events` bucket to see Hive-partitioned JSON Lines files flushed every 10 seconds.

### Kafka Consumer

```sh
docker compose logs -f kafka-consumer
```

Shows events consumed from pgcdc's built-in Kafka protocol server. Any Kafka client library can connect to `localhost:9092`.

### Streaming SQL View

The `order_counts` view runs:

```sql
SELECT channel, COUNT(*) as total
FROM pgcdc_events
GROUP BY channel
TUMBLING WINDOW 30s
```

Results appear on the dashboard's right panel and in pgcdc's stdout as `VIEW_RESULT` events on `pgcdc:_view:order_counts`.

### Prometheus Metrics

```sh
curl http://localhost:9090/metrics
```

### Health Check

```sh
curl http://localhost:9090/healthz
```

## Cleanup

```sh
docker compose down -v
```
