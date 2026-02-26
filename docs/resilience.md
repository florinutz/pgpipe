# Resilience

This document describes pgcdc's failure modes, recovery mechanisms, and the guarantees they provide. Claims are verified against scenario tests in `scenarios/`.

## PostgreSQL Restarts

**Behavior**: When the PostgreSQL connection drops, the detector logs an error and returns from its `Start` method. The errgroup context cancellation tears down the entire pipeline. If `--persistent-slot` is enabled, the replication slot survives the PG restart, and pgcdc can reconnect without data loss from the WAL.

**Scenario coverage**: `scenarios/cooperative_checkpoint_test.go` tests persistent slot + checkpoint behavior. The LISTEN/NOTIFY detector relies on pgx reconnection.

**Recovery path**:
1. Detector detects connection loss (read error on replication stream or LISTEN connection).
2. Detector returns error, cancelling the errgroup context.
3. All adapters receive context cancellation and shut down.
4. Adapters implementing `Drainer` flush in-flight work within `shutdown_timeout`.
5. On restart, pgcdc reads the last checkpoint LSN and resumes from there.

**Key constraint**: The LISTEN/NOTIFY detector uses a dedicated connection (not a pool) because LISTEN state is per-connection. Connection loss means all LISTEN subscriptions are lost.

## Sink Failures

### Webhook Delivery Failure

**Behavior**: Retries with exponential backoff and full jitter (default 5 retries, 1s base, 32s cap). Non-retryable 4xx responses are skipped. After retry exhaustion, the event is sent to the DLQ.

**Scenario coverage**: `scenarios/dlq_test.go` tests webhook delivery failure and DLQ recording.

**Recovery path**:
1. HTTP POST fails or returns 5xx/429.
2. Adapter retries with increasing backoff.
3. After `maxRetries` exhausted: event recorded to DLQ (stderr or pg_table).
4. Adapter acks the event and continues to the next one.
5. DLQ records can be inspected via `pgcdc dlq list` and replayed via `pgcdc dlq replay`.

### Kafka Terminal Errors

**Behavior**: Non-retriable Kafka errors (e.g., authorization denied, message too large) are detected via `kerr.Error.Retriable`. Terminal errors send the event to DLQ. Retriable errors (e.g., broker unavailable) trigger reconnection with backoff.

**Scenario coverage**: `scenarios/kafka_test.go` tests Kafka publish and reconnection.

### Database Sink Failures

**Behavior**: The pg_table, embedding, and redis adapters reconnect with exponential backoff on connection loss. Non-connection errors (e.g., constraint violations for pg_table) log a warning and skip the event.

**Scenario coverage**: `scenarios/embedding_test.go`, `scenarios/multi_adapter_test.go`.

### S3 Flush Failures

**Behavior**: Failed S3 uploads put events back into the buffer for retry (all-or-nothing per flush). After 10 consecutive failures, the adapter triggers a reconnect cycle with backoff.

**Scenario coverage**: None specific to S3 flush failure (S3 adapter tested via MinIO in integration).

## WAL Accumulation

When adapters process events slower than the WAL produces them, WAL segments accumulate and can exhaust PostgreSQL disk space.

### Bus-Level Backpressure

**Fast mode** (default): Events are dropped for slow adapters. The detector is never blocked, but data loss occurs for those adapters.

**Reliable mode**: The bus blocks when subscriber channels are full, which back-pressures the detector. No data loss, but throughput is limited by the slowest adapter.

### Source-Aware Backpressure

When `--backpressure` is enabled with `--persistent-slot` and `--detector wal`, the backpressure controller monitors WAL lag and responds with three zones:

| Zone | Trigger | Action |
|------|---------|--------|
| **Green** | lag < `--bp-warn-threshold` (500MB) | Full speed, no action |
| **Yellow** | lag >= warn, lag < critical | Proportional throttle on detector, shed best-effort adapters |
| **Red** | lag >= `--bp-critical-threshold` (2GB) | Pause detector, shed normal + best-effort adapters |

**Hysteresis**: Red zone exits only when lag drops below warn threshold, preventing oscillation.

**Scenario coverage**: `scenarios/backpressure_test.go` tests zone transitions and adapter shedding.

**Load shedding**: Shed adapters have their events auto-acked without delivery. The cooperative checkpoint still advances because shed events are acked. Critical adapters are never shed.

## Adapter Panics

**Behavior**: All adapter goroutines are wrapped in `Pipeline.safeGo()`, which uses `defer/recover` to catch panics. Recovered panics are:
1. Logged with full stack trace via `slog.Error`.
2. Counted in `pgcdc_panics_recovered_total{component}` Prometheus metric.
3. Returned as errors, which cancels the errgroup and triggers pipeline shutdown.

**Code path**: `pgcdc.go:safeGo()` wraps every `errgroup.Go` call:

```go
func (p *Pipeline) safeGo(g *errgroup.Group, name string, fn func() error) {
    g.Go(func() (err error) {
        defer func() {
            if r := recover(); r != nil {
                metrics.PanicsRecovered.WithLabelValues(name).Inc()
                p.logger.Error("panic recovered", "component", name, "panic", r, "stack", string(debug.Stack()))
                err = fmt.Errorf("panic in %s: %v", name, r)
            }
        }()
        return fn()
    })
}
```

**Design choice**: A panic in any component shuts down the entire pipeline rather than attempting partial recovery. This is intentional: partial pipeline state is harder to reason about than a clean restart with persistent slot recovery.

## Circuit Breaker

The circuit breaker (`internal/circuitbreaker/circuitbreaker.go`) implements the three-state pattern:

```
Closed (normal) → Open (failures >= max) → Half-Open (after reset timeout) → Closed (success) or Open (failure)
```

Configuration is available via CLI flags (`--webhook-cb-failures`, `--webhook-cb-reset`, `--embedding-cb-failures`, `--embedding-cb-reset`), but the circuit breaker is **not yet wired** into adapter delivery loops. It exists as a library ready for integration.

## Rate Limiter

The rate limiter (`internal/ratelimit/ratelimit.go`) provides token-bucket rate limiting with Prometheus metrics. Configuration via `--webhook-rate-limit`, `--webhook-rate-burst`, `--embedding-rate-limit`, `--embedding-rate-burst`. Like the circuit breaker, it is **not yet wired** into adapter delivery loops.

## Graceful Shutdown

On SIGINT/SIGTERM:

1. Root context is cancelled.
2. Detector stops producing events.
3. Bus closes all subscriber channels (adapters see channel close).
4. Errgroup waits for all goroutines to return.
5. `Pipeline.drainAdapters()` calls `Drain(ctx)` on adapters implementing `adapter.Drainer`, bounded by `shutdown_timeout` (default 5s).
6. Checkpoint store is closed.

Adapters with drain support:
- **webhook**: waits for in-flight HTTP requests (`sync.WaitGroup`).
- **kafka**: franz-go client handles its own flushing on context cancel.
- **s3**: flushes remaining buffered events to S3.

## Startup Validation

When `--skip-validation` is not set, `Pipeline.Validate()` calls `Validate(ctx)` on all adapters implementing `adapter.Validator` before starting the pipeline. This catches configuration errors early:

| Adapter | Validation Check |
|---------|-----------------|
| webhook | DNS resolution of target host |
| kafka | Broker connectivity via Metadata request |
| nats | NATS server connectivity |
| embedding | API URL reachability + pgvector DB connectivity |
| search | Search engine health endpoint |
| redis | Redis PING |
| s3 | HeadBucket on target bucket |

Validation failures abort startup with a `ValidationError` containing the adapter name and underlying error.

**Scenario coverage**: `scenarios/cli_validation_test.go` tests the `pgcdc validate` command.

## Dead Letter Queue Management

Failed events are captured to the DLQ (stderr JSON lines or `pgcdc_dead_letters` PostgreSQL table). The CLI provides management commands:

- `pgcdc dlq list`: List DLQ records with optional filters (adapter, time range, ID).
- `pgcdc dlq replay`: Replay DLQ records to their original adapter destinations. Supports `--dry-run` and adapter-specific overrides.
- `pgcdc dlq purge`: Delete DLQ records matching filters.

**Scenario coverage**: `scenarios/dlq_test.go` tests DLQ recording and retrieval.

## Schema Migration Safety

The migration system (`internal/migrate/migrate.go`) uses a `pgcdc_migrations` version table with sequential version tracking. Migrations are embedded SQL files applied at startup. The `--skip-migrations` flag allows skipping for read-only or restricted database access.

## Persistent Slot + Checkpointing

With `--persistent-slot`:
1. A named, non-temporary replication slot is created (default `pgcdc_slot`).
2. LSN checkpoints are persisted to `pgcdc_checkpoints` table.
3. On restart, pgcdc resumes from the checkpointed LSN.
4. The slot survives pgcdc crashes and PostgreSQL restarts.

The `--checkpoint-db` flag allows storing checkpoints in a separate database from the source. The `--slot-name` flag allows custom slot naming for multi-instance deployments.

**Scenario coverage**: `scenarios/cooperative_checkpoint_test.go` tests checkpoint persistence and resumption.
