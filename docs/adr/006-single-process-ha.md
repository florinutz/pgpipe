# ADR-006: Single-Process High Availability

## Status

Accepted

## Context

pgcdc is a single-binary CDC tool that streams changes from PostgreSQL, MySQL, and MongoDB to various sinks. Users deploying pgcdc in production need to understand its availability characteristics and failure recovery model.

The core question: how does pgcdc handle crashes, restarts, and scaling?

### Constraints

- pgcdc uses PostgreSQL logical replication slots, which are inherently single-consumer. Only one connection can consume from a slot at a time.
- LISTEN/NOTIFY channels are per-connection. Multiple listeners receive duplicate events.
- MySQL binlog replication uses a server ID that must be unique per replica.
- MongoDB Change Streams use resume tokens that are per-cursor.

## Decision

pgcdc uses a **single-process architecture with persistent replication slots and LSN checkpointing** for crash recovery. It does not implement distributed coordination or multi-process consensus.

### How It Works

1. **Persistent slot**: `--persistent-slot` creates a named, non-temporary replication slot. The slot survives both pgcdc and PostgreSQL restarts.
2. **LSN checkpointing**: The last-processed WAL position is persisted to `pgcdc_checkpoints` table. On restart, pgcdc resumes from the checkpoint.
3. **Cooperative checkpointing**: `--cooperative-checkpoint` ensures the checkpoint only advances to the minimum LSN acknowledged by all adapters, preventing data loss when adapters process at different speeds.
4. **Heartbeat**: `--heartbeat-interval` periodically writes to a heartbeat table to keep the replication slot advancing on idle databases, preventing WAL bloat.

### Deployment Model

```
                    ┌──────────────────────┐
                    │    PostgreSQL         │
                    │  ┌────────────────┐   │
                    │  │ Replication    │   │
                    │  │ Slot: pgcdc_slot│  │
                    │  └───────┬────────┘   │
                    └──────────┼────────────┘
                               │
                    ┌──────────┴────────────┐
                    │      pgcdc (active)    │
                    │  checkpoint table      │
                    │  ┌──────────────────┐  │
                    │  │ LSN: 0/1A3B4C0  │  │
                    │  └──────────────────┘  │
                    └───────────────────────┘
```

On crash, the process restarts (via systemd, Kubernetes, or Docker restart policy) and resumes from the checkpointed LSN.

## Options Considered

### Option 1: Active-Passive with Distributed Lock (Rejected)

Use etcd, ZooKeeper, or a Kubernetes lease to elect an active instance. Standby instances wait for the lock.

**Pros**:
- Near-zero recovery time (standby takes over immediately).
- Standard HA pattern.

**Cons**:
- Adds operational dependency (etcd/ZooKeeper cluster or Kubernetes API).
- Replication slots are single-consumer; the standby cannot "warm up" its slot.
- Complexity: lock expiry, split-brain prevention, checkpoint synchronization.
- The recovery time for a single-process restart (seconds) is comparable to leader election failover.

### Option 2: Partition-Aware Horizontal Scaling (Rejected)

Shard tables across multiple pgcdc instances, each with its own replication slot and publication.

**Pros**:
- Horizontal throughput scaling.
- Independent failure domains per shard.

**Cons**:
- Requires explicit table partitioning at the PostgreSQL level or publication management.
- Cross-table ordering guarantees are lost.
- Dramatically increases operational complexity.
- Most CDC workloads are write-limited by the source database, not the CDC consumer.

### Option 3: Sidecar Model (Rejected)

Run pgcdc as a sidecar container alongside the application, consuming from localhost PostgreSQL.

**Pros**:
- Co-located with the database, minimal network latency.
- Lifecycle tied to the application pod.

**Cons**:
- Not applicable for centralized CDC (multiple databases to one pgcdc).
- Complicates resource allocation (shared CPU/memory with application).
- Does not address HA; sidecar dies with the pod.

### Option 4: Single-Process with Persistent Slots (Chosen)

Let pgcdc be a simple, restartable process. Use PostgreSQL's built-in replication slot persistence and a checkpoint table for crash recovery.

**Pros**:
- Zero additional infrastructure dependencies.
- Simple operational model: one process, one configuration file.
- Crash recovery is built into PostgreSQL (slot persistence) and pgcdc (checkpoint table).
- Process restart time (1-5 seconds) is acceptable for most CDC workloads.
- Cooperative checkpointing prevents data loss even with slow adapters.
- Backpressure controller prevents WAL bloat during sink outages.

**Cons**:
- Not horizontally scalable for write throughput (single slot = single consumer).
- Recovery time is process restart time (typically 1-5 seconds, depending on systemd/k8s).
- No standby instance for instant failover.

## Consequences

### Positive

- **Simple**: One binary, one config file, one database connection. No distributed coordination.
- **Crash-resilient**: Persistent slot + checkpoint table = no data loss on restart (at-least-once with WAL detector).
- **Observable**: Single process means all metrics, logs, and health checks are co-located.
- **Predictable**: No split-brain, no leader election race conditions, no distributed state to debug.

### Negative

- **Not horizontally scalable for writes**: A single replication slot can only be consumed by one connection. If CDC throughput exceeds what one process can handle, the answer is to optimize the process (faster adapters, bus tuning) rather than adding instances.
- **Recovery gap**: During restart, events accumulate in the WAL. The persistent slot prevents WAL recycling, and the checkpoint ensures no events are skipped. The gap is the restart duration, typically 1-5 seconds.
- **WAL retention risk**: If pgcdc is down for an extended period, the persistent slot prevents WAL cleanup, which can fill the disk. Mitigation: `--heartbeat-interval`, `--slot-lag-warn`, and external monitoring of `pgcdc_slot_lag_bytes`.

### Operational Recommendations

1. **Use a process manager**: systemd (`Restart=always`), Kubernetes (`restartPolicy: Always`), or Docker (`restart: unless-stopped`).
2. **Enable persistent slot**: `--persistent-slot` is required for crash-resilient operation.
3. **Enable cooperative checkpoint**: `--cooperative-checkpoint` prevents the checkpoint from advancing past what slow adapters have processed.
4. **Monitor slot lag**: Alert on `pgcdc_slot_lag_bytes` exceeding a threshold. Use `--slot-lag-warn` for log warnings.
5. **Set heartbeat interval**: `--heartbeat-interval 30s` keeps the slot advancing on idle databases.
6. **Configure backpressure**: `--backpressure` with appropriate thresholds prevents WAL exhaustion during sink outages.
7. **Limit WAL retention**: Set `max_slot_wal_keep_size` in PostgreSQL (PG 13+) as a safety net against unbounded WAL retention.
