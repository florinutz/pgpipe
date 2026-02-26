# Building a Kafka Wire Protocol Server in Go

pgcdc includes a built-in Kafka protocol server that speaks the Kafka wire protocol directly. Any Kafka consumer library -- librdkafka, franz-go, sarama, the Java client, confluent-kafka-python -- connects to pgcdc and consumes CDC events as if it were a Kafka cluster. No Kafka infrastructure needed.

This post covers why we built it, how the wire protocol works, and the engineering challenges we encountered.

## Why Build a Kafka Protocol Server?

CDC pipelines commonly feed into Kafka, but Kafka is significant infrastructure. A ZooKeeper or KRaft cluster, broker provisioning, topic management, monitoring, upgrades -- it is a lot of operational overhead when all you want is "stream database changes to my application."

Many teams already have Kafka consumer code. Their services use librdkafka or the Confluent client. Asking them to rewrite consumers for a custom protocol is a non-starter.

The insight: if pgcdc speaks the Kafka wire protocol, existing consumer code works unchanged. The application connects to `pgcdc:9092` instead of `kafka:9092`. CDC events appear as Kafka records. No Kafka cluster to manage.

This is not a replacement for Kafka in general. It is a replacement for the specific pattern of "PostgreSQL -> Kafka -> consumers" when the only producer is CDC.

## Wire Protocol Fundamentals

The Kafka protocol is TCP-based with length-prefixed request/response framing. Every request starts with:

```
[4 bytes: request length (int32, big-endian)]
[2 bytes: API key (int16)]
[2 bytes: API version (int16)]
[4 bytes: correlation ID (int32)]
[variable: client ID (nullable string)]
```

The response mirrors the correlation ID so the client can match responses to requests on a multiplexed connection.

pgcdc implements 11 API keys:

| API Key | Name | Version Range |
|---------|------|---------------|
| 18 | ApiVersions | 0-3 |
| 3 | Metadata | 0 |
| 10 | FindCoordinator | 0 |
| 11 | JoinGroup | 0 |
| 14 | SyncGroup | 0 |
| 12 | Heartbeat | 0 |
| 13 | LeaveGroup | 0 |
| 2 | ListOffsets | 1 |
| 1 | Fetch | 0 |
| 8 | OffsetCommit | 2 |
| 9 | OffsetFetch | 0 |

This minimal set is sufficient for consumer group lifecycle, partition assignment, offset management, and data consumption.

## Flexible Encoding: The ApiVersions Challenge

The most interesting wire format challenge is ApiVersions v3+. Kafka introduced "flexible encoding" in KIP-482, which changes the serialization format for newer API versions.

In non-flexible encoding, arrays are prefixed with an int32 length and strings with an int16 length. In flexible encoding, arrays and strings use unsigned varint (compact) lengths, and every struct has a trailing "tagged fields" section (also varint-prefixed).

The catch: ApiVersions itself is the mechanism clients use to discover which API versions a broker supports. A client sends ApiVersions v3 before it knows anything about the broker. So the request header for ApiVersions v3+ uses the flexible format (compact nullable string for client ID, tagged fields), while all other API keys at version 0 use the non-flexible format.

Our `readRequest` function handles this:

```go
if hdr.APIKey == apiApiVersions && hdr.APIVersion >= 3 {
    // Flexible header: compact_nullable_string + tagged_fields
    n, bytesRead := readUvarintBuf(buf[off:])
    // ...
} else {
    // Standard header: nullable string (int16 length)
    clientIDLen := int16(binary.BigEndian.Uint16(buf[off:off+2]))
    // ...
}
```

The response encoding mirrors this: ApiVersions v3+ responses use compact arrays and tagged field buffers. Lower versions use standard int32 array lengths.

## Topic Registry and Partition Hashing

pgcdc channels map to Kafka topics using a straightforward convention: `pgcdc:orders` becomes `pgcdc.orders`. Topics are created lazily when the first event arrives for a channel.

Each topic has N partitions (configurable, default 8). Events are assigned to partitions using FNV-1a hashing on a key extracted from the JSON payload:

```go
func extractKey(payload json.RawMessage, keyColumn, fallback string) string {
    var obj map[string]json.RawMessage
    if err := json.Unmarshal(payload, &obj); err != nil {
        return fallback
    }
    raw, ok := obj[keyColumn]
    if !ok {
        return fallback
    }
    var s string
    if err := json.Unmarshal(raw, &s); err == nil {
        return s
    }
    return string(raw)
}

func partitionForKey(key string, partitionCount int) int32 {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int32(h.Sum32() % uint32(partitionCount))
}
```

The key column defaults to `id`, so events for the same entity consistently land in the same partition, preserving per-entity ordering.

## Ring Buffer Partitions

Each partition is a fixed-size ring buffer with monotonically increasing offsets. This is the core data structure that makes the system work without external storage.

```go
type partition struct {
    mu         sync.Mutex
    ring       []record
    capacity   int
    nextOffset int64
    size       int
    waiters    []chan struct{}
}
```

Records are stored at `ring[offset % capacity]`. When the buffer is full, the oldest records are silently overwritten. Consumers that fall behind receive `OFFSET_OUT_OF_RANGE`, which is standard Kafka behavior for expired segments.

The default capacity is 10,000 records per partition, which is typically sufficient for real-time consumers. This is not a durable log -- it is a bounded buffer optimized for the common case where consumers keep up.

## Long-Poll Fetch

The Fetch API is where consumers actually receive data. A naive implementation would return immediately with whatever is available, but that creates a busy-wait loop when the partition is idle.

Instead, we implement long-poll: when a partition has no new data at the requested offset, the Fetch handler registers a waiter channel on the partition. When new records are appended, all waiters are woken:

```go
func (p *partition) append(rec record) int64 {
    p.mu.Lock()
    // ... store record ...
    waiters := p.waiters
    p.waiters = nil
    p.mu.Unlock()

    for _, ch := range waiters {
        close(ch)
    }
    return offset
}

func (p *partition) waiter() <-chan struct{} {
    p.mu.Lock()
    defer p.mu.Unlock()
    ch := make(chan struct{})
    p.waiters = append(p.waiters, ch)
    return ch
}
```

The Fetch handler uses a `select` with the waiter channel, a timeout, and the context cancellation. This gives sub-millisecond latency for new data while keeping idle connections quiet.

## Consumer Group State Machine

Consumer groups are the most complex part of the Kafka protocol. pgcdc implements a simplified version of the state machine with four states:

```
Empty -> PreparingRebalance -> CompletingRebalance -> Stable
```

**JoinGroup**: Members join the group and the first member becomes the leader. The leader receives the member list and is responsible for partition assignment.

**SyncGroup**: The leader sends partition assignments for all members. Non-leader members wait (blocking on a channel) until the leader's SyncGroup arrives.

**Heartbeat**: Members periodically heartbeat to indicate liveness. If a member misses its session timeout, it is removed and a rebalance is triggered.

**Session reaping**: A background goroutine periodically checks member heartbeat timestamps and removes expired members:

```go
func (gc *groupCoordinator) startReaper(done chan struct{}) {
    go func() {
        ticker := time.NewTicker(gc.sessionTimeout / 3)
        defer ticker.Stop()
        for {
            select {
            case <-done:
                return
            case <-ticker.C:
                gc.reapExpired()
            }
        }
    }()
}
```

## RecordBatch v2 Encoding

Kafka Fetch responses use the RecordBatch v2 format (introduced in KIP-98). This is a non-trivial binary format:

1. **Batch header** (61 bytes): base offset, batch length, partition leader epoch, magic byte (2), CRC32C, attributes, first/last offset delta, first/last timestamps, producer ID/epoch, base sequence, record count.
2. **Records**: each record uses varint-encoded lengths for key, value, and headers.

The CRC32C checksum covers everything from attributes through the last record. Our encoder computes it over the already-serialized bytes:

```go
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// After serializing the batch body:
crc := crc32.Checksum(body, crc32cTable)
```

Each record header (like `pgcdc-channel`, `pgcdc-operation`, `pgcdc-event-id`) is encoded as a varint-prefixed key-value pair within the record.

## Offset Persistence

Consumer group offsets are persisted using pgcdc's existing `checkpoint.Store` interface. The key format is `kafka:{group}:{topic}:{partition}`, with the offset stored as a uint64.

This means offset state survives pgcdc restarts. When a consumer group reconnects, it can resume from its committed offset (assuming the ring buffer still contains that data).

## What We Chose Not to Implement

Several Kafka features are intentionally absent:

- **Produce API**: pgcdc is a CDC consumer, not a general-purpose message broker. Events flow from the database through the bus.
- **Transactions/idempotency on the consumer side**: The ring buffer is in-memory. Consumer-side exactly-once would require persistent storage, which defeats the zero-infrastructure goal.
- **Replication**: There is one "broker" (pgcdc itself). No leader election or replica synchronization.
- **ACLs**: Authentication and authorization are out of scope for the initial implementation.
- **Log compaction**: The ring buffer is a fixed window, not a compacted log.

These constraints are acceptable for the target use case: consuming CDC events in real-time with existing Kafka client libraries.

## Performance Characteristics

The kafkaserver adapter adds minimal overhead to the CDC pipeline:

- **Memory**: N partitions x buffer_size records. Default: 8 x 10,000 = 80,000 records.
- **Latency**: Sub-millisecond from event ingestion to Fetch response (long-poll wake latency).
- **Throughput**: Bounded by TCP I/O and JSON payload size. Typical CDC workloads are well within limits.
- **Connection handling**: Each client gets a goroutine. Buffered read/write (64KB) reduces syscall overhead.

## Conclusion

Building a Kafka wire protocol server was a deliberate trade-off: significant implementation complexity (binary protocol encoding, consumer group state machine, RecordBatch v2 format) in exchange for zero-infrastructure CDC consumption using any Kafka client library.

The ring buffer approach means this is not durable storage. It is a real-time bridge from database changes to Kafka consumers, eliminating the operational burden of a full Kafka cluster when CDC is the only producer.
