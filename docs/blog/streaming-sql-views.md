# Streaming SQL Views Over CDC Events

pgcdc includes a streaming SQL engine that runs continuous analytics over CDC events. You write SQL with window clauses, and pgcdc emits aggregated results as new events on the bus. No external stream processor needed.

This post covers the SQL parsing approach, the three window types, the aggregation internals, and how late events are handled.

## The Problem

CDC produces a stream of individual changes: INSERT, UPDATE, DELETE. Applications often need aggregated views over these changes: "How many orders were placed in the last 5 minutes?" or "What is the average order amount per customer in each 1-hour window?"

The traditional answer is to pipe CDC events into Apache Flink, ksqlDB, or a similar stream processing system. These are powerful tools, but they are also significant infrastructure to operate.

pgcdc takes a different approach: embed the stream processing directly into the CDC pipeline. The view adapter processes events through SQL-defined windows and re-injects the aggregated results back into the bus, where any adapter can consume them.

## SQL Parsing

pgcdc's view engine parses SQL using the TiDB parser, a full MySQL-compatible SQL parser written in Go. The challenge is that streaming SQL requires window clauses that are not part of standard SQL:

```sql
SELECT channel, COUNT(*) as event_count, AVG(payload.amount) as avg_amount
FROM pgcdc_events
WHERE operation = 'INSERT'
GROUP BY channel
HAVING COUNT(*) > 10
TUMBLING WINDOW 5m
```

The window clause (`TUMBLING WINDOW 5m`, `SLIDING WINDOW 1h SLIDE 5m`, `SESSION WINDOW 30m`) is not valid SQL syntax. Neither is `ALLOWED LATENESS 1m`. Our parser handles this in three stages:

**Stage 1: Extract custom clauses via regex.** Before the SQL reaches the TiDB parser, we strip the window and lateness clauses:

```go
var (
    tumblingRe = regexp.MustCompile(`(?i)\s+TUMBLING\s+WINDOW\s+(\S+)\s*$`)
    slidingRe  = regexp.MustCompile(`(?i)\s+SLIDING\s+WINDOW\s+(\S+)\s+SLIDE\s+(\S+)\s*$`)
    sessionRe  = regexp.MustCompile(`(?i)\s+SESSION\s+WINDOW\s+(\S+)\s*$`)
    latenessRe = regexp.MustCompile(`(?i)\s+ALLOWED\s+LATENESS\s+(\S+)`)
)
```

The durations use Go's `time.ParseDuration` format: `5m`, `1h30m`, `500ms`.

**Stage 2: Parse standard SQL with TiDB.** The stripped query is valid SQL that the TiDB parser can handle. We extract the SELECT fields (with aggregate functions), FROM table (must be `pgcdc_events`), WHERE predicate, GROUP BY columns, and HAVING predicate.

**Stage 3: Build the ViewDef.** The parsed AST and extracted window parameters are combined into a `ViewDef` struct that drives the window engine:

```go
type ViewDef struct {
    Name            string
    SelectItems     []SelectItem
    FromTable       string        // must be "pgcdc_events"
    Where           Predicate
    GroupBy         []string
    Having          Predicate
    WindowType      WindowType    // Tumbling, Sliding, or Session
    WindowSize      time.Duration
    SlideSize       time.Duration // for sliding windows
    SessionGap      time.Duration // for session windows
    AllowedLateness time.Duration
    Emit            EmitMode      // row or batch
    MaxGroups       int
}
```

## Aggregate Functions

The view engine supports seven aggregate functions, each implemented as an `Aggregator` interface:

```go
type Aggregator interface {
    Add(v any) bool
    Result() any
    Reset()
}
```

**COUNT**: Counts non-nil values. For `COUNT(*)`, the caller passes a non-nil sentinel. For `COUNT(field)`, nil values are skipped -- matching SQL semantics.

**SUM, AVG, MIN, MAX**: Standard numeric aggregations. Non-numeric values are silently skipped. AVG maintains a running sum and count to avoid a second pass.

**COUNT(DISTINCT)**: Maintains a `map[string]struct{}` of stringified values. Memory usage grows linearly with cardinality, bounded by `max_groups`.

**STDDEV (Population Standard Deviation)**: Uses Welford's online algorithm for numerical stability:

```go
type stddevAgg struct {
    n    int64
    mean float64
    m2   float64
}

func (a *stddevAgg) Add(v any) bool {
    f, ok := toFloat64(v)
    if !ok {
        return false
    }
    a.n++
    delta := f - a.mean
    a.mean += delta / float64(a.n)
    delta2 := f - a.mean
    a.m2 += delta * delta2
    return true
}

func (a *stddevAgg) Result() any {
    if a.n < 2 {
        return float64(0)
    }
    return math.Sqrt(a.m2 / float64(a.n))
}
```

Welford's algorithm computes variance in a single pass without storing all values, and is numerically stable even for large datasets with values far from zero. The `m2` accumulator tracks the sum of squared differences from the running mean.

## Tumbling Windows

A tumbling window divides time into fixed, non-overlapping intervals. At the end of each interval, the window emits its aggregated results and resets.

```
Time:   |--- Window 1 ---|--- Window 2 ---|--- Window 3 ---|
Events: * * * *           * *   * *         * * * * *
Emit:              ^                 ^                  ^
```

The implementation maintains a map of group keys to aggregation states:

```go
type TumblingWindow struct {
    def    *ViewDef
    groups map[string]*groupState
    windowStart time.Time
}

type groupState struct {
    key         string
    keyValues   map[string]any
    aggregators []Aggregator
}
```

On each `Add()`, the event's group-by fields are extracted from the payload, serialized into a group key string, and the corresponding aggregators are updated. On `Flush()` (triggered by the ticker), all groups are evaluated against the HAVING predicate, emitted as events, and the window resets.

## Sliding Windows

A sliding window of size W with slide interval S overlaps: at any given time, an event contributes to `ceil(W/S)` active windows.

```
Window 1:  |------W------|
Window 2:       |------W------|
Window 3:            |------W------|
Slide:     |--S--|--S--|--S--|
```

Rather than maintaining W/S independent copies of the data, pgcdc uses a sub-window approach. The sliding window maintains `ceil(W/S)` tumbling sub-windows, each covering one slide interval. Events are added to the current sub-window. On each slide:

1. The oldest sub-window is evicted.
2. Results are merged across all remaining sub-windows.
3. The merged result is emitted.
4. A new empty sub-window is added.

For most aggregators, merging is straightforward: sum the counts, sum the sums, take min of mins, max of maxes. COUNT(DISTINCT) unions the seen sets. STDDEV requires Welford's parallel merge formula to combine partial statistics from different sub-windows without re-processing the raw data.

## Session Windows

A session window groups events by inactivity. As long as events keep arriving within the gap duration, the session stays open. When the gap elapses with no events for a group, the session closes and results are emitted.

```
Events: * * * *    [gap > timeout]    * * *    [gap > timeout]
        |---Session 1---|              |--Session 2--|
Emit:                   ^                            ^
```

Each group key maintains its own session state with a `lastSeen` timestamp. The ticker periodically checks all sessions and closes those that have exceeded the gap timeout:

```go
type sessionState struct {
    groups   map[string]*groupState
    lastSeen time.Time
    start    time.Time
}
```

Session windows are useful for user activity analysis: "What did this user do in their browsing session?" The session closes naturally when the user goes idle.

## Late Event Handling

In real-world CDC, events can arrive out of order. A WAL event might be processed slightly after the window it belongs to has already closed. Tumbling windows support `ALLOWED LATENESS` for this case:

```sql
SELECT channel, COUNT(*) as cnt
FROM pgcdc_events
GROUP BY channel
ALLOWED LATENESS 1m
TUMBLING WINDOW 5m
```

When a window closes, it is not immediately discarded. Instead, it moves to a `closedWindows` list and remains there for the lateness duration. Late events that fall within a closed window's time range update that window's aggregators and mark it for re-emission:

```go
type closedWindow struct {
    groups   map[string]*groupState
    start    time.Time
    end      time.Time
    closedAt time.Time
    reEmit   bool
}
```

On the next flush cycle, windows marked for re-emission produce updated results, and windows past the lateness deadline are garbage-collected. This provides eventually-correct results for tumbling windows without unbounded memory.

## Re-Injection and Loop Prevention

View results are emitted as `VIEW_RESULT` events on channels named `pgcdc:_view:<name>`. These events are re-injected into the bus's ingest channel via the `adapter.Reinjector` interface, making them available to all other adapters.

This creates a potential infinite loop: a view could process its own output. The engine prevents this with a simple prefix check:

```go
func (e *Engine) Process(ev event.Event) {
    if strings.HasPrefix(ev.Channel, "pgcdc:_view:") {
        return
    }
    // ...
}
```

Any event on a `pgcdc:_view:*` channel is silently skipped by all view instances.

## Emit Modes

Views support two emit modes:

**Row mode** (`--view-emit row`): One event per group key per window. If a window has 100 groups, 100 events are emitted. Good for downstream consumers that process individual groups.

**Batch mode** (`--view-emit batch`): One event containing all groups as a `rows` array. Good for dashboard consumers or aggregation sinks that want the full window result in a single payload.

## Configuration

Views can be defined in three ways:

**CLI flag** (repeatable):
```sh
pgcdc listen --view-query 'order_stats:SELECT channel, COUNT(*) as cnt FROM pgcdc_events GROUP BY channel TUMBLING WINDOW 1m'
```

**YAML config** (`views:` section):
```yaml
views:
  order_stats:
    query: |
      SELECT channel, COUNT(*) as cnt
      FROM pgcdc_events
      GROUP BY channel
      TUMBLING WINDOW 1m
    emit: row
    max_groups: 10000
```

**Programmatic** (library usage):
```go
def, _ := view.Parse("order_stats", query, view.EmitRow, 10000)
engine := view.NewEngine([]*view.ViewDef{def}, logger)
```

CLI flags win on name conflict with YAML. Multiple views can run simultaneously, each with independent window types and parameters.

## Cardinality Control

The `max_groups` parameter (default 100,000) caps the number of distinct group keys per window. When the limit is reached, new groups are rejected. This prevents a cardinality explosion from consuming unbounded memory -- a common risk with `GROUP BY` on high-cardinality fields.

## Practical Example

Consider tracking order volumes per product category in 5-minute tumbling windows:

```sql
SELECT payload.category, COUNT(*) as order_count, SUM(payload.total) as revenue
FROM pgcdc_events
WHERE operation = 'INSERT' AND channel = 'pgcdc:orders'
GROUP BY payload.category
HAVING COUNT(*) > 5
TUMBLING WINDOW 5m
```

Every 5 minutes, pgcdc emits events on `pgcdc:_view:category_revenue` containing the order count and total revenue per category. These results flow through the bus to any configured adapter: a webhook for alerting, SSE for a real-time dashboard, Kafka for downstream analytics.

The entire pipeline -- CDC capture, windowed aggregation, and result delivery -- runs in a single process with no external dependencies beyond the source database.
