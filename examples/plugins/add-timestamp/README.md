# add-timestamp plugin

A pgcdc Wasm transform plugin that adds a `processed_at` timestamp to every event's row data.

## What it does

When an event passes through this transform, it:
1. Parses the event payload JSON
2. Adds a `processed_at` field (RFC 3339 UTC) to the `row` object
3. Returns the modified event

Events without a `row` object are passed through unchanged.

## Build

Requires [TinyGo](https://tinygo.org/getting-started/install/):

```sh
make build
```

This produces `add-timestamp.wasm`.

## Usage

### CLI

```sh
pgcdc listen \
  --db postgres://localhost/mydb \
  --channel orders \
  --plugin-transform add-timestamp:./add-timestamp.wasm
```

### YAML config

```yaml
database_url: postgres://localhost/mydb
channels: [orders]
adapters: [stdout]

plugins:
  transforms:
    - name: add-timestamp
      path: ./add-timestamp.wasm
      encoding: json
      scope: global
```

## Example output

Before:
```json
{
  "id": "...",
  "channel": "orders",
  "operation": "INSERT",
  "payload": {"op": "INSERT", "table": "orders", "row": {"id": 1, "total": 99.99}}
}
```

After:
```json
{
  "id": "...",
  "channel": "orders",
  "operation": "INSERT",
  "payload": {"op": "INSERT", "table": "orders", "row": {"id": 1, "total": 99.99, "processed_at": "2026-02-26T12:00:00Z"}}
}
```
