package s3

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/florinutz/pgcdc/event"
	"github.com/parquet-go/parquet-go"
)

// s3Row is the Parquet row struct for S3 raw format.
type s3Row struct {
	EventID   string `parquet:"event_id"`
	Operation string `parquet:"operation"`
	Timestamp int64  `parquet:"timestamp,timestamp(microsecond)"`
	Channel   string `parquet:"channel"`
	Source    string `parquet:"source"`
	Payload   string `parquet:"payload"`
}

// writeJSONL encodes events as JSON Lines (one JSON object per line).
func writeJSONL(events []event.Event) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			return nil, fmt.Errorf("encode event %s: %w", ev.ID, err)
		}
	}
	return buf.Bytes(), nil
}

// writeParquet encodes events as a Parquet file with Snappy compression.
func writeParquet(events []event.Event) ([]byte, error) {
	rows := make([]s3Row, 0, len(events))
	for _, ev := range events {
		rows = append(rows, s3Row{
			EventID:   ev.ID,
			Operation: ev.Operation,
			Timestamp: ev.CreatedAt.UnixMicro(),
			Channel:   ev.Channel,
			Source:    ev.Source,
			Payload:   string(ev.Payload),
		})
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[s3Row](&buf, parquet.Compression(&parquet.Snappy))

	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("write parquet rows: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}
