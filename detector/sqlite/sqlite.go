package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
)

const source = "sqlite"

const (
	defaultPollInterval = 500 * time.Millisecond
	defaultBatchSize    = 100
)

// Detector implements detector.Detector by polling a pgcdc_changes table in
// a local SQLite database. It follows the same polling pattern as the outbox
// detector.
type Detector struct {
	dbPath        string
	pollInterval  time.Duration
	batchSize     int
	keepProcessed bool
	logger        *slog.Logger
}

// New creates a SQLite detector. Duration and size parameters default to
// sensible values when zero.
func New(dbPath string, pollInterval time.Duration, batchSize int, keepProcessed bool, logger *slog.Logger) *Detector {
	if logger == nil {
		logger = slog.Default()
	}
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Detector{
		dbPath:        dbPath,
		pollInterval:  pollInterval,
		batchSize:     batchSize,
		keepProcessed: keepProcessed,
		logger:        logger.With("detector", source),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string { return source }

// Start polls the pgcdc_changes table and emits events. It blocks until ctx is
// cancelled. The caller owns the events channel; Start does NOT close it.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	db, err := sql.Open("sqlite", d.dbPath)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Enable WAL mode for better concurrent access.
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		d.logger.Warn("set WAL mode", "error", err)
	}

	d.logger.Info("sqlite detector started", "db", d.dbPath, "poll_interval", d.pollInterval)

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n, err := d.poll(ctx, db, events)
			if err != nil {
				d.logger.Error("poll", "error", err)
				metrics.SQLiteErrors.Inc()
				continue
			}
			if n > 0 {
				metrics.SQLiteEventsProcessed.Add(float64(n))
				d.logger.Debug("poll cycle", "rows", n)
			}
		}
	}
}

func (d *Detector) poll(ctx context.Context, db *sql.DB, events chan<- event.Event) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(ctx,
		`SELECT rowid, table_name, operation, row_data, old_data, created_at
		 FROM pgcdc_changes
		 WHERE processed = 0
		 ORDER BY rowid
		 LIMIT ?`, d.batchSize)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type changeRow struct {
		rowID     int64
		tableName string
		operation string
		rowData   sql.NullString
		oldData   sql.NullString
		createdAt string
	}

	var processed []changeRow
	for rows.Next() {
		var r changeRow
		if err := rows.Scan(&r.rowID, &r.tableName, &r.operation, &r.rowData, &r.oldData, &r.createdAt); err != nil {
			return 0, fmt.Errorf("scan: %w", err)
		}
		processed = append(processed, r)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows iteration: %w", err)
	}

	if len(processed) == 0 {
		return 0, nil
	}

	// Emit events for each row.
	var rowIDs []int64
	for _, r := range processed {
		payload := buildPayload(r.operation, r.rowData, r.oldData)
		channel := "pgcdc:" + r.tableName

		ev, evErr := event.New(channel, r.operation, payload, source)
		if evErr != nil {
			d.logger.Error("create event", "error", evErr)
			continue
		}

		select {
		case events <- ev:
			rowIDs = append(rowIDs, r.rowID)
		case <-ctx.Done():
			return 0, nil
		}
	}

	// Clean up processed rows.
	if len(rowIDs) > 0 {
		for _, id := range rowIDs {
			if d.keepProcessed {
				if _, err := tx.ExecContext(ctx, "UPDATE pgcdc_changes SET processed = 1 WHERE rowid = ?", id); err != nil {
					return 0, fmt.Errorf("mark processed: %w", err)
				}
			} else {
				if _, err := tx.ExecContext(ctx, "DELETE FROM pgcdc_changes WHERE rowid = ?", id); err != nil {
					return 0, fmt.Errorf("delete processed: %w", err)
				}
			}
		}

		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit: %w", err)
		}

		metrics.SQLitePolled.Inc()
	}

	return len(rowIDs), nil
}

func buildPayload(operation string, rowData, oldData sql.NullString) json.RawMessage {
	m := map[string]any{}

	if rowData.Valid {
		var data any
		if err := json.Unmarshal([]byte(rowData.String), &data); err == nil {
			m["new"] = data
		} else {
			m["new"] = rowData.String
		}
	}

	if oldData.Valid {
		var data any
		if err := json.Unmarshal([]byte(oldData.String), &data); err == nil {
			m["old"] = data
		} else {
			m["old"] = oldData.String
		}
	}

	b, _ := json.Marshal(m)
	return b
}
