package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/jackc/pgx/v5"
)

const source = "snapshot"

// Snapshot exports existing rows from a table as SNAPSHOT events.
type Snapshot struct {
	dbURL     string
	table     string
	where     string
	batchSize int
	logger    *slog.Logger
}

// New creates a Snapshot. batchSize controls how many rows are buffered
// before flushing to the events channel (defaults to 1000 if <= 0).
func New(dbURL, table, where string, batchSize int, logger *slog.Logger) *Snapshot {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Snapshot{
		dbURL:     dbURL,
		table:     table,
		where:     where,
		batchSize: batchSize,
		logger:    logger.With("component", "snapshot"),
	}
}

// Run exports all rows from the table as SNAPSHOT events to the events channel.
// It uses a REPEATABLE READ transaction to get a consistent view of the table.
// The caller owns the events channel; Run does NOT close it.
func (s *Snapshot) Run(ctx context.Context, events chan<- event.Event) error {
	start := time.Now()

	conn, err := pgx.Connect(ctx, s.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Start a REPEATABLE READ transaction for a consistent snapshot.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	channel := "pgcdc:" + s.table

	// Build the query.
	safeTable := pgx.Identifier{s.table}.Sanitize()
	query := "SELECT * FROM " + safeTable
	if s.where != "" {
		query += " WHERE " + s.where
	}

	s.logger.Info("starting snapshot", "table", s.table, "where", s.where)

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	var count int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}

		row := make(map[string]any, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = values[i]
		}

		payload := map[string]any{
			"op":    event.OpSnapshot,
			"table": s.table,
			"row":   row,
			"old":   nil,
		}

		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			s.logger.Error("marshal payload failed", "error", err)
			continue
		}

		ev, err := event.New(channel, event.OpSnapshot, payloadJSON, source)
		if err != nil {
			s.logger.Error("create event failed", "error", err)
			continue
		}

		select {
		case events <- ev:
			count++
			metrics.SnapshotRowsExported.Inc()
		case <-ctx.Done():
			return ctx.Err()
		}

		if count%int64(s.batchSize) == 0 {
			s.logger.Info("snapshot progress", "rows", count, "elapsed", time.Since(start))
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	elapsed := time.Since(start)
	metrics.SnapshotDuration.Observe(elapsed.Seconds())
	s.logger.Info("snapshot complete", "table", s.table, "rows", count, "elapsed", elapsed)

	return nil
}
