//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/snapshot"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Snapshot(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "snapshot_test_orders"

		// Create and populate the table.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)

		safeTable := pgx.Identifier{table}.Sanitize()
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')`, safeTable))
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// Insert 100 rows.
		for i := range 100 {
			_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", safeTable), fmt.Sprintf(`{"item": %d}`, i))
			if err != nil {
				t.Fatalf("insert row %d: %v", i, err)
			}
		}
		conn.Close(ctx)

		// Run snapshot through stdout adapter.
		capture := newLineCapture()
		logger := testLogger()

		snap := snapshot.New(connStr, table, "", 50, logger)
		b := bus.New(256, logger)
		stdoutAdapter := stdout.New(capture, logger)

		sub, err := b.Subscribe(stdoutAdapter.Name())
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}

		snapCtx, snapCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer snapCancel()

		g, gCtx := errgroup.WithContext(snapCtx)

		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return stdoutAdapter.Start(gCtx, sub) })
		g.Go(func() error {
			err := snap.Run(gCtx, b.Ingest())
			snapCancel()
			return err
		})

		_ = g.Wait()

		// Collect all lines.
		var events []event.Event
		for {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					t.Fatalf("invalid JSON: %v\nraw: %s", err, line)
				}
				events = append(events, ev)
			default:
				goto done
			}
		}
	done:
		if len(events) != 100 {
			t.Fatalf("expected 100 events, got %d", len(events))
		}

		// Verify event properties.
		for i, ev := range events {
			if ev.Channel != "pgcdc:"+table {
				t.Errorf("event %d: channel = %q, want %q", i, ev.Channel, "pgcdc:"+table)
			}
			if ev.Operation != "SNAPSHOT" {
				t.Errorf("event %d: operation = %q, want SNAPSHOT", i, ev.Operation)
			}
			if ev.Source != "snapshot" {
				t.Errorf("event %d: source = %q, want snapshot", i, ev.Source)
			}
			if ev.ID == "" {
				t.Errorf("event %d: ID is empty", i)
			}
		}
	})

	t.Run("non-existent table returns error", func(t *testing.T) {
		snap := snapshot.New(connStr, "this_table_does_not_exist", "", 0, testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		events := make(chan event.Event, 100)
		err := snap.Run(ctx, events)
		if err == nil {
			t.Fatal("expected error for non-existent table")
		}
	})
}
