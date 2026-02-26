//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_ToastCache(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path - cache resolves TOAST", func(t *testing.T) {
		table := "toast_cache_happy"
		pub := "pgcdc_toast_happy"
		createToastTable(t, connStr, table)
		createPublication(t, connStr, pub, table)

		capture := newLineCapture()
		startWALPipelineWithToastCache(t, connStr, pub, 100000, stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		// INSERT with large text (>2KB to trigger TOAST storage).
		largeText := strings.Repeat("x", 3000)
		insertToastRow(t, connStr, table, "active", largeText)
		insertLine := capture.waitLine(t, 10*time.Second)

		var insertEv event.Event
		if err := json.Unmarshal([]byte(insertLine), &insertEv); err != nil {
			t.Fatalf("invalid JSON for insert: %v", err)
		}
		var insertPayload map[string]any
		json.Unmarshal(insertEv.Payload, &insertPayload)
		row := insertPayload["row"].(map[string]any)
		idStr := row["id"].(string)
		var rowID int
		for _, c := range idStr {
			rowID = rowID*10 + int(c-'0')
		}

		// UPDATE only the status column — description is unchanged TOAST.
		updateToastStatus(t, connStr, table, rowID, "shipped")
		updateLine := capture.waitLine(t, 10*time.Second)

		var updateEv event.Event
		if err := json.Unmarshal([]byte(updateLine), &updateEv); err != nil {
			t.Fatalf("invalid JSON for update: %v", err)
		}
		var updatePayload map[string]any
		json.Unmarshal(updateEv.Payload, &updatePayload)

		// Assert: cache should have resolved the TOAST column.
		updateRow := updatePayload["row"].(map[string]any)
		desc, ok := updateRow["description"]
		if !ok {
			t.Fatal("expected description column in update row")
		}
		descStr, ok := desc.(string)
		if !ok || descStr != largeText {
			t.Errorf("expected description resolved from cache (%d chars), got %v", len(largeText), desc)
		}

		// Assert: no _unchanged_toast_columns metadata (cache hit).
		if _, hasToast := updatePayload["_unchanged_toast_columns"]; hasToast {
			t.Error("expected no _unchanged_toast_columns (cache should have resolved it)")
		}
	})

	t.Run("cache miss emits metadata", func(t *testing.T) {
		table := "toast_cache_miss"
		pub := "pgcdc_toast_miss"
		createToastTable(t, connStr, table)
		createPublication(t, connStr, pub, table)

		// Insert a row BEFORE starting the pipeline so the cache is cold.
		largeText := strings.Repeat("y", 3000)
		insertToastRow(t, connStr, table, "pending", largeText)

		// Get the row ID.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		var rowID int
		err = conn.QueryRow(ctx, fmt.Sprintf("SELECT id FROM %s LIMIT 1", pgx.Identifier{table}.Sanitize())).Scan(&rowID)
		conn.Close(ctx)
		if err != nil {
			t.Fatalf("get row id: %v", err)
		}

		capture := newLineCapture()
		startWALPipelineWithToastCache(t, connStr, pub, 100000, stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		// UPDATE only status — pipeline hasn't seen the INSERT, so cache is cold.
		updateToastStatus(t, connStr, table, rowID, "shipped")
		updateLine := capture.waitLine(t, 10*time.Second)

		var updateEv event.Event
		if err := json.Unmarshal([]byte(updateLine), &updateEv); err != nil {
			t.Fatalf("invalid JSON for update: %v", err)
		}
		var updatePayload map[string]any
		json.Unmarshal(updateEv.Payload, &updatePayload)

		// Assert: description should be null (cache miss).
		updateRow := updatePayload["row"].(map[string]any)
		if desc, exists := updateRow["description"]; exists && desc != nil {
			t.Errorf("expected description=null on cache miss, got %v", desc)
		}

		// Assert: _unchanged_toast_columns should contain "description".
		toastCols, ok := updatePayload["_unchanged_toast_columns"]
		if !ok {
			t.Fatal("expected _unchanged_toast_columns metadata on cache miss")
		}
		toastArr, ok := toastCols.([]any)
		if !ok || len(toastArr) == 0 {
			t.Fatalf("expected non-empty _unchanged_toast_columns array, got %v", toastCols)
		}
		found := false
		for _, col := range toastArr {
			if col == "description" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected 'description' in _unchanged_toast_columns, got %v", toastArr)
		}
	})
}

// ─── TOAST-specific helpers ─────────────────────────────────────────────────

// createToastTable creates a table with a TEXT column that will use TOAST
// storage for large values. Uses REPLICA IDENTITY DEFAULT (not FULL) so
// unchanged TOAST columns arrive as 'u' in the WAL.
func createToastTable(t *testing.T, connStr, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createToastTable connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		status TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT ''
	)`, safeTable)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createToastTable: %v", err)
	}

	// Force EXTERNAL storage on description so PG externalizes without
	// compression. Repeated characters compress too well and stay inline,
	// defeating the TOAST unchanged-column test.
	storage := fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN description SET STORAGE EXTERNAL`, safeTable)
	if _, err := conn.Exec(ctx, storage); err != nil {
		t.Fatalf("createToastTable set storage external: %v", err)
	}

	// Explicitly set REPLICA IDENTITY DEFAULT (PK only) — this is the
	// default, but being explicit makes the test's intent clear.
	identity := fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY DEFAULT`, safeTable)
	if _, err := conn.Exec(ctx, identity); err != nil {
		t.Fatalf("createToastTable set replica identity: %v", err)
	}
}

func insertToastRow(t *testing.T, connStr, table, status, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertToastRow connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (status, description) VALUES ($1, $2)", pgx.Identifier{table}.Sanitize()),
		status, description,
	)
	if err != nil {
		t.Fatalf("insertToastRow: %v", err)
	}
}

func updateToastStatus(t *testing.T, connStr, table string, id int, status string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("updateToastStatus connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx,
		fmt.Sprintf("UPDATE %s SET status = $1 WHERE id = $2", pgx.Identifier{table}.Sanitize()),
		status, id,
	)
	if err != nil {
		t.Fatalf("updateToastStatus: %v", err)
	}
}

// startWALPipelineWithToastCache starts a WAL pipeline with the TOAST cache enabled.
func startWALPipelineWithToastCache(t *testing.T, connStr string, publication string, cacheSize int, adapters ...adapter.Adapter) {
	t.Helper()

	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())

	det := walreplication.New(connStr, publication, 0, 0, false, false, logger)
	det.SetToastCache(cacheSize)
	b := bus.New(64, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

	for _, a := range adapters {
		sub, err := b.Subscribe(a.Name())
		if err != nil {
			cancel()
			t.Fatalf("subscribe %s: %v", a.Name(), err)
		}
		a := a
		g.Go(func() error { return a.Start(gCtx, sub) })
	}

	t.Cleanup(func() {
		cancel()
		g.Wait()
	})
}
