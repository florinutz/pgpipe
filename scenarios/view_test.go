//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	viewadapter "github.com/florinutz/pgcdc/adapter/view"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/view"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

func TestScenario_View(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path: count and sum with group by", func(t *testing.T) {
		table := "view_orders"
		pubName := "pgcdc_view_orders"

		// Create table with typed columns (not JSONB) so WAL row values are directly accessible.
		createViewTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		// Parse view definition.
		// WAL payload is: {"op":"INSERT","table":"view_orders","row":{"id":"1","region":"us-east","amount":"100"},"old":null}
		// So we use payload.row.region and payload.row.amount.
		def, err := view.Parse("orders_per_window",
			"SELECT COUNT(*) as order_count, SUM(payload.row.amount) as total_revenue, payload.row.region "+
				"FROM pgcdc_events "+
				"WHERE channel = 'pgcdc:view_orders' AND operation = 'INSERT' "+
				"GROUP BY payload.row.region "+
				"TUMBLING WINDOW 2s",
			view.EmitRow, 0)
		if err != nil {
			t.Fatalf("parse view: %v", err)
		}

		logger := testLogger()
		engine := view.NewEngine([]*view.ViewDef{def}, logger)
		va := viewadapter.New(engine, logger)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)

		det := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		b := bus.New(64, logger)

		ctx, cancel := context.WithCancel(context.Background())
		va.SetIngestChan(b.Ingest())

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		viewSub, err := b.Subscribe("view")
		if err != nil {
			cancel()
			t.Fatalf("subscribe view: %v", err)
		}
		g.Go(func() error { return va.Start(gCtx, viewSub) })

		stdoutSub, err := b.Subscribe("stdout")
		if err != nil {
			cancel()
			t.Fatalf("subscribe stdout: %v", err)
		}
		g.Go(func() error { return stdoutAdapter.Start(gCtx, stdoutSub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		time.Sleep(3 * time.Second)

		// Insert test data.
		insertViewOrderRow(t, connStr, table, "us-east", 100)
		insertViewOrderRow(t, connStr, table, "us-east", 200)
		insertViewOrderRow(t, connStr, table, "eu-west", 50)

		// Consume the 3 INSERT events.
		for i := 0; i < 3; i++ {
			line := capture.waitLine(t, 10*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Fatalf("unmarshal insert event %d: %v\nraw: %s", i, err, line)
			}
			if ev.Operation != "INSERT" {
				t.Fatalf("expected INSERT, got %s", ev.Operation)
			}
		}

		// Wait for the 2s tumbling window to fire and emit VIEW_RESULT events.
		var viewResults []event.Event
		deadline := time.After(10 * time.Second)
		for len(viewResults) < 2 {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation == "VIEW_RESULT" {
					viewResults = append(viewResults, ev)
				}
			case <-deadline:
				t.Fatalf("timed out waiting for VIEW_RESULT events (got %d)", len(viewResults))
			}
		}

		// Verify results by region.
		byRegion := make(map[string]map[string]any)
		for _, ev := range viewResults {
			if ev.Channel != "pgcdc:_view:orders_per_window" {
				t.Errorf("channel = %q, want pgcdc:_view:orders_per_window", ev.Channel)
			}
			var p map[string]any
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				t.Fatalf("unmarshal view result: %v", err)
			}
			region := p["region"].(string)
			byRegion[region] = p
		}

		usEast := byRegion["us-east"]
		if usEast == nil {
			t.Fatal("missing us-east group")
		}
		if int64(usEast["order_count"].(float64)) != 2 {
			t.Errorf("us-east order_count = %v, want 2", usEast["order_count"])
		}
		if usEast["total_revenue"].(float64) != 300 {
			t.Errorf("us-east total_revenue = %v, want 300", usEast["total_revenue"])
		}

		euWest := byRegion["eu-west"]
		if euWest == nil {
			t.Fatal("missing eu-west group")
		}
		if int64(euWest["order_count"].(float64)) != 1 {
			t.Errorf("eu-west order_count = %v, want 1", euWest["order_count"])
		}
		if euWest["total_revenue"].(float64) != 50 {
			t.Errorf("eu-west total_revenue = %v, want 50", euWest["total_revenue"])
		}

		if usEast["_window"] == nil {
			t.Error("missing _window in view result")
		}
	})

	t.Run("batch emit mode", func(t *testing.T) {
		table := "view_batch_orders"
		pubName := "pgcdc_view_batch_orders"
		createViewTable(t, connStr, table)
		createPublication(t, connStr, pubName, table)

		def, err := view.Parse("batch_view",
			"SELECT COUNT(*) as cnt, payload.row.region "+
				"FROM pgcdc_events "+
				"WHERE channel = 'pgcdc:view_batch_orders' AND operation = 'INSERT' "+
				"GROUP BY payload.row.region "+
				"TUMBLING WINDOW 2s",
			view.EmitBatch, 0)
		if err != nil {
			t.Fatalf("parse view: %v", err)
		}

		logger := testLogger()
		engine := view.NewEngine([]*view.ViewDef{def}, logger)
		va := viewadapter.New(engine, logger)
		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, logger)
		det := walreplication.New(connStr, pubName, 0, 0, false, false, logger)
		b := bus.New(64, logger)

		ctx, cancel := context.WithCancel(context.Background())
		va.SetIngestChan(b.Ingest())

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		viewSub, _ := b.Subscribe("view")
		g.Go(func() error { return va.Start(gCtx, viewSub) })
		stdoutSub, _ := b.Subscribe("stdout")
		g.Go(func() error { return stdoutAdapter.Start(gCtx, stdoutSub) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		time.Sleep(3 * time.Second)

		insertViewOrderRow(t, connStr, table, "us-east", 10)
		insertViewOrderRow(t, connStr, table, "eu-west", 20)

		// Consume INSERT events.
		for i := 0; i < 2; i++ {
			capture.waitLine(t, 10*time.Second)
		}

		// Wait for batch VIEW_RESULT.
		deadline := time.After(10 * time.Second)
		for {
			select {
			case line := <-capture.lines:
				var ev event.Event
				if err := json.Unmarshal([]byte(line), &ev); err != nil {
					continue
				}
				if ev.Operation != "VIEW_RESULT" {
					continue
				}

				var p map[string]any
				json.Unmarshal(ev.Payload, &p)
				rows, ok := p["rows"].([]any)
				if !ok {
					t.Fatalf("batch result missing rows array, got %T", p["rows"])
				}
				if len(rows) != 2 {
					t.Errorf("batch rows = %d, want 2", len(rows))
				}
				if p["_window"] == nil {
					t.Error("batch result missing _window")
				}
				return

			case <-deadline:
				t.Fatal("timed out waiting for batch VIEW_RESULT")
			}
		}
	})
}

// createViewTable creates a table with explicit region and amount columns.
func createViewTable(t *testing.T, connStr, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("createViewTable connect: %v", err)
	}
	defer conn.Close(ctx)

	safeTable := pgx.Identifier{table}.Sanitize()
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, region TEXT, amount NUMERIC)`, safeTable)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("createViewTable: %v", err)
	}
	identity := fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL`, safeTable)
	if _, err := conn.Exec(ctx, identity); err != nil {
		t.Fatalf("createViewTable set replica identity: %v", err)
	}
}

// insertViewOrderRow inserts a row with region and amount into a view test table.
func insertViewOrderRow(t *testing.T, connStr, table, region string, amount float64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("insertViewOrderRow connect: %v", err)
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (region, amount) VALUES ($1, $2)", pgx.Identifier{table}.Sanitize()), region, amount)
	if err != nil {
		t.Fatalf("insertViewOrderRow: %v", err)
	}
}
