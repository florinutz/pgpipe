//go:build integration

package scenarios

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/event"
)

func TestScenario_AllTables(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		// Create two separate tables to verify multi-table coverage.
		createTable(t, connStr, "at_orders")
		createTable(t, connStr, "at_customers")

		// Create a FOR ALL TABLES publication — simulates --all-tables flag behavior.
		pubName := "pgcdc_at_pub"
		createPublicationAllTables(t, connStr, pubName)

		// Start WAL pipeline with the all-tables publication.
		capture := newLineCapture()
		startWALPipeline(t, connStr, pubName, stdout.New(capture, testLogger()))
		time.Sleep(3 * time.Second)

		// Insert into both tables.
		insertRow(t, connStr, "at_orders", map[string]any{"item": "widget"})
		insertRow(t, connStr, "at_customers", map[string]any{"name": "alice"})

		// Collect events until we see INSERT events for both tables.
		// FOR ALL TABLES may deliver events from other test tables — skip non-target events.
		seen := make(map[string]bool)
		for !seen["pgcdc:at_orders"] || !seen["pgcdc:at_customers"] {
			line := capture.waitLine(t, 10*time.Second)
			var ev event.Event
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				continue // skip non-parseable lines from other tables
			}
			if ev.Operation == "INSERT" {
				seen[ev.Channel] = true
			}
		}
	})
}
