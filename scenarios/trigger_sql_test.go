//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestScenario_TriggerSQL(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		// Generate trigger SQL using the CLI.
		output, err := runPGCDC("init", "--table", "trigger_test_orders")
		if err != nil {
			t.Fatalf("pgcdc init: %v\noutput: %s", err, output)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.Close(ctx)

		// Create the table (trigger SQL expects it to exist).
		_, err = conn.Exec(ctx, "CREATE TABLE trigger_test_orders (id SERIAL PRIMARY KEY, data JSONB DEFAULT '{}')")
		if err != nil {
			t.Fatalf("create table: %v", err)
		}

		// Execute the generated trigger SQL.
		_, err = conn.Exec(ctx, output)
		if err != nil {
			t.Fatalf("execute trigger SQL: %v\nSQL: %s", err, output)
		}

		// Subscribe to the notification channel.
		_, err = conn.Exec(ctx, `LISTEN "pgcdc:trigger_test_orders"`)
		if err != nil {
			t.Fatalf("listen: %v", err)
		}

		// Insert a row to fire the trigger.
		_, err = conn.Exec(ctx, `INSERT INTO trigger_test_orders (data) VALUES ('{"item":"widget"}')`)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}

		// Wait for the notification.
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			t.Fatalf("wait for notification: %v", err)
		}

		if notification.Channel != "pgcdc:trigger_test_orders" {
			t.Errorf("channel = %q, want %q", notification.Channel, "pgcdc:trigger_test_orders")
		}

		var payload map[string]json.RawMessage
		if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
			t.Fatalf("payload not JSON: %v\nraw: %s", err, notification.Payload)
		}
		var op string
		json.Unmarshal(payload["op"], &op)
		if op != "INSERT" {
			t.Errorf("op = %q, want INSERT", op)
		}
	})

	t.Run("invalid table name rejected", func(t *testing.T) {
		output, err := runPGCDC("init", "--table", "123-invalid!")
		if err == nil {
			t.Fatal("expected error for invalid table name")
		}
		if !strings.Contains(output, "invalid table name") {
			t.Errorf("unexpected output: %s", output)
		}
	})

	t.Run("invalid channel name rejected", func(t *testing.T) {
		output, err := runPGCDC("init", "--table", "orders", "--channel", "bad channel!")
		if err == nil {
			t.Fatal("expected error for invalid channel name")
		}
		if !strings.Contains(output, "invalid channel name") {
			t.Errorf("unexpected output: %s", output)
		}
	})
}
