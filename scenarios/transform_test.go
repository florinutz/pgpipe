//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pgcdc "github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/transform"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Transform(t *testing.T) {
	connStr := startPostgres(t)
	channel := "transform_test"

	t.Run("happy path", func(t *testing.T) {
		// Pipeline with:
		// - global: drop ssn column
		// - per-adapter: mask email with hash, rename created_at → createdAt
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			// Global transforms.
			pgcdc.WithTransform(transform.DropColumns("ssn")),
			// Per-adapter transforms.
			pgcdc.WithAdapterTransform("stdout", transform.Mask(
				transform.MaskField{Field: "email", Mode: transform.MaskHash},
			)),
			pgcdc.WithAdapterTransform("stdout", transform.RenameFields(map[string]string{
				"created_at": "createdAt",
			})),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		// Give pipeline time to connect.
		time.Sleep(1 * time.Second)

		// Send a flat JSON payload with fields at top level.
		sendNotify(t, connStr, channel,
			`{"name":"Alice","ssn":"123-45-6789","email":"alice@example.com","created_at":"2024-01-01"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		payload, ok := ev["payload"].(map[string]any)
		if !ok {
			t.Fatal("payload is not a map")
		}

		// ssn should be dropped.
		if _, found := payload["ssn"]; found {
			t.Error("ssn should have been dropped by DropColumns")
		}

		// email should be masked (SHA-256 hash, not the original).
		email, _ := payload["email"].(string)
		if email == "alice@example.com" || email == "" {
			t.Errorf("email should be hashed, got %q", email)
		}
		// Hash should be 64 hex chars.
		if len(email) != 64 {
			t.Errorf("email hash length = %d, want 64", len(email))
		}

		// created_at should be renamed to createdAt.
		if _, found := payload["created_at"]; found {
			t.Error("created_at should have been renamed")
		}
		if _, found := payload["createdAt"]; !found {
			t.Error("createdAt should exist after rename")
		}

		// Name should be untouched.
		if payload["name"] != "Alice" {
			t.Errorf("name = %v, want Alice", payload["name"])
		}
	})

	t.Run("filter drops non-matching events", func(t *testing.T) {
		// Pipeline with content-based filter: only pass events where payload
		// field "status" equals "published".
		logger := testLogger()
		lc := newLineCapture()

		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		p := pgcdc.NewPipeline(det,
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(logger),
			pgcdc.WithAdapter(stdout.New(lc, logger)),
			pgcdc.WithTransform(transform.FilterField("status", "published")),
		)

		ctx, cancel := context.WithCancel(context.Background())
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return p.Run(gCtx) })
		t.Cleanup(func() {
			cancel()
			_ = g.Wait()
		})

		time.Sleep(1 * time.Second)

		// Send a "draft" event — should be filtered out.
		sendNotify(t, connStr, channel, `{"id":1,"status":"draft","title":"Draft Post"}`)

		// Send a "published" event — should pass through.
		sendNotify(t, connStr, channel, `{"id":2,"status":"published","title":"Live Post"}`)

		line := lc.waitLine(t, 5*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		payload, ok := ev["payload"].(map[string]any)
		if !ok {
			t.Fatal("payload is not a map")
		}

		// The event that came through should be the "published" one.
		if payload["status"] != "published" {
			t.Errorf("expected published event to pass filter, got status=%v", payload["status"])
		}
		if payload["title"] != "Live Post" {
			t.Errorf("title = %v, want Live Post", payload["title"])
		}
	})
}
