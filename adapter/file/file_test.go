package file_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/florinutz/pgcdc/adapter/adaptertest"
	"github.com/florinutz/pgcdc/adapter/file"
	"github.com/florinutz/pgcdc/event"
)

func TestFileAdapter_Contract(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	adaptertest.RunContractTests(t, "file", func(ctx context.Context, ch <-chan event.Event) error {
		path := filepath.Join(t.TempDir(), "out.jsonl")
		return file.New(path, 0, 0, logger).Start(ctx, ch)
	})
}

func TestFileAdapter_WritesJSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	path := filepath.Join(t.TempDir(), "out.jsonl")
	a := file.New(path, 0, 0, logger)

	ch := make(chan event.Event, 1)
	ev := adaptertest.TestEvent()
	ch <- ev
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	var got event.Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}
	if got.ID != ev.ID {
		t.Errorf("event ID = %q, want %q", got.ID, ev.ID)
	}
}
