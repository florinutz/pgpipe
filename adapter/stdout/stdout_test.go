package stdout_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/florinutz/pgcdc/adapter/adaptertest"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/event"
)

func TestStdoutAdapter_Contract(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := stdout.New(&bytes.Buffer{}, logger)

	adaptertest.RunContractTests(t, a.Name(), func(ctx context.Context, ch <-chan event.Event) error {
		// Each subtest needs a fresh adapter to avoid shared writer state.
		return stdout.New(&bytes.Buffer{}, logger).Start(ctx, ch)
	})
}

func TestStdoutAdapter_WritesJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := stdout.New(&buf, logger)

	ch := make(chan event.Event, 1)
	ev := adaptertest.TestEvent()
	ch <- ev
	close(ch)

	if err := a.Start(context.Background(), ch); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	var got event.Event
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}
	if got.ID != ev.ID {
		t.Errorf("event ID = %q, want %q", got.ID, ev.ID)
	}
}
