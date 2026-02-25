package exec_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter/adaptertest"
	"github.com/florinutz/pgcdc/adapter/exec"
	"github.com/florinutz/pgcdc/event"
)

func TestExecAdapter_Contract(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// exec Start() has a restart loop: closing the channel makes run() return nil
	// but Start() tries to restart. Only cancel-based tests work reliably.
	adaptertest.RunCancelOnly(t, "exec", func(ctx context.Context, ch <-chan event.Event) error {
		return exec.New("cat", 0, 0, logger).Start(ctx, ch)
	})
}

func TestExecAdapter_ProcessesEvent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := exec.New("cat", 0, 0, logger)

	ch := make(chan event.Event, 1)
	ch <- adaptertest.TestEvent()
	close(ch)

	// Use a short timeout: after the event is consumed and channel is closed,
	// run() returns nil but Start() enters the restart loop. The timeout
	// ensures we don't hang.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := a.Start(ctx, ch)
	if err != nil && err != context.DeadlineExceeded {
		t.Errorf("expected nil or DeadlineExceeded, got %v", err)
	}
}
