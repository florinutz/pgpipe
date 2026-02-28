package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/florinutz/pgcdc/event"
)

// Adapter writes events as JSON-lines to an io.Writer.
// It is the simplest adapter, useful for debugging and unix piping
// (e.g. pgcdc listen -c orders | jq .payload).
//
// Implements adapter.Deliverer — the middleware stack provides the event loop,
// metrics, and cooperative checkpoint ack.
type Adapter struct {
	w      io.Writer
	mu     sync.Mutex // protects concurrent writes
	logger *slog.Logger
}

// New creates a stdout adapter that writes JSON-lines to w.
// If w is nil it defaults to os.Stdout.
func New(w io.Writer, logger *slog.Logger) *Adapter {
	if w == nil {
		w = os.Stdout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{
		w:      w,
		logger: logger,
	}
}

// Deliver writes a single event as a JSON line to the underlying writer.
// Implements adapter.Deliverer.
func (a *Adapter) Deliver(_ context.Context, ev event.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := json.NewEncoder(a.w).Encode(ev); err != nil {
		return fmt.Errorf("encode event %s: %w", ev.ID, err)
	}
	return nil
}

// Start is the legacy event loop kept for backward compatibility.
// When the middleware detects Deliverer, it uses Deliver() instead.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("stdout adapter started (legacy path)")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if err := a.Deliver(ctx, ev); err != nil {
				return err
			}
		}
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "stdout"
}
