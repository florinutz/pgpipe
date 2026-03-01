package testutil

import (
	"log/slog"
	"testing"

	"github.com/florinutz/pgcdc/bus"
)

// StartTestBus creates a new fast-mode bus with a small buffer and registers
// t.Cleanup to close subscriber channels via context cancellation. Returns the
// bus ready to use in tests. The caller must start the bus with b.Start(ctx).
func StartTestBus(t *testing.T) *bus.Bus {
	t.Helper()
	b := bus.New(64, slog.Default())
	return b
}
