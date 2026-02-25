package sse_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/florinutz/pgcdc/adapter/adaptertest"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/event"
)

func TestSSEAdapter_Contract(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	adaptertest.RunContractTests(t, "sse", func(ctx context.Context, ch <-chan event.Event) error {
		return sse.New(100, 0, logger).Start(ctx, ch)
	})
}
