package grpc_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/florinutz/pgcdc/adapter/adaptertest"
	adaptergrpc "github.com/florinutz/pgcdc/adapter/grpc"
	"github.com/florinutz/pgcdc/event"
)

func TestGRPCAdapter_Contract(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// grpc Start() blocks on ctx.Done() even after channel close (runs a server),
	// so we only run cancel-based tests.
	adaptertest.RunCancelOnly(t, "grpc", func(ctx context.Context, ch <-chan event.Event) error {
		return adaptergrpc.New(":0", logger).Start(ctx, ch)
	})
}
