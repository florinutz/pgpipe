package reconnect

import (
	"context"
	"log/slog"
	"time"

	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/prometheus/client_golang/prometheus"
)

// Loop runs runFn in a reconnect loop with exponential backoff.
// runFn should block while the connection is healthy and return an error when
// it fails. If runFn returns nil, Loop returns nil (clean exit). Loop returns
// nil when ctx is cancelled.
func Loop(ctx context.Context, name string, base, max time.Duration,
	logger *slog.Logger, errCounter prometheus.Counter,
	runFn func(ctx context.Context) error,
) error {
	if logger == nil {
		logger = slog.Default()
	}

	var attempt int
	for {
		err := runFn(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}

		delay := backoff.Jitter(attempt, base, max)
		if errCounter != nil {
			errCounter.Inc()
		}
		logger.Error("connection lost, reconnecting",
			"adapter", name,
			"error", err,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
