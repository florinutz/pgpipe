//go:build !no_nats

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	natsadapter "github.com/florinutz/pgcdc/adapter/nats"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeNATSAdapter(cfg config.Config, enc encoding.Encoder, logger *slog.Logger) (adapter.Adapter, error) {
	return natsadapter.New(
		cfg.Nats.URL,
		cfg.Nats.Subject,
		cfg.Nats.Stream,
		cfg.Nats.CredFile,
		cfg.Nats.MaxAge,
		cfg.Nats.BackoffBase,
		cfg.Nats.BackoffCap,
		enc,
		logger,
	), nil
}
