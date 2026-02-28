//go:build !no_nats

package cmd

import (
	natsadapter "github.com/florinutz/pgcdc/adapter/nats"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "nats",
		Description: "NATS JetStream publish",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: natsadapter.New(
					cfg.Nats.URL,
					cfg.Nats.Subject,
					cfg.Nats.Stream,
					cfg.Nats.CredFile,
					cfg.Nats.MaxAge,
					cfg.Nats.BackoffBase,
					cfg.Nats.BackoffCap,
					ctx.NatsEncoder,
					ctx.Logger,
				),
			}, nil
		},
	})
}
