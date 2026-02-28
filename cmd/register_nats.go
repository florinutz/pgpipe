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
		ViperKeys: [][2]string{
			{"nats-url", "nats.url"},
			{"nats-subject", "nats.subject"},
			{"nats-stream", "nats.stream"},
			{"nats-cred-file", "nats.cred_file"},
			{"nats-max-age", "nats.max_age"},
			{"nats-encoding", "nats.encoding"},
			// schema-registry-* flags are registered as core/shared flags in listen.go init()
			// to handle the case where kafka is built out (no_kafka) but nats is not.
		},
	})

	// NATS adapter flags.
	f := listenCmd.Flags()
	f.String("nats-url", "nats://localhost:4222", "NATS server URL")
	f.String("nats-subject", "pgcdc", "NATS subject prefix")
	f.String("nats-stream", "pgcdc", "NATS JetStream stream name")
	f.String("nats-cred-file", "", "NATS credentials file")
	f.Duration("nats-max-age", 0, "NATS stream max message age (default 24h)")
	f.String("nats-encoding", "json", "NATS message encoding: json, avro, or protobuf")
}
