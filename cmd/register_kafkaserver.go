//go:build !no_kafkaserver

package cmd

import (
	"fmt"

	kafkaserveradapter "github.com/florinutz/pgcdc/adapter/kafkaserver"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "kafkaserver",
		Description: "Kafka wire protocol server (no Kafka cluster needed)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg

			// Create checkpoint store for offset persistence.
			var cpStore checkpoint.Store
			ksCheckpointDB := cfg.KafkaServer.CheckpointDB
			if ksCheckpointDB == "" {
				ksCheckpointDB = cfg.DatabaseURL
			}
			if ksCheckpointDB != "" {
				var err error
				cpStore, err = checkpoint.NewPGStore(ctx.Ctx, ksCheckpointDB, ctx.Logger)
				if err != nil {
					return registry.AdapterResult{}, fmt.Errorf("create kafkaserver checkpoint store: %w", err)
				}
			}

			return registry.AdapterResult{
				Adapter: kafkaserveradapter.New(
					cfg.KafkaServer.Addr,
					cfg.KafkaServer.PartitionCount,
					cfg.KafkaServer.BufferSize,
					cfg.KafkaServer.SessionTimeout,
					cfg.KafkaServer.KeyColumn,
					cpStore,
					ctx.Logger,
				),
			}, nil
		},
	})
}
