package cmd

import (
	"github.com/florinutz/pgcdc/detector/outbox"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "outbox",
		Description: "Transactional outbox table polling",
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			det := outbox.New(
				cfg.DatabaseURL,
				cfg.Outbox.Table,
				cfg.Outbox.PollInterval,
				cfg.Outbox.BatchSize,
				cfg.Outbox.KeepProcessed,
				cfg.Outbox.BackoffBase,
				cfg.Outbox.BackoffCap,
				ctx.Logger,
			)
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
