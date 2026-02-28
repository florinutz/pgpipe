package cmd

import (
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "listen_notify",
		Description: "PostgreSQL LISTEN/NOTIFY",
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			det := listennotify.New(cfg.DatabaseURL, cfg.Channels, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, ctx.Logger)
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
