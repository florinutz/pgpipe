package cmd

import (
	"fmt"

	mongodbdetector "github.com/florinutz/pgcdc/detector/mongodb"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mongodb",
		Description: "MongoDB Change Streams",
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if cfg.MongoDB.URI == "" {
				return registry.DetectorResult{}, fmt.Errorf("--mongodb-uri is required for MongoDB detector")
			}
			if cfg.MongoDB.Scope != "cluster" && cfg.MongoDB.Database == "" {
				return registry.DetectorResult{}, fmt.Errorf("--mongodb-database is required for MongoDB detector (unless --mongodb-scope=cluster)")
			}
			det := mongodbdetector.New(
				cfg.MongoDB.URI, cfg.MongoDB.Scope, cfg.MongoDB.Database,
				cfg.MongoDB.Collections, cfg.MongoDB.FullDocument,
				cfg.MongoDB.MetadataDB, cfg.MongoDB.MetadataColl,
				cfg.MongoDB.BackoffBase, cfg.MongoDB.BackoffCap,
				ctx.Logger,
			)
			if ctx.TracerProvider != nil {
				det.SetTracer(ctx.TracerProvider.Tracer("pgcdc"))
			}
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
