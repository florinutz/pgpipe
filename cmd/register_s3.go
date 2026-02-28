//go:build !no_s3

package cmd

import (
	s3adapter "github.com/florinutz/pgcdc/adapter/s3"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "s3",
		Description: "S3-compatible object storage (JSON Lines / Parquet)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: s3adapter.New(
					cfg.S3.Bucket,
					cfg.S3.Prefix,
					cfg.S3.Endpoint,
					cfg.S3.Region,
					cfg.S3.AccessKeyID,
					cfg.S3.SecretAccessKey,
					cfg.S3.Format,
					cfg.S3.FlushInterval,
					cfg.S3.FlushSize,
					cfg.S3.DrainTimeout,
					cfg.S3.BackoffBase,
					cfg.S3.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
	})
}
