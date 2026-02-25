//go:build !no_s3

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	s3adapter "github.com/florinutz/pgcdc/adapter/s3"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeS3Adapter(cfg config.Config, logger *slog.Logger) (adapter.Adapter, error) {
	return s3adapter.New(
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
		logger,
	), nil
}
