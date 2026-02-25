//go:build !no_kafkaserver

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	kafkaserveradapter "github.com/florinutz/pgcdc/adapter/kafkaserver"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeKafkaServerAdapter(cfg config.Config, cpStore checkpoint.Store, logger *slog.Logger) (adapter.Adapter, error) {
	return kafkaserveradapter.New(
		cfg.KafkaServer.Addr,
		cfg.KafkaServer.PartitionCount,
		cfg.KafkaServer.BufferSize,
		cfg.KafkaServer.SessionTimeout,
		cfg.KafkaServer.KeyColumn,
		cpStore,
		logger,
	), nil
}
