//go:build no_kafkaserver

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeKafkaServerAdapter(_ config.Config, _ checkpoint.Store, _ *slog.Logger) (adapter.Adapter, error) {
	return nil, fmt.Errorf("kafkaserver adapter not available (built with -tags no_kafkaserver)")
}
