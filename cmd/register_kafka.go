//go:build !no_kafka

package cmd

import (
	kafkaadapter "github.com/florinutz/pgcdc/adapter/kafka"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "kafka",
		Description: "Kafka topic publish",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: kafkaadapter.New(
					cfg.Kafka.Brokers,
					cfg.Kafka.Topic,
					cfg.Kafka.SASLMechanism,
					cfg.Kafka.SASLUsername,
					cfg.Kafka.SASLPassword,
					cfg.Kafka.TLSCAFile,
					cfg.Kafka.TLS,
					cfg.Kafka.BackoffBase,
					cfg.Kafka.BackoffCap,
					ctx.KafkaEncoder,
					ctx.Logger,
					cfg.Kafka.TransactionalID,
					cfg.Kafka.CBMaxFailures,
					cfg.Kafka.CBResetTimeout,
					cfg.Kafka.RateLimit,
					cfg.Kafka.RateLimitBurst,
				),
			}, nil
		},
	})
}
