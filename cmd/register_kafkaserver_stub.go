//go:build no_kafkaserver

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "kafkaserver",
		Description: "Kafka wire protocol server (not available — built with -tags no_kafkaserver)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("kafkaserver adapter not available (built with -tags no_kafkaserver)")
		},
	})
}
