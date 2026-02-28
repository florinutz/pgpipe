//go:build no_kafka

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "kafka",
		Description: "Kafka topic publish (not available — built with -tags no_kafka)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("kafka adapter not available (built with -tags no_kafka)")
		},
	})
}
