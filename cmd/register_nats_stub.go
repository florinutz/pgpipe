//go:build no_nats

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "nats",
		Description: "NATS JetStream publish (not available — built with -tags no_nats)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("nats adapter not available (built with -tags no_nats)")
		},
	})
}
