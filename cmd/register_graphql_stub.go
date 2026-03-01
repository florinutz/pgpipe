//go:build no_graphql

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "graphql",
		Description: "GraphQL subscriptions over WebSocket (not available — built with -tags no_graphql)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("graphql adapter not available (built with -tags no_graphql)")
		},
	})
}
