//go:build no_arrow

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "arrow",
		Description: "Apache Arrow Flight gRPC server (not available — built with -tags no_arrow)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("arrow adapter not available (built with -tags no_arrow)")
		},
	})
}
