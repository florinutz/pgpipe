//go:build no_views

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "view",
		Description: "Streaming SQL view engine (not available — built with -tags no_views)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("view adapter not available (built with -tags no_views)")
		},
	})
}
