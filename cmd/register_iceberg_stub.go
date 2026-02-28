//go:build no_iceberg

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "iceberg",
		Description: "Apache Iceberg table writes (not available — built with -tags no_iceberg)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("iceberg adapter not available (built with -tags no_iceberg)")
		},
	})
}
