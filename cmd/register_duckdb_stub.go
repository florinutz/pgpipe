//go:build no_duckdb

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "duckdb",
		Description: "DuckDB in-process analytics database (not available — built with -tags no_duckdb)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("duckdb adapter not available (built with -tags no_duckdb)")
		},
	})
}
