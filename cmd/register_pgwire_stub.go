//go:build no_pgwire

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "pgwire",
		Description: "PostgreSQL wire protocol gateway (not available — built with -tags no_pgwire)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("pgwire adapter not available (built with -tags no_pgwire)")
		},
	})
}
