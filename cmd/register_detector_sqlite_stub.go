//go:build no_sqlite

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "sqlite",
		Description: "SQLite change tracking (not available — built with -tags no_sqlite)",
		Create: func(_ registry.DetectorContext) (registry.DetectorResult, error) {
			return registry.DetectorResult{}, fmt.Errorf("sqlite detector not available (built with -tags no_sqlite)")
		},
	})
}
