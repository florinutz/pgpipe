//go:build no_s3

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "s3",
		Description: "S3-compatible object storage (not available — built with -tags no_s3)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("s3 adapter not available (built with -tags no_s3)")
		},
	})
}
