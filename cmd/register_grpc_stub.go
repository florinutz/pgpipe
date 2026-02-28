//go:build no_grpc

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "grpc",
		Description: "gRPC streaming server (not available — built with -tags no_grpc)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("grpc adapter not available (built with -tags no_grpc)")
		},
	})
}
