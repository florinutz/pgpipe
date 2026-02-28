//go:build no_redis

package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "redis",
		Description: "Redis cache invalidation / sync (not available — built with -tags no_redis)",
		Create: func(_ registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{}, fmt.Errorf("redis adapter not available (built with -tags no_redis)")
		},
	})
}
