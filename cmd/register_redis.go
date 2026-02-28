//go:build !no_redis

package cmd

import (
	redisadapter "github.com/florinutz/pgcdc/adapter/redis"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "redis",
		Description: "Redis cache invalidation / sync",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: redisadapter.New(
					cfg.Redis.URL,
					cfg.Redis.Mode,
					cfg.Redis.KeyPrefix,
					cfg.Redis.IDColumn,
					cfg.Redis.BackoffBase,
					cfg.Redis.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
	})
}
