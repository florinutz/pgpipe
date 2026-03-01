//go:build !no_arrow

package cmd

import (
	arrowadapter "github.com/florinutz/pgcdc/adapter/arrow"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "arrow",
		Description: "Apache Arrow Flight gRPC server for analytical queries",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: arrowadapter.New(
					cfg.Arrow.Addr,
					cfg.Arrow.BufferSize,
					nil,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"arrow-addr", "arrow.addr"},
			{"arrow-buffer-size", "arrow.buffer_size"},
		},
		Spec: []registry.ParamSpec{
			{Name: "arrow-addr", Type: "string", Default: ":8815", Description: "Arrow Flight gRPC server listen address"},
			{Name: "arrow-buffer-size", Type: "int", Default: 10000, Description: "Per-channel event buffer size"},
		},
	})

	f := listenCmd.Flags()
	f.String("arrow-addr", ":8815", "Arrow Flight gRPC server listen address")
	f.Int("arrow-buffer-size", 10000, "Arrow Flight per-channel event buffer size")
}
