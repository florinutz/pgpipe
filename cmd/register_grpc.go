//go:build !no_grpc

package cmd

import (
	grpcadapter "github.com/florinutz/pgcdc/adapter/grpc"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "grpc",
		Description: "gRPC streaming server",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: grpcadapter.New(cfg.GRPC.Addr, ctx.Logger),
			}, nil
		},
		ViperKeys: [][2]string{
			{"grpc-addr", "grpc.addr"},
		},
	})

	// gRPC adapter flags.
	listenCmd.Flags().String("grpc-addr", ":9090", "gRPC server listen address")
}
