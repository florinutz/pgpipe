//go:build !no_iceberg

package cmd

import (
	icebergadapter "github.com/florinutz/pgcdc/adapter/iceberg"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "iceberg",
		Description: "Apache Iceberg table writes (Hadoop catalog, Parquet)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: icebergadapter.New(
					cfg.Iceberg.CatalogType,
					cfg.Iceberg.CatalogURI,
					cfg.Iceberg.Warehouse,
					cfg.Iceberg.Namespace,
					cfg.Iceberg.Table,
					cfg.Iceberg.Mode,
					cfg.Iceberg.SchemaMode,
					cfg.Iceberg.PrimaryKeys,
					cfg.Iceberg.FlushInterval,
					cfg.Iceberg.FlushSize,
					cfg.Iceberg.BackoffBase,
					cfg.Iceberg.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
	})
}
