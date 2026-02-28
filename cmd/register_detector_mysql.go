package cmd

import (
	"fmt"

	mysqldetector "github.com/florinutz/pgcdc/detector/mysql"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mysql",
		Description: "MySQL binlog replication",
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if cfg.MySQL.ServerID == 0 {
				return registry.DetectorResult{}, fmt.Errorf("--mysql-server-id is required and must be > 0 for MySQL detector")
			}
			if cfg.MySQL.Addr == "" {
				return registry.DetectorResult{}, fmt.Errorf("--mysql-addr is required for MySQL detector")
			}
			det := mysqldetector.New(
				cfg.MySQL.Addr, cfg.MySQL.User, cfg.MySQL.Password,
				cfg.MySQL.ServerID, cfg.MySQL.Tables, cfg.MySQL.UseGTID,
				cfg.MySQL.Flavor, cfg.MySQL.BinlogPrefix,
				cfg.MySQL.BackoffBase, cfg.MySQL.BackoffCap,
				ctx.Logger,
			)
			if ctx.TracerProvider != nil {
				det.SetTracer(ctx.TracerProvider.Tracer("pgcdc"))
			}
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
