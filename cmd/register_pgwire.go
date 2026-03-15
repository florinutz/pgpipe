//go:build !no_pgwire

package cmd

import (
	pgwireadapter "github.com/florinutz/pgcdc/adapter/pgwire"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "pgwire",
		Description: "PostgreSQL wire protocol gateway (query CDC events with psql)",
		ConfigKey:   "pgwire",
		DefaultConfig: func() any {
			return &config.PGWireConfig{
				Addr:       ":5433",
				BufferSize: 10000,
			}
		},
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: pgwireadapter.New(
					cfg.PGWire.Addr,
					cfg.PGWire.BufferSize,
					cfg.PGWire.Password,
					cfg.PGWire.TLSCert,
					cfg.PGWire.TLSKey,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"pgwire-addr", "pgwire.addr"},
			{"pgwire-buffer", "pgwire.buffer_size"},
			{"pgwire-password", "pgwire.password"},
			{"pgwire-tls-cert", "pgwire.tls_cert"},
			{"pgwire-tls-key", "pgwire.tls_key"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "pgwire-addr",
				Type:        "string",
				Default:     ":5433",
				Description: "PGWire listen address",
			},
			{
				Name:        "pgwire-buffer",
				Type:        "int",
				Default:     10000,
				Description: "Ring buffer size for events",
				Validations: []string{"min:1"},
			},
			{
				Name:        "pgwire-password",
				Type:        "string",
				Description: "Optional password for client authentication",
			},
			{
				Name:        "pgwire-tls-cert",
				Type:        "string",
				Description: "TLS certificate file for encrypted connections",
			},
			{
				Name:        "pgwire-tls-key",
				Type:        "string",
				Description: "TLS key file for encrypted connections",
			},
		},
	})

	// PGWire adapter flags.
	f := listenCmd.Flags()
	f.String("pgwire-addr", ":5433", "PGWire listen address")
	f.Int("pgwire-buffer", 10000, "PGWire ring buffer size")
	f.String("pgwire-password", "", "PGWire password for client authentication")
	f.String("pgwire-tls-cert", "", "PGWire TLS certificate file")
	f.String("pgwire-tls-key", "", "PGWire TLS key file")
}
