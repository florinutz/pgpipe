package cmd

import (
	"encoding/hex"
	"fmt"

	"github.com/florinutz/pgcdc/adapter/chain"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "chain",
		Description: "Preprocessing chain (compress, encrypt) wrapping a terminal adapter",
		ConfigKey:   "chain",
		DefaultConfig: func() any {
			return &config.ChainConfig{}
		},
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg

			// Resolve the terminal adapter name.
			terminal := cfg.Chain.Terminal
			if terminal == "" {
				return registry.AdapterResult{}, fmt.Errorf("chain adapter requires --chain-terminal (the inner adapter to wrap)")
			}

			// Create the terminal adapter via the registry.
			termResult, err := registry.CreateAdapter(terminal, ctx)
			if err != nil {
				return registry.AdapterResult{}, fmt.Errorf("create chain terminal adapter %q: %w", terminal, err)
			}

			// Build the link chain from config flags.
			var links []chain.Link

			if cfg.Chain.Compress {
				links = append(links, chain.NewCompress(0))
			}

			if cfg.Chain.EncryptKey != "" {
				key, decErr := hex.DecodeString(cfg.Chain.EncryptKey)
				if decErr != nil {
					return registry.AdapterResult{}, fmt.Errorf("decode --chain-encrypt-key: %w (expected hex-encoded AES key)", decErr)
				}
				enc, encErr := chain.NewEncrypt(key)
				if encErr != nil {
					return registry.AdapterResult{}, fmt.Errorf("create encrypt link: %w", encErr)
				}
				links = append(links, enc)
			}

			if cfg.Chain.BatchSize > 0 {
				links = append(links, chain.NewBatch(cfg.Chain.BatchSize))
			}

			a := chain.New("", termResult.Adapter, ctx.Logger, links...)
			return registry.AdapterResult{
				Adapter:          a,
				MiddlewareConfig: termResult.MiddlewareConfig,
				SSEBroker:        termResult.SSEBroker,
				WSBroker:         termResult.WSBroker,
			}, nil
		},
		ViperKeys: [][2]string{
			{"chain-terminal", "chain.terminal"},
			{"chain-compress", "chain.compress"},
			{"chain-encrypt-key", "chain.encrypt_key"},
			{"chain-batch-size", "chain.batch_size"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "chain-terminal",
				Type:        "string",
				Required:    true,
				Description: "Name of the inner adapter to wrap (e.g. webhook, stdout)",
			},
			{
				Name:        "chain-compress",
				Type:        "bool",
				Default:     false,
				Description: "Enable gzip compression link",
			},
			{
				Name:        "chain-encrypt-key",
				Type:        "string",
				Description: "Hex-encoded AES-256 key for GCM encryption link (32 bytes = 64 hex chars)",
			},
			{
				Name:        "chain-batch-size",
				Type:        "int",
				Default:     0,
				Description: "Batch N events before forwarding (0 = disabled)",
				Validations: []string{"min:0"},
			},
		},
	})

	// Chain adapter flags.
	f := listenCmd.Flags()
	f.String("chain-terminal", "", "chain adapter: name of inner adapter to wrap (e.g. webhook)")
	f.Bool("chain-compress", false, "chain adapter: enable gzip compression")
	f.String("chain-encrypt-key", "", "chain adapter: hex-encoded AES-256 encryption key")
	f.Int("chain-batch-size", 0, "chain adapter: batch N events before forwarding")
}
