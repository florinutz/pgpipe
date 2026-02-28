package cmd

import (
	embeddingadapter "github.com/florinutz/pgcdc/adapter/embedding"
	execadapter "github.com/florinutz/pgcdc/adapter/exec"
	fileadapter "github.com/florinutz/pgcdc/adapter/file"
	"github.com/florinutz/pgcdc/adapter/pgtable"
	searchadapter "github.com/florinutz/pgcdc/adapter/search"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "stdout",
		Description: "JSON-lines to stdout",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			return registry.AdapterResult{
				Adapter: stdout.New(nil, ctx.Logger),
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "webhook",
		Description: "HTTP POST to a webhook URL",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			a := webhook.New(
				cfg.Webhook.URL,
				cfg.Webhook.Headers,
				cfg.Webhook.SigningKey,
				cfg.Webhook.MaxRetries,
				cfg.Webhook.Timeout,
				cfg.Webhook.BackoffBase,
				cfg.Webhook.BackoffCap,
				ctx.Logger,
			)
			return registry.AdapterResult{
				Adapter: a,
				Extra: map[string]any{
					"middleware_config": webhookMiddlewareConfig(cfg),
				},
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "sse",
		Description: "Server-Sent Events broker",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			broker := sse.New(cfg.Bus.BufferSize, cfg.SSE.HeartbeatInterval, ctx.Logger)
			return registry.AdapterResult{
				Adapter: broker,
				Extra:   map[string]any{"sse_broker": broker},
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "file",
		Description: "JSON-lines to file with rotation",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: fileadapter.New(cfg.File.Path, cfg.File.MaxSize, cfg.File.MaxFiles, ctx.Logger),
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "exec",
		Description: "JSON-lines to subprocess stdin",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: execadapter.New(cfg.Exec.Command, cfg.Exec.BackoffBase, cfg.Exec.BackoffCap, ctx.Logger),
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "pg_table",
		Description: "INSERT into PostgreSQL table",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			pgTableURL := cfg.PGTable.URL
			if pgTableURL == "" {
				pgTableURL = cfg.DatabaseURL
			}
			return registry.AdapterResult{
				Adapter: pgtable.New(pgTableURL, cfg.PGTable.Table, cfg.PGTable.BackoffBase, cfg.PGTable.BackoffCap, ctx.Logger),
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "ws",
		Description: "WebSocket broker",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			broker := ws.New(cfg.Bus.BufferSize, cfg.WebSocket.PingInterval, ctx.Logger)
			return registry.AdapterResult{
				Adapter: broker,
				Extra:   map[string]any{"ws_broker": broker},
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "embedding",
		Description: "Embed text columns into pgvector table",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			embDBURL := cfg.Embedding.DBURL
			if embDBURL == "" {
				embDBURL = cfg.DatabaseURL
			}
			return registry.AdapterResult{
				Adapter: embeddingadapter.New(
					cfg.Embedding.APIURL,
					cfg.Embedding.APIKey,
					cfg.Embedding.Model,
					cfg.Embedding.Columns,
					cfg.Embedding.IDColumn,
					embDBURL,
					cfg.Embedding.Table,
					cfg.Embedding.Dimension,
					cfg.Embedding.MaxRetries,
					cfg.Embedding.Timeout,
					cfg.Embedding.BackoffBase,
					cfg.Embedding.BackoffCap,
					cfg.Embedding.SkipUnchanged,
					cfg.Embedding.CBMaxFailures,
					cfg.Embedding.CBResetTimeout,
					cfg.Embedding.RateLimit,
					cfg.Embedding.RateLimitBurst,
					ctx.Logger,
				),
			}, nil
		},
	})

	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "search",
		Description: "Typesense / Meilisearch sync",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: searchadapter.New(
					cfg.Search.Engine,
					cfg.Search.URL,
					cfg.Search.APIKey,
					cfg.Search.Index,
					cfg.Search.IDColumn,
					cfg.Search.BatchSize,
					cfg.Search.BatchInterval,
					cfg.Search.BackoffBase,
					cfg.Search.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
	})
}
