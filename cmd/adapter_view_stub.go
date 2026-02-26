//go:build no_views

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeViewAdapter(_ config.Config, _ *slog.Logger) (adapter.Adapter, error) {
	return nil, fmt.Errorf("view adapter not available (built with -tags no_views)")
}
