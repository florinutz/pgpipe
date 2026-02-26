//go:build !no_views

package cmd

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/florinutz/pgcdc/adapter"
	viewadapter "github.com/florinutz/pgcdc/adapter/view"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/view"
)

func makeViewAdapter(cfg config.Config, logger *slog.Logger) (adapter.Adapter, error) {
	defs, err := parseViewConfigs(cfg.Views)
	if err != nil {
		return nil, err
	}
	if len(defs) == 0 {
		return nil, fmt.Errorf("view adapter requires at least one view definition in views: config")
	}

	engine := view.NewEngine(defs, logger)
	return viewadapter.New(engine, logger), nil
}

func parseViewConfigs(views []config.ViewConfig) ([]*view.ViewDef, error) {
	var defs []*view.ViewDef
	for _, vc := range views {
		if vc.Name == "" {
			return nil, fmt.Errorf("view definition missing name")
		}
		if vc.Query == "" {
			return nil, fmt.Errorf("view %q missing query", vc.Name)
		}

		// Validate view name doesn't reference view channels.
		if strings.Contains(vc.Query, "pgcdc:_view:") {
			return nil, fmt.Errorf("view %q: WHERE clause must not reference pgcdc:_view: channels (loop prevention)", vc.Name)
		}

		var emit view.EmitMode
		switch strings.ToLower(vc.Emit) {
		case "", "row":
			emit = view.EmitRow
		case "batch":
			emit = view.EmitBatch
		default:
			return nil, fmt.Errorf("view %q: unknown emit mode %q (expected row or batch)", vc.Name, vc.Emit)
		}

		maxGroups := vc.MaxGroups
		if maxGroups <= 0 {
			maxGroups = 100000
		}

		def, err := view.Parse(vc.Name, vc.Query, emit, maxGroups)
		if err != nil {
			return nil, fmt.Errorf("view %q: %w", vc.Name, err)
		}
		defs = append(defs, def)
	}
	return defs, nil
}
