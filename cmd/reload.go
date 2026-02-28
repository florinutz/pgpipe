package cmd

import (
	"log/slog"
	"strings"

	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/transform"
	"github.com/spf13/cobra"
)

// pluginTransforms holds raw transform funcs from plugins, separated by scope,
// so the SIGHUP handler can include them in every Reload() call.
type pluginTransforms struct {
	global  []transform.TransformFunc
	adapter map[string][]transform.TransformFunc
}

// buildCLITransforms returns global transforms derived from CLI flags (immutable).
func buildCLITransforms(cmd *cobra.Command) []transform.TransformFunc {
	var fns []transform.TransformFunc
	if cols, _ := cmd.Flags().GetStringSlice("drop-columns"); len(cols) > 0 {
		fns = append(fns, transform.DropColumns(cols...))
	}
	if ops, _ := cmd.Flags().GetStringSlice("filter-operations"); len(ops) > 0 {
		fns = append(fns, transform.FilterOperation(ops...))
	}
	if ok, _ := cmd.Flags().GetBool("debezium-envelope"); ok {
		var dopts []transform.DebeziumOption
		if name, _ := cmd.Flags().GetString("debezium-connector-name"); name != "" && name != "pgcdc" {
			dopts = append(dopts, transform.WithConnectorName(name))
		}
		if db, _ := cmd.Flags().GetString("debezium-database"); db != "" {
			dopts = append(dopts, transform.WithDatabase(db))
		}
		fns = append(fns, transform.Debezium(dopts...))
	}
	if ok, _ := cmd.Flags().GetBool("cloudevents-envelope"); ok {
		var copts []transform.CloudEventsOption
		if src, _ := cmd.Flags().GetString("cloudevents-source"); src != "" && src != "/pgcdc" {
			copts = append(copts, transform.WithSource(src))
		}
		if tp, _ := cmd.Flags().GetString("cloudevents-type-prefix"); tp != "" && tp != "io.pgcdc.change" {
			copts = append(copts, transform.WithTypePrefix(tp))
		}
		fns = append(fns, transform.CloudEvents(copts...))
	}
	return fns
}

// buildYAMLTransforms returns global and per-adapter transforms from config (reloadable).
func buildYAMLTransforms(cfg config.Config) (global []transform.TransformFunc, adapter map[string][]transform.TransformFunc) {
	adapter = make(map[string][]transform.TransformFunc)
	for _, spec := range cfg.Transforms.Global {
		if fn := specToTransform(spec); fn != nil {
			global = append(global, fn)
		}
	}
	for adapterName, specs := range cfg.Transforms.Adapter {
		for _, spec := range specs {
			if fn := specToTransform(spec); fn != nil {
				adapter[adapterName] = append(adapter[adapterName], fn)
			}
		}
	}
	return global, adapter
}

// buildCLIRoutes returns routes from CLI --route flags (immutable).
func buildCLIRoutes(cmd *cobra.Command) map[string][]string {
	routeFlags, _ := cmd.Flags().GetStringSlice("route")
	if len(routeFlags) == 0 {
		return nil
	}
	routes := make(map[string][]string, len(routeFlags))
	for _, r := range routeFlags {
		parts := strings.SplitN(r, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			continue
		}
		routes[parts[0]] = strings.Split(parts[1], ",")
	}
	return routes
}

// mergeRoutes merges CLI routes with YAML routes. CLI routes win for the same adapter name.
func mergeRoutes(cli, yaml map[string][]string) map[string][]string {
	if len(cli) == 0 && len(yaml) == 0 {
		return nil
	}
	merged := make(map[string][]string)
	for k, v := range yaml {
		merged[k] = v
	}
	for k, v := range cli {
		merged[k] = v // CLI wins
	}
	return merged
}

// specToTransform converts a config TransformSpec into a TransformFunc
// using the component registry.
func specToTransform(spec config.TransformSpec) transform.TransformFunc {
	fn := registry.SpecToTransform(spec)
	if fn == nil && spec.Type != "" {
		slog.Warn("unknown transform type in config, skipping", "type", spec.Type)
		metrics.ConfigReloadErrors.Inc()
	}
	return fn
}

// buildTransformOpts builds global and per-adapter transforms from CLI flags and YAML config.
func buildTransformOpts(cfg config.Config, cmd *cobra.Command) ([]transform.TransformFunc, map[string][]transform.TransformFunc) {
	cliTransforms := buildCLITransforms(cmd)
	yamlGlobal, yamlAdapter := buildYAMLTransforms(cfg)
	global := make([]transform.TransformFunc, 0, len(cliTransforms)+len(yamlGlobal))
	global = append(global, cliTransforms...)
	global = append(global, yamlGlobal...)
	return global, yamlAdapter
}
