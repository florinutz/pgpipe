package cmd

import (
	"fmt"
	"text/tabwriter"

	"github.com/florinutz/pgcdc/registry"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list [adapters|detectors|transforms]",
	Short: "List available components",
	Long:  `List all registered adapters, detectors, or transforms.`,
	Args:  cobra.MaximumNArgs(1),
	RunE:  runList,
}

func init() {
	rootCmd.AddCommand(listCmd)
}

func runList(cmd *cobra.Command, args []string) error {
	kind := "all"
	if len(args) > 0 {
		kind = args[0]
	}

	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)

	switch kind {
	case "adapters", "adapter":
		_, _ = fmt.Fprintln(w, "NAME\tDESCRIPTION")
		for _, e := range registry.Adapters() {
			_, _ = fmt.Fprintf(w, "%s\t%s\n", e.Name, e.Description)
		}

	case "detectors", "detector":
		_, _ = fmt.Fprintln(w, "NAME\tDESCRIPTION")
		for _, e := range registry.Detectors() {
			_, _ = fmt.Fprintf(w, "%s\t%s\n", e.Name, e.Description)
		}

	case "transforms", "transform":
		_, _ = fmt.Fprintln(w, "NAME\tDESCRIPTION")
		for _, e := range registry.Transforms() {
			_, _ = fmt.Fprintf(w, "%s\t%s\n", e.Name, e.Description)
		}

	case "all":
		_, _ = fmt.Fprintln(w, "ADAPTERS")
		_, _ = fmt.Fprintln(w, "NAME\tDESCRIPTION")
		for _, e := range registry.Adapters() {
			_, _ = fmt.Fprintf(w, "%s\t%s\n", e.Name, e.Description)
		}
		_, _ = fmt.Fprintln(w)

		_, _ = fmt.Fprintln(w, "DETECTORS")
		_, _ = fmt.Fprintln(w, "NAME\tDESCRIPTION")
		for _, e := range registry.Detectors() {
			_, _ = fmt.Fprintf(w, "%s\t%s\n", e.Name, e.Description)
		}
		_, _ = fmt.Fprintln(w)

		_, _ = fmt.Fprintln(w, "TRANSFORMS")
		_, _ = fmt.Fprintln(w, "NAME\tDESCRIPTION")
		for _, e := range registry.Transforms() {
			_, _ = fmt.Fprintf(w, "%s\t%s\n", e.Name, e.Description)
		}

	default:
		return fmt.Errorf("unknown component type %q: expected adapters, detectors, or transforms", kind)
	}

	return w.Flush()
}
