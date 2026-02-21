package cmd

import (
	"embed"
	"fmt"
	"os"
	"regexp"
	"text/template"

	"github.com/spf13/cobra"
)

//go:embed sql/trigger_template.sql
var sqlFS embed.FS

var (
	validTableName   = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	validChannelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_:.\-]*$`)
)

type triggerData struct {
	Table   string
	Channel string
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate SQL trigger for LISTEN/NOTIFY on a table",
	Long: `Outputs CREATE FUNCTION and CREATE TRIGGER SQL statements that configure
a PostgreSQL table to emit LISTEN/NOTIFY events on INSERT, UPDATE, and DELETE.`,
	RunE: runInit,
}

func init() {
	initCmd.Flags().String("table", "", "table name (required)")
	initCmd.Flags().String("channel", "", "channel name (default: pgpipe:<table>)")
	_ = initCmd.MarkFlagRequired("table")
}

func runInit(cmd *cobra.Command, args []string) error {
	table, _ := cmd.Flags().GetString("table")
	channel, _ := cmd.Flags().GetString("channel")

	if !validTableName.MatchString(table) {
		return fmt.Errorf("invalid table name %q: only alphanumeric characters and underscores are allowed", table)
	}

	if channel == "" {
		channel = "pgpipe:" + table
	}

	if !validChannelName.MatchString(channel) {
		return fmt.Errorf("invalid channel name %q: only alphanumeric characters, underscores, colons, dots, and hyphens are allowed", channel)
	}

	tmplBytes, err := sqlFS.ReadFile("sql/trigger_template.sql")
	if err != nil {
		return fmt.Errorf("read embedded template: %w", err)
	}

	tmpl, err := template.New("trigger").Parse(string(tmplBytes))
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	data := triggerData{
		Table:   table,
		Channel: channel,
	}

	if err := tmpl.Execute(os.Stdout, data); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	return nil
}
