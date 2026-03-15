package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/internal/migrate"
	"github.com/florinutz/pgcdc/internal/output"
	"github.com/florinutz/pgcdc/internal/prompt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

type rewindResult struct {
	SlotName    string `json:"slot_name"`
	PreviousLSN string `json:"previous_lsn"`
	NewLSN      string `json:"new_lsn"`
	Method      string `json:"method"`
}

var rewindCmd = &cobra.Command{
	Use:   "rewind",
	Short: "Rewind a checkpoint to an earlier WAL position",
	Long: `Rewind a pipeline's checkpoint to an earlier LSN so that events are replayed
on the next startup. Useful when a pipeline processed bad data or you need to
replay from a specific point.

Exactly one of --lsn, --ago, or --to-beginning must be specified.

Examples:
  pgcdc rewind --slot-name my_slot --lsn 0/1234ABCD
  pgcdc rewind --slot-name my_slot --ago 2h
  pgcdc rewind --slot-name my_slot --to-beginning
  pgcdc rewind --slot-name my_slot --lsn 0/1234ABCD --force`,
	RunE: runRewind,
}

func init() {
	f := rewindCmd.Flags()
	f.String("slot-name", "", "replication slot name (required)")
	_ = rewindCmd.MarkFlagRequired("slot-name")

	f.String("lsn", "", "target WAL LSN in hex format (e.g. 0/1234ABCD)")
	f.Duration("ago", 0, "rewind by duration (uses slot's restart_lsn as target)")
	f.Bool("to-beginning", false, "rewind to the slot's restart_lsn")

	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	f.String("checkpoint-db", "", "checkpoint database connection string (overrides --db)")
	f.Bool("force", false, "skip confirmation prompt")

	output.AddOutputFlag(rewindCmd)
	rootCmd.AddCommand(rewindCmd)
}

func runRewind(cmd *cobra.Command, _ []string) error {
	slotName, _ := cmd.Flags().GetString("slot-name")
	lsnStr, _ := cmd.Flags().GetString("lsn")
	ago, _ := cmd.Flags().GetDuration("ago")
	toBeginning, _ := cmd.Flags().GetBool("to-beginning")
	force, _ := cmd.Flags().GetBool("force")

	// Validate exactly one target mode is specified.
	modeCount := 0
	if lsnStr != "" {
		modeCount++
	}
	if ago > 0 {
		modeCount++
	}
	if toBeginning {
		modeCount++
	}
	if modeCount == 0 {
		return fmt.Errorf("exactly one of --lsn, --ago, or --to-beginning must be specified")
	}
	if modeCount > 1 {
		return fmt.Errorf("only one of --lsn, --ago, or --to-beginning may be specified")
	}

	// Resolve database URLs.
	dbURL := rewindDBURL(cmd, "db")
	checkpointDBURL := rewindDBURL(cmd, "checkpoint-db")
	if checkpointDBURL == "" {
		checkpointDBURL = dbURL
	}
	if checkpointDBURL == "" {
		return fmt.Errorf("no database URL specified; use --db or --checkpoint-db or export PGCDC_DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = checkpointDBURL
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
	defer cancel()

	logger := slog.Default().With("component", "rewind")

	// Run migrations to ensure pgcdc_checkpoints table exists.
	if err := migrate.Run(ctx, checkpointDBURL, logger); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	// Open checkpoint store.
	store, err := checkpoint.NewPGStore(ctx, checkpointDBURL, logger)
	if err != nil {
		return fmt.Errorf("open checkpoint store: %w", err)
	}
	defer func() { _ = store.Close() }()

	// Load current checkpoint.
	currentLSN, err := store.Load(ctx, slotName)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Determine target LSN.
	var targetLSN uint64
	var method string

	switch {
	case lsnStr != "":
		parsed, parseErr := pglogrepl.ParseLSN(lsnStr)
		if parseErr != nil {
			return fmt.Errorf("parse LSN %q: %w", lsnStr, parseErr)
		}
		targetLSN = uint64(parsed)
		method = "lsn"

	case ago > 0:
		restartLSN, lookupErr := queryRestartLSN(ctx, dbURL, slotName)
		if lookupErr != nil {
			return lookupErr
		}
		targetLSN = restartLSN
		method = "ago"
		logger.Info("--ago maps to slot restart_lsn (WAL has no time-based addressing)",
			"ago", ago.String(), "restart_lsn", pglogrepl.LSN(restartLSN).String())

	case toBeginning:
		restartLSN, lookupErr := queryRestartLSN(ctx, dbURL, slotName)
		if lookupErr != nil {
			return lookupErr
		}
		targetLSN = restartLSN
		method = "to-beginning"
	}

	// Safety check: no fast-forwarding.
	if currentLSN > 0 && targetLSN > currentLSN {
		return fmt.Errorf("target LSN %s is ahead of current checkpoint %s; rewinding forward is not supported",
			pglogrepl.LSN(targetLSN).String(), pglogrepl.LSN(currentLSN).String())
	}

	if currentLSN > 0 && targetLSN == currentLSN {
		return fmt.Errorf("target LSN %s is the same as current checkpoint; nothing to do",
			pglogrepl.LSN(targetLSN).String())
	}

	// Confirmation prompt.
	currentStr := pglogrepl.LSN(currentLSN).String()
	if currentLSN == 0 {
		currentStr = "(none)"
	}
	targetStr := pglogrepl.LSN(targetLSN).String()

	if !force {
		p := prompt.New(os.Stdin, os.Stderr)
		msg := fmt.Sprintf("Rewind slot %q from %s to %s?", slotName, currentStr, targetStr)
		confirmed, confirmErr := p.Confirm(msg, false)
		if confirmErr != nil {
			return fmt.Errorf("confirm: %w", confirmErr)
		}
		if !confirmed {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Rewind cancelled.")
			return nil
		}
	}

	// Save new checkpoint.
	if err := store.Save(ctx, slotName, targetLSN); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	logger.Info("checkpoint rewound", "slot", slotName,
		"from", currentStr, "to", targetStr, "method", method)

	// Output result.
	result := rewindResult{
		SlotName:    slotName,
		PreviousLSN: currentStr,
		NewLSN:      targetStr,
		Method:      method,
	}

	printer := output.FromCommand(cmd)
	if printer.IsJSON() {
		return printer.JSON(result)
	}

	w := printer.Writer()
	_, _ = fmt.Fprintf(w, "Slot:         %s\n", result.SlotName)
	_, _ = fmt.Fprintf(w, "Previous LSN: %s\n", result.PreviousLSN)
	_, _ = fmt.Fprintf(w, "New LSN:      %s\n", result.NewLSN)
	_, _ = fmt.Fprintf(w, "Method:       %s\n", result.Method)

	return nil
}

func rewindDBURL(cmd *cobra.Command, flagName string) string {
	db, _ := cmd.Flags().GetString(flagName)
	if db != "" {
		return db
	}
	if flagName == "db" {
		if v := os.Getenv("PGCDC_DATABASE_URL"); v != "" {
			return v
		}
	}
	return ""
}

// queryRestartLSN fetches the restart_lsn of a replication slot from pg_replication_slots.
func queryRestartLSN(ctx context.Context, dbURL, slotName string) (uint64, error) {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return 0, fmt.Errorf("connect to query slot: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var restartLSN *string
	err = conn.QueryRow(ctx,
		"SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&restartLSN)
	if err == pgx.ErrNoRows {
		return 0, fmt.Errorf("replication slot %q not found in pg_replication_slots", slotName)
	}
	if err != nil {
		return 0, fmt.Errorf("query restart_lsn: %w", err)
	}
	if restartLSN == nil {
		return 0, fmt.Errorf("replication slot %q has no restart_lsn (slot may not have been used yet)", slotName)
	}

	parsed, parseErr := pglogrepl.ParseLSN(*restartLSN)
	if parseErr != nil {
		return 0, fmt.Errorf("parse restart_lsn %q: %w", *restartLSN, parseErr)
	}
	return uint64(parsed), nil
}
