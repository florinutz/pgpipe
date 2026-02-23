Add a new CLI subcommand to pgcdc.

## Steps

1. Read `cmd/root.go` to understand the command registration pattern
2. Read `cmd/init.go` as a reference for a simple subcommand
3. Create `cmd/$ARGUMENTS.go` with:
   - Package `cmd`
   - A `var <name>Cmd = &cobra.Command{...}` with Use, Short, Long, RunE
   - An `init()` function that:
     - Adds flags via `<name>Cmd.Flags()`
     - Binds to viper with `mustBindPFlag()` if config-file support is needed
     - Calls `rootCmd.AddCommand(<name>Cmd)`
   - A `run<Name>(cmd *cobra.Command, args []string) error` function
4. Follow conventions:
   - Error wrapping: `fmt.Errorf("verb: %w", err)`
   - Use `slog.Default()` for logging if needed
   - Validation errors return early with descriptive messages
5. If the command has observable output behavior, write a scenario test
6. Run `make test-all` — all tests must pass
