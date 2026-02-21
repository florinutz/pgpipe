Analyze test coverage to decide if a new test is needed.

## Steps

1. Run unit test coverage:
   ```
   go test -cover -count=1 ./internal/...
   ```
2. Run scenario test coverage:
   ```
   go test -cover -count=1 -tags=integration -timeout=300s ./scenarios/...
   ```
3. For detailed per-function coverage, run:
   ```
   go test -coverprofile=cover.out -count=1 ./internal/...
   go tool cover -func=cover.out
   rm cover.out
   ```
4. Report per-package coverage numbers
5. Identify packages below 60% coverage
6. Cross-reference with `SCENARIOS.md` — are the uncovered paths tested by scenarios?
7. Recommend whether a unit test or scenario test is needed:
   - Pure function with no I/O → unit test
   - Touches channels, network, or goroutines → scenario test
   - Already covered by existing scenario → no test needed
8. Clean up any generated coverage files
