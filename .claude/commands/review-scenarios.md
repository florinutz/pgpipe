Audit the scenario registry for consistency with actual test files.

## Steps

1. Read `SCENARIOS.md` and extract the registered scenarios
2. List actual scenario test files:
   ```
   ls scenarios/*_test.go
   ```
3. Check bidirectional consistency:
   - Every file in `scenarios/` (except `helpers_test.go`) should be registered in SCENARIOS.md
   - Every file listed in SCENARIOS.md should exist in `scenarios/`
4. For each scenario file, verify:
   - Has `//go:build integration` build tag
   - Top-level test follows `TestScenario_<Name>` naming
   - Has at least `t.Run("happy path", ...)` subtest
5. Count total scenarios and warn if approaching 20 (consolidation needed)
6. Report any inconsistencies found
