Run the full local CI gate (mirrors what GitHub Actions would check).

## Steps

Run each check sequentially, stopping on first failure:

1. **Format check**:
   ```
   make fmt
   ```
2. **Vet**:
   ```
   make vet
   ```
3. **Lint** (if golangci-lint is installed):
   ```
   make lint
   ```
4. **Unit tests**:
   ```
   make test
   ```
5. **Coverage**:
   ```
   make coverage
   ```
6. **Scenario tests** (requires Docker):
   ```
   make test-scenarios
   ```
7. **Docker build**:
   ```
   make docker-build
   ```
8. Report pass/fail for each step. If any step fails, stop and report the failure details.
