## Summary

<!-- What does this PR do, and why? Link to the issue it closes if applicable. -->

Closes #

## Type of Change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Documentation update
- [ ] Refactor (no functional change)
- [ ] Test (adds or improves tests only)
- [ ] Chore (dependency update, CI, tooling)

## Testing

<!-- Describe what tests were added or updated. Paste the relevant `go test` output below. -->

**Tests added/updated:**

**How to verify:**

```
make check
```

## Checklist

- [ ] All existing tests pass (`make check`)
- [ ] Race detector is clean (`go test -race ./...`)
- [ ] Coverage is maintained or improved (`make coverage`)
- [ ] Docs updated (README, ADR, runbook) if the change affects behavior or architecture
- [ ] No new lint warnings (`golangci-lint run`)
