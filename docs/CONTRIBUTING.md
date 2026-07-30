# Contributing to greenthreads

This document is the comprehensive contributor guide. The shorter `CONTRIBUTING.md` at the repository root is a quick reference; this file has the full details.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Running Tests Locally](#running-tests-locally)
- [Code Style](#code-style)
- [Testing Requirements](#testing-requirements)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [What Makes a Good PR](#what-makes-a-good-pr)
- [Architecture Notes](#architecture-notes)
- [Debugging Tips](#debugging-tips)

---

## Getting Started

1. Fork the repository on GitHub.
2. Clone your fork:

   ```bash
   git clone https://github.com/<your-username>/greenthreads.git
   cd greenthreads
   ```

3. Download module dependencies:

   ```bash
   go mod download
   ```

4. Verify your setup passes the full test suite:

   ```bash
   make check
   ```

If `make check` passes, your environment is ready.

---

## Development Environment

### Required

| Tool         | Version   | Purpose                              |
| ------------ | --------- | ------------------------------------ |
| Go           | 1.21+     | Build and test                       |
| golangci-lint | latest   | Lint enforcement                     |
| govulncheck  | latest    | Dependency vulnerability scanning    |

Install the linter and vuln scanner:

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install golang.org/x/vuln/cmd/govulncheck@latest
```

### Optional but Recommended

- **pprof**: bundled with Go; accessed at `http://localhost:6060/debug/pprof/` when `cmd/server` runs with `GREENTHREADS_PPROF_ADDR=localhost:6060`
- **delve**: `go install github.com/go-delve/delve/cmd/dlv@latest`

---

## Running Tests Locally

Run the full check (vet, lint, race tests, coverage):

```bash
make check
```

Individual commands:

```bash
# Unit tests with race detector
go test -race ./...

# Unit tests with coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run fuzz targets for a short duration (CI runs 10s each)
go test -fuzz=FuzzNewFiber -fuzztime=30s ./internal/fiber/
go test -fuzz=FuzzFiberChannelSendReceive -fuzztime=30s ./internal/sync/

# Benchmarks
go test -bench=. -benchmem ./...

# Vulnerability scan
govulncheck ./...
```

Stress tests are tagged `//go:build stress` and run only when explicitly requested. They simulate thousands of fibers and may take several minutes:

```bash
go test -tags stress -race -timeout 10m ./...
```

---

## Code Style

- All Go code must be formatted with `gofmt` (enforced by CI).
- Run the linter before pushing: `golangci-lint run ./...`
- Prefer the simplest correct solution. Do not add abstractions speculatively.
- Error messages are lowercase, no trailing period, and include enough context to identify the caller (e.g., `"schedule fiber: scheduler stopped"`).
- Public API surface is intentionally minimal. New exported symbols require a clear use case that cannot be served by existing symbols.
- Comment every exported symbol. Comments on unexported symbols are encouraged for anything non-obvious.

---

## Testing Requirements

Every pull request that changes behavior must include a regression test that fails on the old code and passes on the new code. This is not optional.

Specific requirements:

- **Race detector clean**: all tests must pass under `go test -race ./...`. A PR that introduces a data race will not be merged.
- **Coverage maintained**: the project targets 85% statement coverage. PRs that drop coverage more than 1-2 percentage points need justification.
- **Fuzz-corpus entries**: if you fix a bug that was found by a fuzzer or could be encoded as a fuzz input, add a seed corpus entry under `testdata/fuzz/<FuzzFunctionName>/`.
- **Benchmark regressions**: if your change affects a hot path (scheduler dispatch, fiber completion), include before/after benchmark output in the PR description.

---

## Submitting a Pull Request

### Branch naming

```
feature/<short-description>
fix/<short-description>
chore/<short-description>
```

### Commit message convention

greenthreads uses [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <short imperative summary>

<optional body>

<optional footer>
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`

Examples:

```
fix(scheduler): prevent double MarkCompleted from inflating TotalCompleted
feat(runtime): add StartWithContext for context-bounded lifecycle
chore: update golangci-lint to v1.60
```

### PR description

Include:

1. What invariant or behavior the PR protects or adds.
2. The security or operational impact (even if none — state "no security impact").
3. The commands you ran to verify it.

### Before opening the PR

```bash
make check          # full lint + race test + coverage
govulncheck ./...   # vuln scan
```

---

## What Makes a Good PR

**Focused**: one logical change per PR. A PR that fixes a scheduler bug and also refactors the metrics package is two PRs.

**Tested**: the PR body shows `go test -race ./...` output. CI output alone is not sufficient — paste the relevant lines so reviewers can scan them without clicking through.

**Described**: the PR title is a complete sentence in imperative mood. The body explains the *why*, not just the *what*.

**Architecture-aware**: if the change modifies a contract between packages (e.g., Scheduler interface, fiber state machine transitions, deadlock detector scan algorithm), update the relevant ADR under `docs/adr/` or open a new one.

**Runbook-aware**: if the change affects how the server is operated, deployed, or observed, update `RUNBOOK.md`.

---

## Architecture Notes

The codebase is organized around a strict internal/external boundary:

- `internal/fiber` — Fiber type, state machine, bounded simulated stack
- `internal/scheduler` — Scheduler interface and four implementations (FIFO, RoundRobin, Priority, WorkStealing)
- `internal/runtime` — Admission loop, lifecycle (Start/Stop/Reset), deadlock detector, metrics injection
- `internal/sync` — FiberChannel, FiberMutex, FiberRWMutex, FiberWaitGroup
- `internal/metrics` — Per-run and lifetime counters, Prometheus-compatible histogram, event tracker
- `web` — WebSocket control plane, HTTP metrics endpoint, static assets
- `cmd/server` — Main entry point; wires runtime to web server

All packages under `internal/` are not part of the public API. Nothing under `internal/` imports from `web/` or `cmd/` — the dependency graph is strictly layered.

There is no `pkg/` directory yet. A stable public API will be carved out in a future milestone. Until then, users embed the server binary or fork.

Architectural decisions are recorded in `docs/adr/`. Read them before changing anything that touches the Scheduler interface, fiber lifecycle, or auth model.

---

## Debugging Tips

### Deadlock detector output

When the deadlock detector fires, it prints a snapshot to the event tracker accessible via `/metrics` or `Runtime.GetEvents`. To read it from the CLI:

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/metrics
```

Look for `greenthreads_deadlock_active 1` and `greenthreads_blocked_fibers`.

### pprof

Run the server with the pprof listener enabled:

```bash
GREENTHREADS_PPROF_ADDR=localhost:6060 go run ./cmd/server
```

Then in another terminal:

```bash
# CPU profile for 30 seconds
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Goroutine dump (useful for diagnosing blocked fibers)
curl http://localhost:6060/debug/pprof/goroutine?debug=2
```

### Race detector false positives

There are no known false positives. If the race detector fires, treat it as a real bug. Check whether the access is on a `Fiber` field that is read outside the fiber's `mu` lock.

### Reading scheduler queue state

```go
rt.GetScheduler().GetRunQueue()    // fibers waiting for dispatch
rt.GetScheduler().GetBlockedQueue() // fibers parked on sync primitives
rt.GetAllFibers()                  // full fiber map snapshot
```

All three return clones, so they are safe to inspect from any goroutine.
