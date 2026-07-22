# Green Threads

Green Threads is a bounded Go scheduler and synchronization experiment with a browser visualization control plane.

The runtime admits each fiber function exactly once to an owned goroutine. It provides scheduler selection, lifecycle state, metrics, and fiber-aware wait queues; it does not provide stackful context switching or preemptive interruption of arbitrary Go code.

## Architecture

```text
Browser
  | same-origin WebSocket / health / metrics
web.Server (private mux, limits, auth/origin checks)
  | typed control messages
runtime.Runtime (lifecycle, admission, snapshots, metrics)
  +-- scheduler: FIFO | RoundRobin | Priority | WorkStealing
  +-- fiber: bounded simulated stack and synchronized state
  +-- sync: channel, mutex, RWMutex, wait group, semaphore
  +-- deadlock detector and event history
```

## Prerequisites

- Go `1.26.5` (the version in `go.mod`)
- Docker 24+ for container builds
- `golangci-lint` for local linting
- `govulncheck` for local dependency scanning

## Local Setup

```bash
git clone <repository-url>
cd User-Level-Threading-Library
go mod download
go test ./...
go run ./cmd/server
```

The server listens on `127.0.0.1:8080` by default. Open `http://127.0.0.1:8080`. The web UI initializes a runtime before spawning fibers.

## Configuration

| Setting | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `-port` | TCP port string | `8080` | No | Used when `-listen` is omitted; valid range is 1-65535. |
| `-listen` | host:port string | `127.0.0.1:8080` | No | Explicit bind address. Non-loopback binds require the auth token. |
| `GREENTHREADS_AUTH_TOKEN` | secret string | empty | For non-loopback | Bearer token or `?token=` accepted by the WebSocket handshake. Use a secret manager in production. |

WebSocket limits are bounded: 64 clients, 32 KiB messages, 30 messages per client per second, and up to 10,000 *concurrently active* fibers per runtime (the cap is on active fibers, not lifetime creations, so a long-lived runtime can keep spawning as old fibers finish). The server accepts same-origin requests by default. The auth token is preferred via the `Authorization: Bearer <token>` header; it is also accepted via `?token=` for browser WebSocket clients that cannot set headers, but that leaks the token into access logs and browser history, so `AllowTokenInQuery` can be disabled in `ServerConfig` for non-browser deployments. Use `NewServerWithConfig` to set an explicit origin allowlist and limits in an embedding application.

## Test and Verification Commands

```bash
go test ./...                         # unit and HTTP/WebSocket integration tests
go test -race ./...                   # concurrent code gate
go vet ./...
golangci-lint run ./...
govulncheck ./...
make coverage COVERAGE_MIN=45
make bench
make fuzz
```

The CI coverage baseline is 45% aggregate. `scripts/check_benchmarks.sh` fails when the FIFO scheduler exceeds 500,000 ns/op by default; set `FIFO_MAX_NS_PER_OP` only when a reviewed platform baseline changes.

## Docker

```bash
docker build --tag greenthreads:local .
docker run --rm -p 8080:8080 \
  -e GREENTHREADS_AUTH_TOKEN='replace-with-a-secret' \
  greenthreads:local
```

The image binds to `0.0.0.0:8080` and refuses to start without `GREENTHREADS_AUTH_TOKEN`. The image is non-root and contains only the statically linked server binary.

## HTTP and WebSocket API

- `GET /healthz`: process liveness; returns 200 while the HTTP process is serving.
- `GET /readyz`: returns 200 only when a runtime exists and is running, otherwise 503.
- `GET /metrics`: Prometheus text exposition for runtime counters.
- `GET /`: embedded visualization assets.
- `GET /ws`: authenticated, same-origin WebSocket control channel.

Message types are `init`, `spawn`, `stop`, `reset`, and `getState`. `init` accepts `schedulerType` (`fifo`, `roundrobin`, `priority`, or `workstealing`) and `numWorkers` (1-64). `spawn` accepts a name up to 128 characters, priority -100..100, and duration 1..60000 ms. Invalid messages return a generic error response and do not expose internal details.

The Go packages are under `internal/`, so this repository is currently intended to be built as a single module or forked into an application. Moving a stable public API to `pkg/` is tracked in `ISSUES.md`.

## Operations

See [RUNBOOK.md](RUNBOOK.md) for deployment, rollback, health checks, and failure diagnosis. Architectural decisions are recorded in [docs/adr](docs/adr).

## Contributing

- Branches: `feature/<short-name>`, `fix/<short-name>`, or `chore/<short-name>`.
- Commits: Conventional Commits, for example `fix: preserve unbuffered channel values`.
- Pull requests must include a regression test, risk summary, and verification output.
- Required checks are build, unit tests, race detector, vet, lint, coverage, benchmark, vulnerability scan, and container scan.
- Keep public contracts and operational limits documented when behavior changes.

## License

MIT. See [LICENSE](LICENSE).
