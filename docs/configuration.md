# Configuration

All server flags have environment-variable equivalents. Environment variables
take precedence and are the preferred mechanism in containerised deployments —
they keep secrets out of process listings.

---

## Server flags and environment variables

| Flag | Env var | Type | Default | Description |
|---|---|---|---|---|
| `-listen` | `GREENTHREADS_LISTEN` | string | `127.0.0.1:8080` | Bind address. Non-loopback binds require an auth token. |
| `-port` | — | int | `8080` | Port used when `-listen` is omitted. Valid: 1–65535. |
| — | `GREENTHREADS_AUTH_TOKEN` | string | _(empty)_ | Bearer token for `/metrics` and `/ws` on non-loopback. Required for internet-facing operation. |
| `-tls-cert` | `GREENTHREADS_TLS_CERT` | string | _(empty)_ | TLS certificate PEM path. Both cert and key must be set to enable TLS. |
| `-tls-key` | `GREENTHREADS_TLS_KEY` | string | _(empty)_ | TLS private-key PEM path. |
| `-pprof-addr` | — | string | _(empty)_ | Optional pprof endpoint (e.g. `localhost:6060`). Always separate from the main listener. |
| — | `LOG_LEVEL` | string | `INFO` | Log verbosity: `DEBUG`, `INFO`, `WARN`, `ERROR`. |

---

## Runtime options (Go API)

These are passed to `NewRuntimeWithOptions` when embedding the scheduler in
your own application.

| Option | Type | Default | Description |
|---|---|---|---|
| `WithNumWorkers(n)` | int | `runtime.NumCPU()` | Worker goroutine count. Must exceed max simultaneously blocked fibers to prevent slot-exhaustion deadlock. |
| `WithMaxFibers(n)` | int | 10 000 | Hard cap on concurrent live fibers. `Spawn` returns `ErrMaxFibersReached` when reached. |
| `WithSchedulerType(t)` | SchedulerType | `TypeFIFO` | Scheduling algorithm. |
| `WithStackSize(bytes)` | int64 | 65 536 | Stack-size hint (advisory; the Go runtime manages actual stack growth). |
| `WithDetectorConfig(cfg)` | DetectorConfig | interval=1s, timeout=5s | Deadlock detector scan frequency and per-fiber wait threshold. |

---

## Compile-time limits

These constants are in `web/server.go` and can be overridden via
`NewServerWithConfig` when embedding the server.

| Constant | Default | Description |
|---|---|---|
| `maxClients` | 64 | Simultaneous WebSocket connections. Excess clients are rejected with HTTP 429. |
| `maxMessageSize` | 32 KiB | WebSocket frame payload limit. Oversized frames close the connection. |
| `messageRateLimit` | 30 / client / s | Clients exceeding this are disconnected. |

---

## Sizing `numWorkers`

The most common configuration mistake is setting `numWorkers` too low.

**Rule:** `numWorkers` must exceed the maximum number of fibers that can be
simultaneously blocked on sync primitives.

```
Suppose you have:
  - 4 workers
  - Fibers A, B, C, D all blocked on FiberMutex waiting for fiber E
  - Fiber E is still in the queue (not yet dispatched)

Result: deadlock — no slots available to run E.
Fix:    numWorkers = 5+ so E can run while A–D are parked.
```

**Practical guidance:**

- **Compute-only fibers (no blocking):** `numWorkers = runtime.NumCPU()`
- **IO-heavy or mutex-heavy:** `numWorkers = 2×–4× runtime.NumCPU()`
- **Unknown blocking depth:** start at `numWorkers = 32` and tune down with
  the Prometheus `greenthreads_fibers_running` gauge

---

## Example: local development

```bash
# Default: loopback only, no auth, no TLS. Open http://localhost:8080.
go run ./cmd/server
```

---

## Example: production server

```bash
GREENTHREADS_AUTH_TOKEN="$(openssl rand -hex 32)" \
GREENTHREADS_LISTEN="0.0.0.0:8443" \
GREENTHREADS_TLS_CERT="/etc/tls/cert.pem" \
GREENTHREADS_TLS_KEY="/etc/tls/key.pem" \
LOG_LEVEL=INFO \
  ./greenthreads-server
```

---

## Example: Docker with secrets

```bash
# Generate token once and store it.
TOKEN=$(openssl rand -hex 32)

docker run --rm \
  -p 8080:8080 \
  -e GREENTHREADS_LISTEN="0.0.0.0:8080" \
  -e GREENTHREADS_AUTH_TOKEN="$TOKEN" \
  ghcr.io/sanskarpan/greenthreads:latest
```

For Docker Swarm or Kubernetes, mount the token as a secret rather than
passing it via an environment variable visible in `docker inspect`:

```yaml
# docker-compose.yml
services:
  greenthreads:
    image: ghcr.io/sanskarpan/greenthreads:latest
    ports:
      - "8080:8080"
    environment:
      GREENTHREADS_LISTEN: "0.0.0.0:8080"
    secrets:
      - greenthreads_token
    # The server reads GREENTHREADS_AUTH_TOKEN; mount the secret at that path
    # and use an entrypoint wrapper to export it.

secrets:
  greenthreads_token:
    external: true
```

---

## Example: custom runtime options (Go embedding)

```go
rt := runtime.NewRuntimeWithOptions(
    runtime.WithSchedulerType(scheduler.TypePriority),
    runtime.WithNumWorkers(32),
    runtime.WithMaxFibers(100_000),
    runtime.WithDetectorConfig(runtime.DetectorConfig{
        Interval: 500 * time.Millisecond,
        Timeout:  3 * time.Second,
    }),
)
```

---

## Environment variable precedence

When both a flag and an environment variable are set, the **environment
variable wins**. This lets you set defaults in flags for local dev and
override them at deploy time without changing the binary.

```bash
# Flag sets port 8080; env var overrides to 9090.
GREENTHREADS_LISTEN="0.0.0.0:9090" ./greenthreads-server -listen 0.0.0.0:8080
# → binds to 0.0.0.0:9090
```

---

## Prometheus scrape configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: greenthreads
    scrape_interval: 15s
    static_configs:
      - targets: ['greenthreads:8080']
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/greenthreads-token
```

Store the auth token in a file rather than inline in the Prometheus config to
avoid it appearing in log output.
