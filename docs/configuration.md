# Configuration

All flags have environment-variable equivalents. Environment variables take
precedence for secret values like the auth token.

---

## Server flags and environment variables

| Flag / Env var | Type | Default | Description |
|---|---|---|---|
| `-listen` / `GREENTHREADS_LISTEN` | string | `127.0.0.1:8080` | Explicit bind address. Non-loopback binds require `GREENTHREADS_AUTH_TOKEN`. |
| `-port` | int | `8080` | Port used when `-listen` is omitted. Valid range: 1–65535. |
| `GREENTHREADS_AUTH_TOKEN` | string | _(empty)_ | Bearer token enforced on all non-loopback binds. Prefer `Authorization: Bearer` header over `?token=` query param. |
| `-tls-cert` / `GREENTHREADS_TLS_CERT` | string | _(empty)_ | Path to TLS certificate PEM. Both cert and key must be set to enable TLS. |
| `-tls-key` / `GREENTHREADS_TLS_KEY` | string | _(empty)_ | Path to TLS private key PEM. |
| `-pprof-addr` | string | _(empty)_ | Optional pprof endpoint address (e.g. `localhost:6060`). Never exposed on the main listener. |
| `LOG_LEVEL` | string | `INFO` | Log verbosity: `DEBUG`, `INFO`, `WARN`, `ERROR`. |

---

## Runtime options

| Option | Type | Default | Description |
|---|---|---|---|
| `WithNumWorkers(n)` | int | `runtime.NumCPU()` | Worker goroutine count. Set higher than max simultaneously blocked fibers to avoid deadlock. |
| `WithMaxFibers(n)` | int | 10 000 | Hard cap on concurrent live fibers. Spawn returns an error when the limit is reached. |
| `WithSchedulerType(t)` | SchedulerType | `TypeFIFO` | Scheduling algorithm. |
| `WithStackSize(bytes)` | int64 | 65 536 | Stack-size hint for goroutine allocation (advisory). |
| `WithDetectorConfig(cfg)` | DetectorConfig | interval=1s, timeout=5s | Deadlock detector scan frequency and per-fiber wait timeout. |

---

## Compile-time limits

These are constants in `internal/web/server.go`. Override via
`NewServerWithConfig` when embedding the server in another application.

| Constant | Default | Description |
|---|---|---|
| Max WebSocket clients | 64 | Simultaneous WebSocket connections. |
| Max message size | 32 KiB | WebSocket frame payload limit. |
| Message rate limit | 30 / client / s | Clients exceeding this are disconnected. |

---

## Example: production server invocation

```bash
GREENTHREADS_AUTH_TOKEN="$(openssl rand -hex 32)" \
GREENTHREADS_LISTEN="0.0.0.0:8443" \
GREENTHREADS_TLS_CERT="/etc/tls/cert.pem" \
GREENTHREADS_TLS_KEY="/etc/tls/key.pem" \
LOG_LEVEL=INFO \
  ./greenthreads-server
```
