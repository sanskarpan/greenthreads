# Observability

All metrics are exposed on `GET /metrics` in Prometheus text format. Bearer-
token auth is enforced on non-loopback binds. WebSocket events stream to
connected browser clients in real time.

---

## Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `greenthreads_fibers_spawned_total` | Counter | Total fibers spawned since last `Reset()`. |
| `greenthreads_fibers_completed_total` | Counter | Fibers that returned normally. |
| `greenthreads_fibers_panicked_total` | Counter | Fibers whose function panicked (recovered by runtime). |
| `greenthreads_fibers_running` | Gauge | Currently executing fibers (holds a worker slot). |
| `greenthreads_scheduler_queue_depth` | Gauge | Fibers waiting in the scheduler queue (Runnable state). |
| `greenthreads_fiber_duration_seconds` | Histogram | End-to-end fiber execution time from Spawn to Done. |
| `greenthreads_context_switches_total` | Counter | Scheduler dispatches (Next() calls that returned a fiber). |
| `greenthreads_fiber_panics_total` | Counter | Fibers whose function panicked and was recovered. |
| `greenthreads_deadlocks_total` | Counter | Deadlock episodes flagged by the detector (rising edge). |
| `greenthreads_spawn_errors_total` | Counter | Spawn attempts that returned an error. |
| `greenthreads_broadcast_messages_dropped_total` | Counter | WebSocket broadcast messages dropped due to a full client buffer. |

The `/metrics` endpoint also exposes standard Go runtime metrics via
`prometheus/client_golang`: goroutine count, heap allocation, GC pause, and
more.

### Scrape config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: greenthreads
    static_configs:
      - targets: ['localhost:8080']
    authorization:
      type: Bearer
      credentials: <your-auth-token>
```

---

## Distributed tracing (OpenTelemetry)

Tracing is **opt-in**. It activates only when an OTLP endpoint is configured;
otherwise a no-op tracer is installed and there is zero overhead and no network
activity.

Enable it with the standard OpenTelemetry environment variables:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector:4318" \
OTEL_SERVICE_NAME="greenthreads-server" \
  ./greenthreads-server
```

What is instrumented:

- **HTTP server spans** — one span per request to `/metrics`, `/ws`, `/healthz`,
  `/readyz`, and the UI, via `otelhttp`. Incoming W3C `traceparent` headers are
  honored so spans join an existing distributed trace.
- **Control-plane command spans** — one span per WebSocket command
  (`ws.init`, `ws.spawn`, `ws.stop`, `ws.reset`, `ws.getState`), attributed with
  the command name and whether the client is read-only.

The exporter uses OTLP over HTTP; point `OTEL_EXPORTER_OTLP_ENDPOINT` at any
OTLP-compatible collector (OpenTelemetry Collector, Jaeger, Tempo, etc.).

---

## HTTP endpoints

| Endpoint | Method | Auth required | Description |
|---|---|---|---|
| `/healthz` | GET | No | Process liveness. Returns 200 while the server is running. |
| `/readyz` | GET | No | Runtime readiness. Returns 200 when a runtime is active, 503 otherwise. |
| `/metrics` | GET | On non-loopback | Prometheus text exposition. |
| `/ws` | GET | On non-loopback | WebSocket upgrade to the control plane. |
| `/` | GET | No | Embedded browser visualization UI. |

---

## WebSocket event types

The WebSocket control plane at `/ws` accepts commands from the browser and
streams fiber state changes back.

### Client → server

| Event type | Payload | Description |
|---|---|---|
| `init` | `{schedulerType, numWorkers}` | Create a runtime with the given scheduler and worker count. |
| `spawn` | `{name, priority, durationMs}` | Spawn a fiber with a simulated duration. |
| `stop` | _(empty)_ | Stop the current runtime gracefully. |
| `reset` | _(empty)_ | Reset runtime state and clear all fibers. |
| `getState` | _(empty)_ | Request a full fiber-state snapshot. |

### Server → client

| Event type | Payload | Description |
|---|---|---|
| `state_update` | `{id, from, to, timestamp}` | Streamed fiber state change. |
| `metrics_update` | `{spawned, completed, panicked, running, queueDepth}` | Periodic metrics push. |

### Example messages

```json
{"type": "init", "schedulerType": "priority", "numWorkers": 8}

{"type": "spawn", "name": "worker-1", "priority": 10, "durationMs": 500}

{"type": "stop"}

{"type": "getState"}
```

Invalid messages return a generic error response and do not expose internal
details.
