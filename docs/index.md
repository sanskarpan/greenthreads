# greenthreads

**A fiber scheduler for Go with real-time browser observability.**

Four scheduling algorithms. Fiber-aware sync primitives. Deadlock detection.
Prometheus metrics. A live WebSocket control plane — all in a single binary.

[![CI](https://img.shields.io/github/actions/workflow/status/sanskarpan/greenthreads/ci.yml?branch=main&label=CI&style=flat-square)](https://github.com/sanskarpan/greenthreads/actions)
[![Go](https://img.shields.io/badge/go-%3E%3D1.21-00ADD8?style=flat-square&logo=go)](https://go.dev/dl/)
[![License](https://img.shields.io/github/license/sanskarpan/greenthreads?style=flat-square)](https://github.com/sanskarpan/greenthreads/blob/main/LICENSE)

---

## Quick start

**30 seconds to a running server:**

```bash
git clone https://github.com/sanskarpan/greenthreads
cd greenthreads
go run ./cmd/server
# open http://localhost:8080
```

**Use the scheduler in Go code:**

```go
import (
    "context"
    "fmt"

    "github.com/sanskarpan/greenthreads/internal/runtime"
    "github.com/sanskarpan/greenthreads/internal/scheduler"
)

func main() {
    rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
    if err := rt.Start(); err != nil {
        panic(err)
    }
    defer rt.Stop(context.Background())

    id, err := rt.Spawn(func() {
        fmt.Println("hello from fiber")
    }, "greeter")
    if err != nil {
        panic(err)
    }
    fmt.Printf("spawned fiber %d\n", id)
}
```

**Docker:**

```bash
docker build -t greenthreads:local .
docker run --rm -p 8080:8080 \
  -e GREENTHREADS_AUTH_TOKEN='replace-with-a-secret' \
  greenthreads:local
```

---

## Why greenthreads?

Go goroutines are excellent for concurrency, but they give you no control over
dispatch order, no structured fan-out with error collection, and no built-in
observability beyond `runtime/pprof`. greenthreads adds a thin scheduling layer
on top of real goroutines:

- Choose **FIFO, round-robin, priority, or work-stealing** dispatch.
- Use **`SpawnGroup`** for structured fan-out with unified error collection.
- Get a **Prometheus endpoint** and a **live WebSocket control plane** with
  real-time visibility into every fiber's lifecycle.

The entire stack ships as a single statically linked binary with one runtime
dependency (`gorilla/websocket`).

---

## Features

| Feature | Status |
|---|---|
| Four scheduler algorithms: FIFO, RoundRobin, Priority, WorkStealing | Stable |
| `Spawn` with name, priority, and timeout | Stable |
| `SpawnGroup` structured fan-out with error collection | Stable |
| `SpawnWithResult` — fiber returning a value via channel | Stable |
| Fiber-aware sync: Mutex, RWMutex, Channel, WaitGroup, Semaphore | Stable |
| Generic typed channel: `FiberChannelOf[T]` | Stable |
| Deadlock detector with configurable interval and timeout | Stable |
| Prometheus metrics: lifecycle counters, context switches, Go runtime stats | Stable |
| Browser WebSocket control plane: spawn, stop, reset, inspect | Stable |
| TLS support via cert/key flags or environment variables | Stable |
| Bearer token auth enforced on non-loopback binds | Stable |
| Non-root, statically linked Docker image | Stable |
| Race detector clean; fuzz seeds for all 9 fuzz functions | Stable |

---

## Scheduler comparison

| Scheduler | Order guarantee | Best for | Avg dispatch | Starvation risk |
|---|---|---|---|---|
| **FIFO** | Strict arrival order | Predictable pipelines, testing | ~200 ns/op | None |
| **RoundRobin** | Arrival order | Same as FIFO in this release | ~200 ns/op | None |
| **Priority** | Min-heap; anti-starvation aging after 100 pops | Mixed-criticality workloads | ~400 ns/op | Low |
| **WorkStealing** | Per-worker FIFO; idle workers steal half from busiest | CPU-bound fan-out | ~350 ns/op | Low |

See [Schedulers](schedulers.md) for full implementation details.

---

## Documentation

| Document | Contents |
|---|---|
| [Schedulers](schedulers.md) | Algorithm details, implementation notes, when to use each |
| [Sync Primitives](sync.md) | FiberMutex, FiberChannel, SpawnGroup, and more |
| [API Reference](api.md) | Full Runtime method signatures and return types |
| [Configuration](configuration.md) | Flags, environment variables, compile-time limits |
| [Observability](observability.md) | Prometheus metrics and WebSocket event types |
| [Deployment](deployment.md) | Docker, health checks, production hardening |
| [FAQ](faq.md) | Common questions and answers |
| [Comparison](comparison.md) | greenthreads vs goroutines, errgroup, conc |
| [Architecture](ARCHITECTURE.md) | Internal design for contributors |
| [Contributing](CONTRIBUTING.md) | Development workflow and required checks |
