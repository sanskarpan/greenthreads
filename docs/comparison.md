# Comparison

How greenthreads compares to other approaches for managing concurrent work in Go.

---

## Feature matrix

| Feature | greenthreads | goroutines + WaitGroup | errgroup | conc |
|---|---|---|---|---|
| Pluggable scheduler | Yes — 4 algorithms | No | No | No |
| Priority scheduling | Yes — min-heap | No | No | No |
| Anti-starvation aging | Yes | No | No | No |
| Work stealing | Yes — per-worker deque | Runtime-internal | No | No |
| Deadlock detection | Yes — cycle scan | No | No | No |
| Live browser visualization | Yes — WebSocket | No | No | No |
| Prometheus metrics | Yes — `/metrics` | No | No | No |
| Fiber-blocking sync primitives | Yes — 5 types | OS-level only | No | No |
| Structured fan-out | Yes — SpawnGroup | Manual | Yes | Yes |
| Error collection | Yes | Manual | Yes | Yes |
| Per-fiber result channels | Yes — SpawnWithResult | Manual | No | No |
| Panic recovery | Yes — automatic | Manual | Yes | Yes |
| Zero core dependencies | Yes | Yes | Yes | No |

---

## When to use each

### Plain goroutines + `sync.WaitGroup`

Best when: you need maximum simplicity and the Go scheduler's default behaviour
is acceptable. No extra dependency, no learning curve.

Limitations: no structured error collection, no per-goroutine observability,
no dispatch control.

### `golang.org/x/sync/errgroup`

Best when: you need structured fan-out with first-error cancellation via
`context`. The idiomatic Go choice for most concurrent tasks.

Limitations: no scheduler control, no per-fiber visibility, no Prometheus
metrics.

### `sourcegraph/conc`

Best when: you want a richer API around goroutine pools and panic recovery,
with a fluent builder style.

Limitations: third-party dependencies, no scheduler pluggability, no
observability.

### greenthreads

Best when:

- You need **predictable dispatch order** (FIFO, priority, work-stealing).
- You're building a **mixed-criticality system** where some tasks must preempt
  others in the queue.
- You want **live visibility** into concurrency behaviour during development or
  production debugging.
- You want **Prometheus integration** without adding a separate metrics library.
- You need a **deadlock detector** that surfaces cycle conditions automatically.
