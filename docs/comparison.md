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

## Side-by-side: fan-out 10 tasks and collect results

All four approaches spawn 10 concurrent workers that each return an integer.

### Plain goroutines + `sync.WaitGroup`

```go
var wg sync.WaitGroup
results := make([]int, 10)

for i := 0; i < 10; i++ {
    i := i
    wg.Add(1)
    go func() {
        defer wg.Done()
        results[i] = compute(i) // ❌ no error path without extra channel
    }()
}
wg.Wait()
```

**Verdict:** Minimal and fast, but you wire up error handling yourself. Any
panic in a worker brings down the whole process unless you add `recover()` to
every goroutine.

---

### `golang.org/x/sync/errgroup`

```go
g, ctx := errgroup.WithContext(context.Background())
results := make([]int, 10)

for i := 0; i < 10; i++ {
    i := i
    g.Go(func() error {
        if ctx.Err() != nil {
            return ctx.Err()
        }
        results[i] = compute(i)
        return nil
    })
}

if err := g.Wait(); err != nil {
    log.Fatal(err) // first error wins; others are cancelled via ctx
}
```

**Verdict:** The idiomatic Go choice for structured fan-out with error
propagation. First error cancels the context; all workers should check it.
No scheduler control, no per-goroutine observability.

---

### `sourcegraph/conc`

```go
p := pool.NewWithResults[int]()

for i := 0; i < 10; i++ {
    i := i
    p.Go(func() int {
        return compute(i)
    })
}

results := p.Wait()
```

**Verdict:** Fluent builder API, clean result collection, automatic panic
recovery. Third-party dependency; no scheduler pluggability or metrics.

---

### greenthreads

```go
rt := runtime.NewRuntime(scheduler.TypeWorkStealing, runtime.NumCPU())
if err := rt.Start(); err != nil {
    panic(err)
}
defer rt.Stop(context.Background())

var mu sync.Mutex
results := make([]int, 10)

sg := rt.NewSpawnGroup()
for i := 0; i < 10; i++ {
    i := i
    sg.Spawn(func() {
        v := compute(i)
        mu.Lock()
        results[i] = v
        mu.Unlock()
    }, fmt.Sprintf("worker-%d", i))
}
sg.Wait()

m := rt.GetMetrics()
fmt.Printf("completed=%d panicked=%d queue_depth=%d\n",
    m.Completed, m.Panicked, m.QueueDepth)
```

**Verdict:** More setup than the others, but you get: scheduler control,
automatic panic capture (panicked fibers do not terminate the runtime), live
Prometheus metrics, and the option to watch execution in a browser in real
time. Worth the overhead when observability or dispatch order matters.

---

## Side-by-side: priority fan-out

When some tasks are more urgent than others, the alternatives diverge sharply.

### Plain goroutines — no priority

```go
// No mechanism to ensure urgentTask() runs before batchTask().
go urgentTask()
for i := 0; i < 100; i++ {
    go batchTask(i)
}
```

### greenthreads — explicit priority

```go
rt := runtime.NewRuntimeWithOptions(
    runtime.WithSchedulerType(scheduler.TypePriority),
    runtime.WithNumWorkers(8),
)
rt.Start()
defer rt.Stop(context.Background())

// Spawn 100 low-priority batch fibers first.
for i := 0; i < 100; i++ {
    i := i
    rt.Spawn(batchTask(i), fmt.Sprintf("batch-%d", i))
    // default priority = 10
}

// Urgent control signal — dispatched before any batch fiber when a slot frees.
rt.Spawn(urgentTask, "control") // priority = 1 (lower number = higher priority)
```

The urgent fiber runs next regardless of how many batch fibers are already
queued. No equivalent exists in the standard library or errgroup.

---

## When to use each

### Plain goroutines + `sync.WaitGroup`

Best when: maximum simplicity, no extra dependency, Go scheduler default
behaviour is fine.

Limitations: manual error/panic handling, no dispatch control, no per-goroutine
visibility.

### `golang.org/x/sync/errgroup`

Best when: structured fan-out with first-error cancellation via `context`. The
idiomatic Go choice for the vast majority of concurrent tasks.

Limitations: no scheduler control, no per-fiber visibility, no Prometheus
integration.

### `sourcegraph/conc`

Best when: you want a richer API around goroutine pools and panic recovery,
with a fluent builder style.

Limitations: third-party dependency tree, no scheduler pluggability, no
observability.

### greenthreads

Best when:

- You need **predictable dispatch order** (FIFO) or **priority** (mixed
  criticality).
- You want a **live browser view** of your concurrency graph during
  development or production debugging.
- You want **Prometheus metrics** without adding a separate metrics library.
- You need **deadlock detection** that surfaces cycle conditions
  automatically.
- You're building a **scheduler-aware system** where work-stealing across CPU
  cores matters.

### When NOT to use greenthreads

- Simple one-off parallel tasks with no observability requirement — errgroup
  is simpler and standard.
- Extremely latency-sensitive hot paths where even ~200 ns scheduler overhead
  is significant — plain goroutines win.
- Environments where you cannot tolerate any external dependency — goroutines +
  `sync.WaitGroup` have zero deps.
