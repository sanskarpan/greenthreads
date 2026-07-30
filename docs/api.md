# API Reference

The `runtime.Runtime` type is the primary entry point. All spawn methods are
safe to call concurrently from multiple goroutines.

---

## Construction

| Function | Returns | Description |
|---|---|---|
| `NewRuntime(typ, workers)` | `*Runtime` | Create a runtime with the given scheduler type and worker count. |
| `NewRuntimeWithOptions(opts...)` | `*Runtime` | Functional-options constructor. |
| `WithNumWorkers(n)` | `Option` | Worker goroutine count. Default: `runtime.NumCPU()`. |
| `WithSchedulerType(t)` | `Option` | `TypeFIFO`, `TypeRoundRobin`, `TypePriority`, `TypeWorkStealing`. |
| `WithMaxFibers(n)` | `Option` | Hard cap on concurrently live fibers (default 10 000). |
| `WithStackSize(bytes)` | `Option` | Stack-size hint for goroutine allocation (advisory). |
| `WithDetectorConfig(cfg)` | `Option` | Deadlock detector interval and per-fiber wait timeout. |

### Example: advanced construction

```go
rt := runtime.NewRuntimeWithOptions(
    runtime.WithSchedulerType(scheduler.TypePriority),
    runtime.WithNumWorkers(16),
    runtime.WithMaxFibers(50_000),
    runtime.WithDetectorConfig(runtime.DetectorConfig{
        Interval: 500 * time.Millisecond,
        Timeout:  10 * time.Second,
    }),
)
```

---

## Lifecycle

| Method | Returns | Description |
|---|---|---|
| `rt.Start()` | `error` | Start worker goroutines. Must be called before Spawn. |
| `rt.StartWithContext(ctx)` | `error` | Workers stop when `ctx` is cancelled. |
| `rt.Stop(ctx)` | `error` | Drain then stop. Blocks until all fibers complete or `ctx` expires. |
| `rt.Reset()` | `error` | Drain all fibers and reset per-session counters. Lifetime metrics survive. |
| `rt.Wait()` | `error` | Block until all spawned fibers complete. Does not stop workers. |

### Typical lifecycle

```go
rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)

// 1. Start workers.
if err := rt.Start(); err != nil {
    log.Fatal(err)
}

// 2. Spawn work.
for i := 0; i < 100; i++ {
    i := i
    rt.Spawn(func() { process(i) }, fmt.Sprintf("task-%d", i))
}

// 3. Wait for all fibers to finish (optional — Stop also drains).
rt.Wait()

// 4. Graceful shutdown with a 30-second deadline.
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := rt.Stop(ctx); err != nil {
    log.Printf("stop timed out: %v", err)
}
```

### Reset pattern

`Reset` is useful in test suites or long-running servers that re-use the same
runtime across multiple work batches:

```go
// First batch.
for _, job := range batch1 {
    rt.Spawn(job.fn, job.name)
}
rt.Wait()
m1 := rt.GetMetrics()
fmt.Printf("batch1: %d spawned\n", m1.TotalSpawned)

// Reset — per-session counters go to zero, workers keep running.
rt.Reset()

// Second batch.
for _, job := range batch2 {
    rt.Spawn(job.fn, job.name)
}
rt.Wait()
m2 := rt.GetMetrics()
fmt.Printf("batch2: %d spawned\n", m2.TotalSpawned) // starts from 0 again

// Lifetime counters accumulate across resets.
lt := rt.GetLifetimeMetrics()
fmt.Printf("all-time: %d spawned\n", lt.TotalSpawned)
```

### Context-aware startup

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

if err := rt.StartWithContext(ctx); err != nil {
    log.Fatal(err)
}

// Workers stop when cancel() is called — no explicit rt.Stop() needed.
```

---

## Spawn

| Method | Returns | Description |
|---|---|---|
| `rt.Spawn(fn, name)` | `FiberID, error` | Enqueue a fiber. Returns immediately. |
| `rt.SpawnWithResult(fn, name)` | `FiberID, <-chan any, error` | Fiber's return value is sent to the channel on completion. |
| `rt.SpawnWithTimeout(ctx, fn, d)` | `FiberID, error` | Spawn with a deadline. Caller unblocks after `d`; fiber finishes naturally. |
| `rt.NewSpawnGroup()` | `*SpawnGroup` | Structured fan-out; call `sg.Spawn()` then `sg.Wait()`. |

### Spawn errors

`Spawn` returns a non-nil error when:
- The runtime has not been started (`rt.Start()` not called).
- The runtime is stopped or in the process of stopping.
- The live fiber count has reached `WithMaxFibers`.

```go
id, err := rt.Spawn(func() { doWork() }, "worker")
if err != nil {
    switch {
    case errors.Is(err, runtime.ErrRuntimeStopped):
        // runtime is shutting down
    case errors.Is(err, runtime.ErrMaxFibersReached):
        // backpressure — retry or drop
    default:
        log.Printf("spawn error: %v", err)
    }
}
```

### SpawnWithResult

```go
id, ch, err := rt.SpawnWithResult(func() interface{} {
    return heavyCompute()
}, "compute")
if err != nil {
    panic(err)
}
_ = id

// Block until the fiber completes and returns its value.
result := <-ch
fmt.Printf("result: %v\n", result)
```

The channel is buffered (capacity 1) so the fiber never blocks on send even if
the caller is slow to receive.

### SpawnWithTimeout

```go
ctx := context.Background()
id, err := rt.SpawnWithTimeout(ctx, func() {
    // This runs to completion regardless of the timeout below.
    doSlowWork()
}, 100*time.Millisecond)
if err != nil {
    panic(err)
}

// The caller returns after 100ms. The fiber may still be running.
fmt.Printf("spawned fiber %d, timeout already elapsed\n", id)
```

`SpawnWithTimeout` is intended for **fire-and-forget** work where you want to
limit how long the caller waits, not how long the fiber runs. To cooperatively
cancel a long-running fiber, pass a `context.Context` into the fiber function
and check `ctx.Done()`.

### SpawnGroup fan-out

```go
sg := rt.NewSpawnGroup()

results := make([]int, 10)
var mu sync.Mutex

for i := 0; i < 10; i++ {
    i := i
    sg.Spawn(func() {
        v := expensiveCompute(i)
        mu.Lock()
        results[i] = v
        mu.Unlock()
    }, fmt.Sprintf("worker-%d", i))
}

// Wait blocks until every fiber in the group finishes.
errs := sg.Wait()
for _, err := range errs {
    if err != nil {
        log.Printf("worker error: %v", err)
    }
}
fmt.Println(results)
```

---

## Inspection

| Method | Returns | Description |
|---|---|---|
| `rt.GetFiber(id)` | `FiberHandle, bool` | Read-only snapshot of one fiber. |
| `rt.GetAllFibers()` | `[]FiberHandle` | Snapshot of all registered fibers. |
| `rt.GetMetrics()` | `MetricsSnapshot` | Per-session counters (reset on `Reset()`). |
| `rt.GetLifetimeMetrics()` | `MetricsSnapshot` | Cumulative counters. Always increasing. |
| `rt.DeadlockDetector()` | `*DeadlockDetector` | Access the detector. Nil-safe. |

### FiberHandle fields

`FiberHandle` is a read-only value type returned by `GetFiber` and `GetAllFibers`:

```go
handle, ok := rt.GetFiber(fiberID)
if !ok {
    // Fiber has finished and been collected.
    return
}

fmt.Printf("id=%d name=%q state=%s priority=%d elapsed=%s\n",
    handle.ID, handle.Name, handle.State, handle.Priority, handle.Elapsed())
```

### Polling metrics

```go
ticker := time.NewTicker(5 * time.Second)
defer ticker.Stop()

for range ticker.C {
    m := rt.GetMetrics()
    fmt.Printf("running=%d queued=%d completed=%d panicked=%d\n",
        m.Running, m.QueueDepth, m.Completed, m.Panicked)
}
```

### Deadlock detector

```go
// Enable at startup.
rt.DeadlockDetector().SetEnabled(true)

// Later, inspect for detected deadlocks.
deadlocks := rt.DeadlockDetector().GetDeadlocks()
for _, d := range deadlocks {
    log.Printf("deadlock: fiber %d waiting on fiber %d", d.WaiterID, d.HolderID)
}

// Clear after logging so the list doesn't grow unboundedly.
rt.DeadlockDetector().ClearDeadlocks()
```

The detector runs as a background goroutine that wakes on its configured
`Interval`. It performs a wait-graph cycle scan: if fiber A holds a lock that
B is waiting for, and B holds a lock that A is waiting for, both are flagged.

---

## Fiber states

```
Created → Runnable → Running → Done
                  ↘     ↑
                  Blocked ┘
                  ↘
                  Panicked
```

| State | Description |
|---|---|
| `Created` | Fiber struct allocated, not yet queued. |
| `Runnable` | Queued in the scheduler, waiting for a worker slot. |
| `Running` | Executing in a worker goroutine. Holds one worker slot. |
| `Blocked` | Parked on a sync primitive. Still holds its worker slot. |
| `Panicked` | Fiber function panicked; panic value and stack captured in `PanicInfo`. Slot released. |
| `Done` | Fiber function returned normally. Slot released. |

!!! warning "Blocked fibers hold worker slots"
    A fiber in the `Blocked` state is parked but still occupies one worker
    slot. If all slots are blocked, no new fibers can run — leading to slot
    exhaustion deadlock. Set `numWorkers > max simultaneously blocked fibers`.

---

## Complete working example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "sync"
    "time"

    "github.com/sanskarpan/greenthreads/internal/runtime"
    "github.com/sanskarpan/greenthreads/internal/scheduler"
)

func main() {
    rt := runtime.NewRuntimeWithOptions(
        runtime.WithSchedulerType(scheduler.TypeWorkStealing),
        runtime.WithNumWorkers(8),
        runtime.WithMaxFibers(1000),
    )

    if err := rt.Start(); err != nil {
        log.Fatal(err)
    }

    rt.DeadlockDetector().SetEnabled(true)

    var mu sync.Mutex
    counts := make(map[string]int)

    sg := rt.NewSpawnGroup()
    for i := 0; i < 50; i++ {
        i := i
        category := []string{"alpha", "beta", "gamma"}[i%3]
        sg.Spawn(func() {
            time.Sleep(time.Duration(i%5) * time.Millisecond)
            mu.Lock()
            counts[category]++
            mu.Unlock()
        }, fmt.Sprintf("%s-%d", category, i))
    }
    sg.Wait()

    m := rt.GetMetrics()
    fmt.Printf("spawned=%d completed=%d panicked=%d\n",
        m.TotalSpawned, m.Completed, m.Panicked)
    fmt.Println(counts)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := rt.Stop(ctx); err != nil {
        log.Printf("stop: %v", err)
    }
}
```
