# API Reference

The `runtime.Runtime` type is the primary entry point. All spawn methods are
safe to call concurrently.

---

## Construction

| Function | Returns | Description |
|---|---|---|
| `NewRuntime(typ, workers)` | `*Runtime` | Create a runtime with the given scheduler type and worker count. |
| `NewRuntimeWithOptions(opts...)` | `*Runtime` | Functional-options constructor for advanced configuration. |
| `WithNumWorkers(n)` | `Option` | Worker goroutine count. Default: `runtime.NumCPU()`. |
| `WithSchedulerType(t)` | `Option` | Algorithm: `TypeFIFO`, `TypeRoundRobin`, `TypePriority`, `TypeWorkStealing`. |
| `WithMaxFibers(n)` | `Option` | Hard cap on concurrently live fibers. Spawn returns an error when reached. |
| `WithStackSize(bytes)` | `Option` | Stack-size hint (advisory). |
| `WithDetectorConfig(cfg)` | `Option` | Configure deadlock detector interval and timeout. |

---

## Lifecycle

| Method | Returns | Description |
|---|---|---|
| `rt.Start()` | `error` | Start worker goroutines. Must be called before Spawn. |
| `rt.StartWithContext(ctx)` | `error` | Context-aware start; workers stop when ctx is cancelled. |
| `rt.Stop(ctx)` | `error` | Signal workers to drain then stop. Blocks until all fibers complete or ctx expires. |
| `rt.Reset()` | `error` | Drain all fibers and reset counters. Lifetime metrics survive Reset. |
| `rt.Wait()` | `error` | Block until all spawned fibers complete. Does not stop workers. |

---

## Spawn

| Method | Returns | Description |
|---|---|---|
| `rt.Spawn(fn, name)` | `FiberID, error` | Enqueue a fiber with a display name. Returns the fiber ID immediately. |
| `rt.SpawnWithResult(fn, name)` | `FiberID, <-chan any, error` | Spawn a fiber that sends its return value to the result channel on completion. |
| `rt.SpawnWithTimeout(ctx, fn, d)` | `FiberID, error` | Spawn with a context deadline. Caller returns after d; inner goroutine finishes naturally. |
| `rt.NewSpawnGroup()` | `*SpawnGroup` | Create a SpawnGroup for structured fan-out. Call `sg.Spawn()` then `sg.Wait()`. |

---

## Inspection

| Method | Returns | Description |
|---|---|---|
| `rt.GetFiber(id)` | `FiberHandle, bool` | Read-only view of a live fiber: state, name, priority, elapsed time. |
| `rt.GetAllFibers()` | `[]FiberHandle` | Snapshot of all currently registered fibers. |
| `rt.GetMetrics()` | `MetricsSnapshot` | Current counters: spawned, completed, panicked, running, queue depth. |
| `rt.GetLifetimeMetrics()` | `MetricsSnapshot` | Cumulative counters that survive Reset(). Monotonically increasing. |
| `rt.DeadlockDetector()` | `*DeadlockDetector` | Access the detector: SetEnabled, GetDeadlocks, ClearDeadlocks. Nil-safe. |

---

## Fiber states

```
Created → Runnable → Running → Done
                  ↘           ↗
                   Blocked ───
                  ↘
                   Panicked
```

| State | Description |
|---|---|
| `Created` | Fiber struct allocated, not yet queued. |
| `Runnable` | Queued in the scheduler, waiting for a worker slot. |
| `Running` | Executing in a worker goroutine. |
| `Blocked` | Parked on a sync primitive (Mutex, Channel, WaitGroup). Worker slot held. |
| `Panicked` | Fiber function panicked; panic captured in `PanicInfo`. Worker slot released. |
| `Done` | Fiber function returned normally. |

---

## Example: SpawnWithResult

```go
_, ch, err := rt.SpawnWithResult(func() interface{} {
    return computeAnswer()
}, "calculator")
if err != nil {
    panic(err)
}

result := <-ch
fmt.Printf("answer: %v\n", result)
```

## Example: SpawnGroup fan-out

```go
sg := rt.NewSpawnGroup()
for i := 0; i < 10; i++ {
    i := i
    sg.Spawn(func() {
        process(i)
    }, fmt.Sprintf("worker-%d", i))
}

errs := sg.Wait()
for _, err := range errs {
    if err != nil {
        fmt.Printf("worker error: %v\n", err)
    }
}
```
