# FAQ

---

## What is the difference between a fiber and a Go goroutine?

Each fiber *is* a real Go goroutine — greenthreads does not implement its own
stack-switching or assembly-level context switching. The difference is in
**dispatch control**: the runtime decides which fiber runs next according to
the configured scheduling policy (FIFO, Priority, etc.), rather than letting
the Go scheduler decide freely.

This gives you predictable dispatch order, structured fan-out, per-fiber
observability, and deadlock detection — none of which are available with plain
goroutines.

---

## How many workers should I configure?

The rule: `numWorkers > max simultaneously blocked fibers`.

Each blocked fiber holds its worker slot. If all slots are held by fibers
waiting on work that cannot be dispatched (because all slots are full), the
runtime deadlocks.

- **Compute-bound, no blocking:** `runtime.NumCPU()` (the default).
- **IO-heavy or mutex-heavy:** 2×–4× CPU count.
- **Unknown:** start at 32 and tune down by watching `greenthreads_fibers_running`.

The deadlock detector surfaces slot-exhaustion deadlocks automatically when enabled.

---

## Does Priority scheduling guarantee a high-priority fiber always runs first?

Yes, *among fibers waiting in the queue*. A fiber that is already running is
not preempted — greenthreads uses cooperative (non-preemptive) scheduling.

A high-priority fiber spawned while all workers are busy is the first to run
when a slot becomes free, but it cannot forcibly interrupt a running fiber.

For workloads where preemptive priority is required, add explicit yield points
(e.g. short channel operations or `runtime.Gosched()` calls) in long-running
fibers.

---

## My fibers are deadlocking. How do I debug this?

1. Enable the deadlock detector: `rt.DeadlockDetector().SetEnabled(true)`.
2. Call `rt.DeadlockDetector().GetDeadlocks()` — it returns fiber pairs in a
   wait cycle.
3. The most common cause is **slot exhaustion**: all `numWorkers` slots are
   held by fibers blocked on primitives that can only be resolved by fibers
   waiting in the queue.

**Fix:** increase `numWorkers`. Also check whether fibers are blocking on
`time.Sleep` or system calls — each holds a worker slot for the full duration.

If you're unsure which fibers are blocked, call `rt.GetAllFibers()` and filter
for `State == "Blocked"` to see where each is parked.

---

## Can I cancel a fiber mid-execution?

Not directly — there is no preemptive cancellation. `SpawnWithTimeout` wraps
the fiber with a context deadline: the *caller* unblocks after the timeout,
but the inner goroutine continues until it returns naturally.

To support cooperative cancellation inside a fiber, pass a `context.Context`
into the fiber function and check `ctx.Done()` at logical yield points:

```go
rt.Spawn(func() {
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }
        doOneUnit()
    }
}, "cancellable-worker")
```

---

## What happens if a fiber panics?

The runtime wraps every fiber function in a `recover()` call. If the fiber
panics:

- The panic value and stack trace are captured in the fiber's `PanicInfo`.
- The fiber transitions to `Panicked` state.
- `greenthreads_fibers_panicked_total` is incremented.
- The worker slot is released normally — other fibers keep running.
- The runtime itself is not affected.

Inspect panicked fibers:

```go
for _, h := range rt.GetAllFibers() {
    if h.State == "Panicked" {
        log.Printf("fiber %d (%s) panicked: %v\n%s",
            h.ID, h.Name, h.PanicInfo.Value, h.PanicInfo.Stack)
    }
}
```

---

## RoundRobin looks identical to FIFO in benchmarks. Why?

In this release, `TypeRoundRobin` does not implement preemptive quantum-based
switching. A fiber that does not block runs to completion on its worker,
regardless of scheduler type. True round-robin requires cooperative yield
points or runtime preemption — planned for a future release.

Until then, `TypeRoundRobin` provides the same ordering as FIFO but signals
your intent to treat fibers equally. When quantum-based dispatch ships, code
using `TypeRoundRobin` will gain it automatically.

---

## Is the visualization server safe to expose to the internet?

With a strong `GREENTHREADS_AUTH_TOKEN` and TLS enabled, yes — but treat it
as a debug/observability tool, not a production API.

Security measures in place:

- Bearer-token auth enforced on all non-loopback endpoints.
- `Origin` header validated against a configurable allowlist.
- Max 64 simultaneous WebSocket clients; excess clients get HTTP 429.
- Rate limit: 30 messages / client / second.
- Max message size: 32 KiB.

The UI at `/` is not auth-gated by default. Restrict it via a reverse proxy
if you need to prevent unauthenticated access to the visualization.

---

## How do I write tests that use the runtime?

Construct a real runtime in each test. The scheduler and runtime are
deterministic enough for table-driven tests:

```go
func TestFIFOOrder(t *testing.T) {
    rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
    require.NoError(t, rt.Start())
    defer rt.Stop(context.Background())

    var order []int
    var mu sync.Mutex

    sg := rt.NewSpawnGroup()
    for i := 0; i < 5; i++ {
        i := i
        sg.Spawn(func() {
            mu.Lock()
            order = append(order, i)
            mu.Unlock()
        }, fmt.Sprintf("step-%d", i))
    }
    sg.Wait()

    // With FIFO and a single worker, order is guaranteed.
    // With multiple workers, order is non-deterministic.
    assert.Len(t, order, 5)
}
```

Use `WithNumWorkers(1)` for fully deterministic single-threaded tests. Use
`-race` (already in CI) to catch concurrent access bugs.

---

## What is the goroutine overhead per fiber?

Each fiber runs in exactly one real goroutine. Go's goroutine base stack is
2–8 KiB (depending on runtime version); with `WithStackSize(65536)` the
advisory hint is 64 KiB. The goroutine is created by the scheduler on first
dispatch and lives until the fiber function returns.

Extra per-fiber allocation: one `fiber.Fiber` struct (~200 bytes) plus one
`PriorityItem` struct (~48 bytes) when using the priority scheduler.

For a workload of 10 000 concurrent fibers on a typical machine, expect
roughly 200–640 MB of goroutine stack space (worst-case), bounded by
`WithMaxFibers`.

---

## Can I nest SpawnGroups?

Yes. A fiber can itself create a new `SpawnGroup` and call `sg.Wait()`:

```go
rt.Spawn(func() {
    inner := rt.NewSpawnGroup()
    for i := 0; i < 3; i++ {
        i := i
        inner.Spawn(func() { subTask(i) }, fmt.Sprintf("sub-%d", i))
    }
    inner.Wait()
    finalizeParent()
}, "parent")
```

The outer fiber holds a worker slot while `inner.Wait()` blocks. Ensure
`numWorkers` is large enough to accommodate the nesting depth — if all slots
are held by waiting parents, inner fibers cannot be dispatched.

---

## Does greenthreads work with `go test -race`?

Yes. The race detector is clean — CI runs `go test -race ./...` on every
push (see the `Race detector` step in `ci.yml`). All internal synchronisation
uses `sync.Mutex`, `sync.RWMutex`, and `sync/atomic` — no unsafe memory
operations.

---

## How do I profile fiber dispatch overhead?

Enable the pprof endpoint with `-pprof-addr localhost:6060`:

```bash
go run ./cmd/server -pprof-addr localhost:6060

# CPU profile (30 seconds)
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Goroutine dump (useful for seeing blocked fibers)
go tool pprof http://localhost:6060/debug/pprof/goroutine
```

The pprof endpoint is **always** on a separate address and is never exposed
on the main listener, regardless of configuration.

For microbenchmarks of the scheduler itself, see the `*_bench_test.go` files
in `internal/scheduler/`. Run:

```bash
go test -bench=. -benchmem ./internal/scheduler/...
```
