# Schedulers

greenthreads ships four scheduler implementations behind a single `Scheduler`
interface. You pick one at construction — no other code changes needed.

```go
rt := runtime.NewRuntimeWithOptions(
    runtime.WithSchedulerType(scheduler.TypePriority),
    runtime.WithNumWorkers(8),
)
```

---

## FIFO

**Type constant:** `scheduler.TypeFIFO`

The simplest and fastest scheduler. Fibers are dispatched in strict arrival
order using a mutex-protected slice.

| Property | Value |
|---|---|
| Dispatch complexity | O(1) dequeue |
| Allocs per dispatch | 0 |
| Avg latency | ~200 ns/op |
| Starvation risk | None |

**Use when:** you want predictable, deterministic ordering — pipelines, test
reproducibility, or workloads where all fibers have equal priority.

### Example

```go
rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
if err := rt.Start(); err != nil {
    panic(err)
}
defer rt.Stop(context.Background())

// Fibers run in the order they are spawned.
for i := 0; i < 5; i++ {
    i := i
    rt.Spawn(func() {
        fmt.Printf("fiber %d\n", i)
    }, fmt.Sprintf("step-%d", i))
}
// Output order is deterministic: 0, 1, 2, 3, 4
```

### How it works

Internally `FIFOScheduler` holds a `[]*fiber.Fiber` slice protected by a
`sync.Mutex`. `Add` appends to the tail; `Next` removes from the head. There is
no allocation on either path — the slice grows only when the number of queued
fibers exceeds prior capacity.

---

## RoundRobin

**Type constant:** `scheduler.TypeRoundRobin`

Behaves identically to FIFO in the current release. A fiber that does not
block runs to completion on its worker regardless of scheduler type, because
preemptive quantum-based switching is not yet implemented.

`TypeRoundRobin` is an explicit API contract that signals intent to treat
fibers equally. When quantum-based preemption is added in a future release,
`TypeRoundRobin` will gain it automatically — no code changes needed.

| Property | Value |
|---|---|
| Dispatch complexity | O(1) dequeue |
| Allocs per dispatch | 0 |
| Avg latency | ~200 ns/op |
| Starvation risk | None |

**Use when:** you want equal-treatment semantics and want to benefit from
quantum scheduling when it ships. Use `TypeFIFO` if you only need determinism
today without the intent signal.

---

## Priority

**Type constant:** `scheduler.TypePriority`

Dispatches the highest-priority waiting fiber on every `Next()` call using a
min-heap. Lower numeric values = higher priority (priority 1 runs before
priority 100).

| Property | Value |
|---|---|
| Dispatch complexity | O(log n) heap pop |
| Allocs per dispatch | 1 (heap element) |
| Avg latency | ~400 ns/op |
| Starvation risk | Low — aging mitigates it |

**Anti-starvation aging:** every 100 heap pops, every waiting fiber's priority
is boosted by 1. This prevents indefinite deferral under constant high-priority
load. Trigger an immediate boost with `scheduler.(*PriorityScheduler).AgeAll()`.

**Use when:** your workload has mixed criticality — critical control-plane
fibers that must run as soon as a slot is free, alongside background batch
workers.

### Example: mixed priorities

```go
rt := runtime.NewRuntimeWithOptions(
    runtime.WithSchedulerType(scheduler.TypePriority),
    runtime.WithNumWorkers(4),
)
if err := rt.Start(); err != nil {
    panic(err)
}
defer rt.Stop(context.Background())

// Low-priority batch work.
for i := 0; i < 20; i++ {
    i := i
    rt.Spawn(func() {
        processBatch(i)
    }, fmt.Sprintf("batch-%d", i))
}

// High-priority control path — will be dispatched first once a slot is free.
rt.Spawn(func() {
    flushControlSignal()
}, "flush")
// To set priority, use SpawnWithOptions (or the fiber.WithPriority option):
// rt.SpawnWithOptions(fn, "flush", fiber.WithPriority(1))
```

!!! note "Priority is not preemptive"
    A high-priority fiber spawned while all workers are busy is the *first to
    run when a slot becomes free*, but it cannot forcibly interrupt a currently
    executing fiber. Add explicit yield points (short channel operations) if
    you need finer-grained responsiveness.

### Priority heap internals

`PriorityScheduler` wraps the standard library `container/heap` with a
`PriorityQueue` type. Each `PriorityItem` caches the priority value so heap
comparisons never take a lock on the fiber struct. The `index` field in each
item enables O(log n) `heap.Fix` and `heap.Remove` without a linear scan.

---

## WorkStealing

**Type constant:** `scheduler.TypeWorkStealing`

Each worker has its own local deque. When a worker's deque is empty, it finds
the busiest other worker and steals half its fibers. This reduces lock
contention on a single shared queue and improves utilization when fiber
execution times are uneven.

| Property | Value |
|---|---|
| Dispatch (local pop) | O(1) |
| Steal path | O(n workers) scan + O(k) transfer |
| Allocs per dispatch | 0 (steal path may alloc) |
| Avg latency | ~350 ns/op |
| Starvation risk | Low — stealing rebalances continuously |

**Use when:** fibers have highly uneven execution times, or the workload is
CPU-bound with many independent tasks and you want to maximize core
utilization.

**Avoid when:** all fibers are short and uniform — FIFO's lower per-op
overhead wins in that case.

### Example: CPU-bound fan-out

```go
rt := runtime.NewRuntimeWithOptions(
    runtime.WithSchedulerType(scheduler.TypeWorkStealing),
    runtime.WithNumWorkers(runtime.NumCPU()),
    runtime.WithMaxFibers(10_000),
)
if err := rt.Start(); err != nil {
    panic(err)
}
defer rt.Stop(context.Background())

sg := rt.NewSpawnGroup()
for _, item := range largeDataset {
    item := item
    sg.Spawn(func() {
        heavyCompute(item)
    }, "worker")
}
sg.Wait()
```

### How work stealing works

Each `WorkStealingScheduler` maintains a `[]deque` slice — one per worker. On
`Add`, the fiber is appended to the worker with the shortest local deque (round-
robin tiebreak). On `Next(workerID)`, the worker pops from its own deque first.
If empty, it scans all workers, finds the one with the most fibers, and moves
half of them into its own deque before returning the first.

---

## Performance comparison

Benchmark numbers from CI (`linux/amd64`, no race detector). Values represent
scheduler dispatch overhead only, excluding fiber function runtime.

| Scheduler | BenchmarkNext | BenchmarkConcurrent |
|---|---|---|
| FIFO | ~200 ns/op, 0 allocs | ~210 ns/op |
| RoundRobin | ~200 ns/op, 0 allocs | ~210 ns/op |
| Priority | ~400 ns/op, 1 alloc | ~420 ns/op |
| WorkStealing | ~350 ns/op, 0 allocs | ~370 ns/op |

See `scripts/check_benchmarks.sh` for the CI gate thresholds.

---

## Common pitfalls

### Slot exhaustion deadlock

All `numWorkers` slots are held by fibers blocked on primitives that can only
be resolved by fibers still in the queue.

```
Workers: [fiber-A blocked on mu] [fiber-B blocked on mu] [fiber-C blocked on mu] [fiber-D blocked on mu]
Queue:   [fiber-E holds mu, waiting to be dispatched]
```

**Fix:** ensure `numWorkers > max simultaneously blocked fibers`. The deadlock
detector surfaces this automatically when enabled.

### Misunderstanding priority with non-preemptive scheduling

```go
// ❌ This does NOT interrupt the running fiber:
go func() {
    time.Sleep(1 * time.Millisecond)
    rt.Spawn(urgentFiber, "urgent")  // urgent joins the queue, doesn't preempt
}()
```

High-priority fibers are first in the queue — not first in the CPU. Running
fibers finish naturally before the high-priority one is dispatched.

### Holding locks across blocking system calls

```go
// ❌ Worker slot is held for the entire sleep:
rt.Spawn(func() {
    mu.Lock(f)
    time.Sleep(5 * time.Second)  // holds slot + lock for 5s
    mu.Unlock()
}, "bad")
```

Each sleeping/blocking fiber occupies a worker slot. Use `WithNumWorkers` to
account for worst-case simultaneous blockers.

---

## Choosing a scheduler

```
Is strict arrival order important?
  Yes → FIFO

Do fibers have different criticality levels?
  Yes → Priority

Are fibers CPU-bound with uneven durations or many workers?
  Yes → WorkStealing

Are all fibers short and uniform?
  → FIFO (lowest overhead)

Do you want equal-treatment semantics to opt in to future quantum scheduling?
  → RoundRobin
```
