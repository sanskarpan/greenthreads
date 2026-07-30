# Schedulers

greenthreads ships four scheduler implementations behind a single `Scheduler`
interface. You pick one at runtime construction — no other code changes needed.

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

- **Dispatch:** O(1) dequeue from the front of the slice.
- **Allocs per dispatch:** 0.
- **Avg latency:** ~200 ns/op.
- **Starvation risk:** None — every fiber eventually reaches the front.

**Use when:** you want predictable, deterministic ordering; useful for
pipelines, test reproducibility, and workloads where all fibers have equal
priority.

---

## RoundRobin

**Type constant:** `scheduler.TypeRoundRobin`

Behaves identically to FIFO in the current release. A fiber that does not
block runs to completion on its worker regardless of scheduler type, because
preemptive quantum-based switching is not yet implemented.

`TypeRoundRobin` is an explicit API contract that signals your intent to treat
fibers equally. When preemptive yielding is added in a future release,
`TypeRoundRobin` will automatically gain quantum-based dispatch without code
changes.

- **Avg latency:** ~200 ns/op.
- **Starvation risk:** None.

**Use when:** you want equal-treatment semantics and plan to benefit from
quantum scheduling in a future release. Use `TypeFIFO` if you need the same
behaviour today without the intent signal.

---

## Priority

**Type constant:** `scheduler.TypePriority`

Dispatches the highest-priority waiting fiber on every `Next()` call using a
min-heap keyed by priority. Lower numeric values = higher priority (priority 1
is dispatched before priority 100).

**Anti-starvation aging:** after every 100 heap pops, the scheduler boosts the
priority of all waiting fibers by 1. This prevents indefinite deferral of
low-priority fibers under constant high-priority load. You can trigger an
immediate boost with `scheduler.(*PriorityScheduler).AgeAll()`.

- **Dispatch:** O(log n) heap pop.
- **Allocs per dispatch:** 1 (heap element).
- **Avg latency:** ~400 ns/op.
- **Starvation risk:** Low — aging mitigates it.

**Use when:** your workload has mixed criticality: critical control-plane
fibers that must run immediately alongside background batch workers.

!!! note "Priority is not preemptive"
    A high-priority fiber spawned while all workers are busy will be the
    **first to run when a slot becomes free**, but it cannot forcibly interrupt
    a currently running fiber.

---

## WorkStealing

**Type constant:** `scheduler.TypeWorkStealing`

Each worker has its own local deque. When a worker's deque is empty, it steals
half the fibers from the busiest worker's deque. This reduces contention on a
single shared queue and improves CPU utilization when fiber execution times are
uneven.

- **Dispatch (no steal):** O(1) local pop.
- **Steal path:** O(n) scan to find the busiest worker + O(k) transfer.
- **Allocs per dispatch:** 0 (steal path may alloc during transfer).
- **Avg latency:** ~350 ns/op.
- **Starvation risk:** Low — stealing rebalances load continuously.

**Use when:** fibers have highly uneven execution times, or your workload is
CPU-bound with many independent tasks and you want to maximize core utilization.

**Avoid when:** all fibers are short and uniform — FIFO's lower overhead wins
in that case.

---

## Choosing a scheduler

```
Is ordering important?
  Yes → FIFO

Do fibers have different criticality?
  Yes → Priority

Are fibers CPU-bound with uneven durations?
  Yes → WorkStealing

Otherwise → FIFO (lowest overhead)
```
