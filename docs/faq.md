# FAQ

---

## What is the difference between a greenthreads fiber and a Go goroutine?

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

The general rule: `numWorkers > max_simultaneously_blocked_fibers`.

Each blocked fiber holds its worker slot. If all slots are occupied by fibers
waiting on work from fibers that have not yet been dispatched, the runtime
deadlocks.

- **Compute-bound, no blocking:** `runtime.NumCPU()` (the default) is a good
  starting point.
- **IO-heavy or mutex-heavy:** increase workers significantly — 2x or 4x CPU
  count is common.

The deadlock detector will surface slot-exhaustion deadlocks when they occur.

---

## Does Priority scheduling guarantee that a high-priority fiber always runs first?

Yes, *among fibers waiting in the queue*. A fiber that is already running is
not preempted — greenthreads uses cooperative (non-preemptive) scheduling.

A high-priority fiber spawned while all workers are busy will be the first to
run when a slot becomes free, but it cannot forcibly interrupt a running fiber.

For workloads where preemptive priority is required, add explicit yield points
(e.g. short channel operations) in long-running fibers.

---

## My fibers are deadlocking. How do I debug this?

1. Enable the deadlock detector: `rt.DeadlockDetector().SetEnabled(true)`.
2. The detector populates `GetDeadlocks()` with the offending fiber IDs.
3. The most common cause is **slot exhaustion**: all `numWorkers` slots are
   held by fibers blocked on primitives that can only be resolved by fibers
   waiting in the queue.

**Fix:** increase `numWorkers`. Check also whether fibers are blocking on
`time.Sleep` or system calls — each holds a worker slot for the full duration.

---

## Can I cancel a fiber mid-execution?

Not directly — there is no preemptive cancellation. `SpawnWithTimeout` wraps
the fiber with a `context.Context` deadline: the *caller* unblocks after the
timeout, but the inner goroutine continues until it returns naturally.

To support cooperative cancellation inside a fiber, pass a `context.Context`
into the fiber function and check `ctx.Done()` at logical yield points.

---

## What happens if a fiber panics?

The runtime wraps every fiber function in a `recover()` call. If the fiber
panics:

- The panic value and stack trace are captured in the fiber's `PanicInfo`.
- The state transitions to `Panicked`.
- `greenthreads_fibers_panicked_total` is incremented.
- The worker slot is released normally.
- The runtime and other fibers continue running unaffected.

Inspect panicked fibers via `rt.GetAllFibers()` and check the `PanicInfo`
field.

---

## RoundRobin looks identical to FIFO in benchmarks. Why?

In this release, `TypeRoundRobin` does not implement preemptive quantum-based
switching. A fiber that does not block runs to completion on its worker,
regardless of scheduler type. True round-robin requires cooperative yield
points — planned for a future release.

Until then, `TypeRoundRobin` provides the same ordering guarantee as FIFO but
with an explicit API contract that signals your intent to treat fibers equally.

---

## Is the visualization server safe to expose to the internet?

With a strong `GREENTHREADS_AUTH_TOKEN` and TLS enabled, yes — but treat it as
a debug/observability tool, not a production API.

Security measures in place:

- Bearer-token auth on all non-loopback endpoints.
- `Origin` header validation against a configurable allowlist.
- Max 64 simultaneous WebSocket clients.
- Rate limit: 30 messages / client / second.
- Max message size: 32 KiB.

The UI at `/` is not auth-gated by default — restrict it via a reverse proxy
if needed. The pprof endpoint (`-pprof-addr`) is always on a separate address.
