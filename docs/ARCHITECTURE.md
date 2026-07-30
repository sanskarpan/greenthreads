# Architecture

This document describes the internal design of greenthreads. It is aimed at contributors and operators who need to understand how the pieces fit together. For usage documentation, see the README.

## Table of Contents

- [Execution Model](#execution-model)
- [Runtime Lifecycle](#runtime-lifecycle)
- [Scheduler Design](#scheduler-design)
- [Fiber State Machine](#fiber-state-machine)
- [Sync Primitives](#sync-primitives)
- [Deadlock Detector](#deadlock-detector)
- [WebSocket Control Plane](#websocket-control-plane)
- [Metrics Architecture](#metrics-architecture)
- [Known Constraints and Design Decisions](#known-constraints-and-design-decisions)

---

## Execution Model

greenthreads implements a **cooperative, goroutine-backed fiber model**. Each fiber is backed by exactly one Go goroutine. The runtime does not perform stackful context switching — there is no `ucontext_t`, no `setjmp`/`longjmp`, and no manual stack manipulation.

The consequence is that goroutine scheduling ultimately belongs to the Go runtime (GOMAXPROCS workers, work-stealing, preemption at safe points). greenthreads adds a layer of structured admission control on top: a fiber must be admitted by the greenthreads scheduler before its goroutine starts, and its lifecycle (state transitions, completion recording, metrics) is managed by the runtime, not by the goroutine itself.

This is sometimes called a "virtual thread" or "green thread" model at the library level. The trade-off versus stackful fibers is:

- No assembly, no platform-specific stack walking — works on all Go-supported platforms
- Preemption is handled by the Go runtime at safe points; no cooperative yield is required for CPU-bound fibers
- A fiber that blocks in a C call or system call holds the goroutine, not just the fiber — if this is frequent, increase `numWorkers`

---

## Runtime Lifecycle

```
NewRuntime(schedulerType, numWorkers)
    │
    ▼
[stopped]
    │
    ├── Start() / StartWithContext(ctx)
    │       │
    │       ▼
    │   [running]
    │       │
    │       ├── Spawn(fn, name)  ──► fiber admitted to scheduler
    │       │
    │       └── Stop(ctx)
    │               │
    │               ▼
    │           [draining]  ← waits for lifecycle goroutines, then fiber goroutines
    │               │
    │               ▼
    │           [stopped]
    │
    └── Reset()  ← clears fibers, scheduler queues, metrics; runtime can be restarted
```

Key invariants:

- `Start` and `Stop` are serialized by `lifecycleMu`. A `Start` cannot begin until the previous `Stop` has fully drained.
- Each `Start` increments an epoch counter. Goroutines capture the epoch's `runCtx` and `resultChan` by value so a new `Start` cannot redirect a stale goroutine.
- `Stop(ctx)` bounds the wait for in-flight fiber goroutines by `ctx`. If the context expires, `Stop` returns `ctx.Err()` and marks the runtime stopped, but stale goroutines may still be running. This is the correct behavior for a graceful shutdown deadline.
- `Reset` clears all state and requires the runtime to be stopped. It is a pre-condition for reuse in a new test run or scheduling epoch.

The `stopped` channel is the synchronization signal between a completed `Stop` and the next `Start`. It is initialized to a closed channel so the very first `Start` does not need to special-case "no prior run."

---

## Scheduler Design

### Interface

All scheduler implementations satisfy `scheduler.Scheduler`:

```go
type Scheduler interface {
    Schedule(f *fiber.Fiber) error
    Next() (*fiber.Fiber, error)
    Remove(fiberID fiber.FiberID) error
    MarkCompleted(fiberID fiber.FiberID)
    BlockFiber(f *fiber.Fiber)
    UnblockFiber(fiberID fiber.FiberID) error
    GetRunQueue() []*fiber.Fiber
    GetBlockedQueue() []*fiber.Fiber
    Size() int
    Type() SchedulerType
    Start() error
    Stop() error
    Clear()
    IsRunning() bool
    GetStats() SchedulerStats
}
```

`BlockFiber` and `UnblockFiber` are the protocol between sync primitives and the scheduler. When a fiber parks on a `FiberChannel` or `FiberMutex`, the sync primitive calls `fiber.Block(reason, obj)` to update the fiber's state, and the scheduler's `BlockFiber` moves it out of the run queue into the blocked queue. When the condition is satisfied, `UnblockFiber` moves it back.

### Implementations

| Type         | Policy                                                                 | Default quantum |
| ------------ | ---------------------------------------------------------------------- | --------------- |
| FIFO         | First-in-first-out; no reordering once admitted                       | n/a             |
| RoundRobin   | FIFO with a per-fiber time quantum; fiber is re-queued after expiry   | 10ms            |
| Priority     | Heap ordered by `Fiber.Priority` (higher value = higher priority); FIFO within a priority tier | n/a |
| WorkStealing | N worker-local FIFO queues; idle workers steal from the tail of the longest queue | n/a |

`WorkStealing` is the only scheduler that creates internal goroutines (one per worker). All others are driven by the runtime's execution loop.

`BaseScheduler` provides the shared queue management, `BlockFiber`/`UnblockFiber`, `MarkCompleted`, and statistics. Concrete schedulers embed `BaseScheduler` and override only `Next()`.

`MarkCompleted` is idempotent: multiple calls for the same fiber ID are safe because the runtime dispatch path and the filter path may both call it. The completed set is pruned to 4096 entries to bound memory growth across very long runs.

---

## Fiber State Machine

```
         ┌────────────────────────────────────┐
         │                                    │
         ▼                                    │
      [Ready] ──── scheduler.Next() ──► [Running]
         ▲                │                   │
         │                │ Block()            │
         │                ▼                   │
         │           [Blocked]                │ fn() returns
         │                │                   │ (or panics)
         │         Unblock()                  │
         └────────────────┘                   ▼
                                         [Finished]
                                              │
                                         complete()
                                              │
                                              ▼
                                          [Dead]
                                       (reaped from rt.fibers)
```

State transitions are guarded by `Fiber.mu`. The state is stored as `FiberState int32` for atomic-compatible reads where the full lock is too coarse, but all writes go through the lock.

A fiber that panics still transitions to `StateFinished`. The panic is captured as `Fiber.failure` and the stack trace as `Fiber.panicStack`. The runtime logs the panic to stderr and records it in the event tracker, but does not terminate the process.

`IsRunnable()`, `IsBlocked()`, and `IsFinished()` are the three query predicates used by the scheduler and deadlock detector. They acquire `Fiber.mu.RLock()`.

---

## Sync Primitives

All sync primitives in `internal/sync` follow the same pattern: a queue of waiting fibers, an explicit handoff, and a fiber state update via `fiber.Block` / `fiber.Unblock`.

### FiberChannel

```
Send(value, currentFiber):
  1. Lock channel
  2. If receiver waiting → handoff value directly → Unblock receiver → close receiver.ready
  3. Else if buffer has space → append to buffer
  4. Else → park currentFiber in senders queue → Block(currentFiber) → unlock → wait on sender.ready

Receive(currentFiber):
  1. Lock channel
  2. If buffer has value → dequeue → admitSenderLocked (unblock first waiting sender if buffer has room)
  3. Else if sender waiting → dequeue → Unblock sender → close sender.ready → return sender.value
  4. Else if closed → return ErrChannelClosed
  5. Else → park currentFiber in receivers queue → Block(currentFiber) → unlock → wait on receiver.ready
```

`admitSenderLocked` is the key optimization: when a receiver drains a buffered slot, it immediately admits the first waiting sender into the buffer rather than requiring a separate scheduler wakeup cycle.

`SendCtx` and `ReceiveCtx` add a `context.Context` race: on cancellation, they re-check under the lock to avoid a TOCTOU window where a handoff races with context cancellation.

### FiberMutex and FiberRWMutex

Thin wrappers around `sync.Mutex` / `sync.RWMutex` that additionally call `fiber.Block` / `fiber.Unblock` when contention is detected, keeping the fiber state machine accurate for the deadlock detector.

### FiberWaitGroup

Identical semantics to `sync.WaitGroup` but with fiber-aware parking: fibers that call `Wait()` park themselves until the counter reaches zero.

---

## Deadlock Detector

The detector runs as a goroutine owned by the runtime, started by `Start` and stopped by `Stop`. It scans the fiber map on a configurable interval (default: 1 second) and applies a configurable timeout (default: 5 seconds) before declaring suspicion.

### Scan Algorithm

```
For each fiber in rt.GetAllFibers():
  skip the main observer fiber
  if IsBlocked() → add to blocked list
  if IsRunnable() or IsRunning() → increment runnable count

noRunnable = (runnable == 0 && len(blocked) > 0)

if not noRunnable:
  lastProgress = now
  if deadlockActive: mark as resolved

if noRunnable && (now - lastProgress) >= timeout:
  if not deadlockActive: record DeadlockInfo snapshot, set deadlockActive = true
```

The detector records suspicion, not proof. It cannot distinguish a legitimate "all fibers blocked waiting for external I/O" from a true deadlock where fibers are blocked waiting on each other with no resolution possible. The timeout is the primary heuristic — a brief all-blocked window (e.g., during a burst of channel handoffs) is not flagged.

History is capped at 100 entries. Configuration (enabled, interval, timeout) survives a `Stop`/`Start` cycle because `Start` copies these fields from the previous detector instance.

---

## WebSocket Control Plane

The `web` package exposes a WebSocket endpoint at `/ws` and an HTTP metrics endpoint at `/metrics`.

### Message Flow

```
Client                          Server (web.Server)
  │                                     │
  │── Upgrade (GET /ws) ──────────────► │
  │                                     │ auth check (token header or ?token=)
  │                                     │ origin check
  │◄── 101 Switching Protocols ─────── │
  │                                     │
  │── {"action":"spawn","name":"f"} ──► │ handleWebSocket loop
  │                                     │  → rt.Spawn(fn, name)
  │◄── {"type":"spawn","fiberID":42} ── │
  │                                     │
  │── {"action":"stop"} ──────────────► │ → rt.Stop(ctx)
  │◄── {"type":"update",...} ──────────│ broadcast goroutine (ticker-driven)
```

The broadcast goroutine runs on a configurable interval (default: 100ms) and publishes `RuntimeUpdate` snapshots — fiber list, metrics snapshot, recent events — to all connected clients. It is separate from the WebSocket read loop so a slow client cannot block the runtime.

### Auth Model

Authentication is bearer-token. The token is compared with `subtle.ConstantTimeCompare` to prevent timing attacks.

Two token slots exist:

- `Config.AuthToken` — full-access (spawn, stop, reset, read)
- `Config.ReadOnlyToken` — read-only (receive updates, query metrics; cannot mutate runtime state)

On loopback addresses (127.0.0.1, ::1) with no token configured, the server operates in unauthenticated dev mode with a warning. On non-loopback addresses, authentication is required.

`AllowTokenInQuery` allows the token via `?token=` for browser WebSocket clients that cannot set `Authorization` headers. It is disabled by default because tokens in URLs appear in server logs.

`TokenRevalidationInterval` enables periodic re-checking of live connections. When set, a connection that presented a valid token at upgrade time will be disconnected if the token changes before the next revalidation.

### Rate Limiting

Each client has a token-bucket rate limiter (default: 30 messages/second). The limit is configurable via `GREENTHREADS_MSG_PER_SEC`. Messages that exceed the rate limit are dropped with a `429 Too Many Requests`-equivalent WebSocket response, not disconnected.

---

## Metrics Architecture

### Two Counter Views

`GetMetrics()` returns a **per-run snapshot**: counters reset to zero on each `Reset()` call. This is useful for per-benchmark reporting.

`GetLifetimeMetrics()` returns a **lifetime snapshot**: the five monotonic counters (`FibersCreated`, `FibersCompleted`, `ContextSwitches`, `Yields`, `ScheduleCalls`) are cumulative across all `Reset()` calls. This is the correct view for Prometheus exposition because Prometheus counters must never decrease between scrapes.

The implementation uses a `counterOffset` struct that accumulates the "lost" counter value each time `Reset()` is called. `GetLifetimeSnapshot()` adds the offset to the current per-run value.

### Prometheus Exposition

`/metrics` returns Prometheus text format. Each metric has `# HELP` and `# TYPE` metadata headers so Prometheus can scrape it without a hand-written recording rule. The endpoint requires authentication on non-loopback configurations.

### Event Tracker

`EventTracker` is a bounded ring buffer (default: 10,000 events) of `FiberEvent` records. Events include `EventCreated`, `EventScheduled`, `EventRunning`, `EventCompleted`, and `EventBlocked`. The WebSocket broadcast includes the most recent N events in each update payload.

---

## Known Constraints and Design Decisions

### Why no stackful context switching?

Stackful switching requires platform-specific assembly or CGo and makes the code non-portable. The Go runtime already provides M:N scheduling with work-stealing, preemption at safe points, and goroutine-per-core dispatch. Adding a greenthreads layer on top of goroutines adds structured admission control and observability without re-implementing what the runtime already does well.

The cost is that a fiber blocked in a cgo call or a raw syscall holds its OS thread. For the target workload (short, CPU-bound or channel-driven fibers), this is acceptable.

### Why internal/ only?

The public API surface for a concurrency library must be extremely stable. Exposing types like `Fiber`, `Scheduler`, and `Runtime` directly before the semantics are finalized would commit us to backward compatibility for external callers before the design is complete. `internal/` enforces this at the compiler level. A stable `pkg/` surface will be introduced in a future milestone after the core semantics are proven.

### Why is there a simulated stack?

`Fiber.Stack` is a bounded byte slice that simulates a call stack for the WebSocket visualization layer. It is not a real execution stack — fibers execute on the Go goroutine stack. The simulated stack lets the UI show push/pop events without requiring actual stack introspection.

### Why is the epoch counter int64 and not uint64?

The epoch is incremented under `rt.mu` write lock, so it does not need to be atomic. `int64` avoids the overflow check that would be needed if it were used in arithmetic with `uint64` fields on 32-bit platforms (which Go requires to be 64-bit aligned at a 4-byte address boundary).

### Bounded completed set in BaseScheduler

`MarkCompleted` maintains a map of seen fiber IDs to make the call idempotent. To prevent unbounded memory growth in long-running processes that spawn millions of short-lived fibers, the set is pruned to 2048 entries when it exceeds 4096. The only consequence is a potential +1 overcount in `TotalCompleted` for a pruned ID, which is negligible in practice.
