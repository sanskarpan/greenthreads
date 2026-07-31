# Issues Registry

This file is the authoritative list of open, confirmed, and resolved defects and improvement areas across the greenthreads library. Findings come from four sources: the original author audit (AUDIT.md, 26 issues closed), a July 2026 first deep-dive (20 issues, 16 now resolved), and a July 2026 comprehensive seven-domain parallel audit covering architecture, performance, security, testing, observability, concurrency correctness, and documentation.

Severities: **Critical** (data loss / hard crash / exploitable) → **Major** (confirmed bug, wrong behaviour in production) → **Minor** (limited blast radius, edge case) → **Nit** (documentation / style).

---

## Open Issues

> **Reconciled 2026-07:** RT-4, RT-5, RT-6, SC-4, SC-6, SC-7 below were verified
> already fixed, and RT-7 (fiber-cap leak), SC-8 (WorkStealing blocked-queue
> race), the OBS-5 panic counter, and the SY-2 *detection* gap were fixed in the
> correctness follow-up. See "Resolved: July 2026 Correctness Follow-up" for
> evidence and regression tests. Entries are kept here for historical context;
> their status lines note the resolution.

### Runtime & Deadlock Detector

---

#### RT-4 · Major · Spawn/Stop race window not eliminated; code comment overclaims
**File:** `internal/runtime/runtime.go:163-167, 223-232`

The comment at line 163 states "This eliminates the Spawn/Stop race where Spawn returned success against a stopped runtime." There is no CAS; the admission check is: read `isStopped()` → call `Schedule()` → re-read `isStopped()`. The `stopped` channel is closed at the **end** of `Stop` (after cancel, detector stop, scheduler stop, and both WaitGroup waits). A Spawn whose final re-check runs after Stop has begun but before `stopped` is closed returns a valid `FiberID` for a fiber whose dispatcher goroutine has already exited. The fiber never executes; any join waits forever.

**Fix:** Test `runCtx.Done()` in the re-check rather than `isStopped()`, so admission is gated on "Stop initiated" rather than "Stop completed". Correct the comment.

---

#### RT-5 · Major · Fiber-map leak: unreaped fibers accumulate across Stop
**File:** `internal/runtime/runtime.go:421-432, 483-487, 517-519`

`complete()` is the only place fibers are deleted from `rt.fibers`. On Stop: in-flight fibers that finish after the execution loop exits drop their result via the `runCtx.Done()` select arm — `complete()` never runs, so the fiber stays in `rt.fibers` with its full stack allocation. Fibers admitted to the scheduler but never dispatched at Stop time also persist (`scheduler.Clear` is only called from `Reset`, not `Stop`). Across repeated Start/Stop cycles without `Reset`, `rt.fibers` grows monotonically. Stale entries appear in `GetAllFibers()` snapshots of subsequent runs.

**Fix:** Drain and reap `rt.fibers` in `Stop` after `fiberWG.Wait` returns or after the context deadline, calling `rt.scheduler.Clear()` and deleting stale map entries.

---

#### RT-6 · Minor · Deadlock detector configuration discarded on every Start
**File:** `internal/runtime/runtime.go:275, 284`; `internal/runtime/deadlock.go:191-234`

`Start` always creates a fresh `DeadlockDetector` with hardcoded defaults (`enabled=true`, 1 s interval, 5 s timeout). Any prior call to `SetEnabled(false)`, `SetTimeout`, or `SetCheckInterval` is silently reverted.

**Fix:** Accept detector configuration as a `RuntimeOption` on `NewRuntime`, or copy the prior detector's settings into the new one at `Start` time.

---

### Fiber

No open issues from the original audit. See CON-4 and DOC-1 for new findings affecting the fiber package.

---

### Scheduler

---

#### SC-3 · Major · `BlockFiber`/`UnblockFiber` are incoherent for Priority and WorkStealing
**File:** `internal/scheduler/scheduler.go:289-324`

`BaseScheduler.BlockFiber` removes the fiber from `s.runQueue` (empty for Priority/WS) and appends to `s.blockedQueue`. The fiber remains in `s.pqueue` / worker `localQueue`, so `Next()` will dispatch a "blocked" fiber. `BaseScheduler.UnblockFiber` appends the fiber to `s.runQueue`, which Priority/WS `Next()` never reads — the fiber is enqueued but can never dispatch (lost fiber). `BlockFiber` does not deduplicate: calling it twice puts two copies in `blockedQueue`.

**Fix:** Override `BlockFiber`/`UnblockFiber` in both subschedulers (operating on `pqueue` and `localQueues` respectively), or remove them from the exported surface.

---

#### SC-4 · Minor · Priority starvation: `AgeAll` is dead code
**File:** `internal/scheduler/priority.go:195-203`

`AgeAll` exists as the anti-starvation mechanism for `PriorityScheduler` but is never called outside a single package-internal test. Under continuous high-priority spawns, low-priority fibers are never dispatched.

**Fix:** Wire `AgeAll` to be called periodically inside the priority scheduler's `Next()` (e.g., every N pops), or remove the promise from the docs and document the starvation property explicitly.

---

#### SC-6 · Minor · WorkStealing global queue is write-dead (never populated)
**File:** `internal/scheduler/workstealing.go:61-92`

`Schedule()` always places fibers into a worker local queue despite its comment claiming "adds to the global queue." `globalQueue` is never written by `Schedule()`. The global-queue branch in `Next()` and `filterFinished` can never return a fiber.

**Fix:** Wire `Schedule()` to overflow into `globalQueue` when all worker queues exceed a threshold, or remove `globalQueue` and the dead branches.

---

#### SC-7 · Nit · `nextWorker` wraparound at `int64` max
**File:** `internal/scheduler/workstealing.go:100`

After 2^63 `Next()` calls, the `atomic.AddInt64` counter wraps negative and `% s.numWorkers` produces a negative index, causing a panic. Not reachable in practice but should be guarded.

**Fix:** Use `atomic.AddUint64` and convert with `% uint64(s.numWorkers)`.

---

### Sync Primitives

---

#### SY-2 · Major · Blocking primitives pin worker slots; bounded dispatch can hard-deadlock
**File:** `internal/runtime/runtime.go:438-449`; `internal/sync/mutex.go`, `channel.go`, `waitgroup.go`

A fiber parked inside `Lock`/`Receive`/`Wait` keeps its dispatch-goroutine alive and its worker slot occupied until its function returns. There is no "blocked fiber releases its slot" pathway. Scenario: `numWorkers` fibers all block on a `FiberChannel` receive whose sender fiber has not yet been dispatched → sender can never be dispatched → permanent deadlock. The deadlock detector **now flags this** (fixed — see "Resolved: July 2026 Correctness Follow-up"), but nothing can break the cycle: detection is diagnostic only.

This is a design-level property: the execution model commits a goroutine per dispatched fiber with no preemption or slot-release on block. The **detection** gap is resolved; the **design** limitation (no slot release on block) remains open and is documented below.

**Fix (design):** Document this fundamental limitation in the package-level godoc and README. Add a runtime check: before parking a fiber, verify that at least one non-blocked worker slot exists; if not, log a warning. Long-term: consider an M:N dispatch model where blocked fibers release their slot.

---

### Web / Ops

---

#### WO-6 · Minor · Docker base images use floating tags, not digests
**File:** `Dockerfile:1, 9`

`golang:1.26.5-alpine` and `gcr.io/distroless/static-debian12:nonroot` are mutable tags. A supply-chain compromise of the base images is not detected until the image is built.

**Fix:** Pin both base images by `@sha256:<digest>` and document the pinned digests with their semantic version as a comment.

---

### API Design & Architecture

---

#### API-1 · Major · RoundRobinScheduler is functionally FIFO; ShouldPreempt() never called
**File:** `internal/scheduler/roundrobin.go:11, 44-55, 77-82`

The type-level comment says "Each fiber gets a time quantum and is rotated to the back of the queue." This is false. `RoundRobinScheduler.Next()` pops the first entry and returns — it never re-enqueues running fibers. The runtime's `runExecutionLoop` never calls `ShouldPreempt()`. The `quantum` field, `SetQuantum()`/`GetQuantum()` and `ShouldPreempt()` are all dead API surface. Users selecting `TypeRoundRobin` expect preemption; they get identical FIFO behaviour.

**Fix:** Either implement true round-robin by calling `ShouldPreempt()` in the execution loop and re-enqueuing running fibers, or rename to `TypeFairFIFO` and correct all documentation.

---

#### API-2 · Major · All blocking sync calls require explicit `*fiber.Fiber` — breaks composition
**File:** `internal/sync/mutex.go:31`; `channel.go:58`; `waitgroup.go:65`

`Lock(currentFiber *fiber.Fiber)`, `Send(value interface{}, currentFiber *fiber.Fiber)`, `Wait(currentFiber *fiber.Fiber)` all require the caller to hold a live `*fiber.Fiber`. This pointer is only obtainable via `GetFiberDirect()` (an unsafe live pointer). Code deep in a call stack must thread the fiber pointer down through every intermediate function. Library functions that don't know whether they're called from inside a fiber or a regular goroutine have no safe option.

**Fix:** Explore goroutine-local storage (via a goroutine-ID-keyed map in the runtime), or thread the fiber pointer via a closure captured at spawn time and stored in a context analogue.

---

#### API-3 · Major · `FiberChannel` uses `interface{}` — pre-generics API, unsafe casts at every use site
**File:** `internal/sync/channel.go:19, 34`

Every `Receive()` returns `interface{}`. Callers must type-assert on every read. With Go 1.18+ generics available, a wrong type assertion is a runtime panic, not a compile error. The channel's type contract is enforced only by convention.

**Fix:** Replace `FiberChannel` with `FiberChannel[T any]` using generics. Keep `FiberChannel` as a `FiberChannel[interface{}]` alias for backwards compatibility.

---

#### API-4 · Major · No fiber completion notification — no `Await` or `Future` primitive
**File:** `internal/runtime/runtime.go:487-515`

There is no `WaitForFiber(id FiberID)` or equivalent. `GetFiber()` returns a snapshot that may show `StateFinished`, but once `complete()` runs, the fiber is deleted from `rt.fibers`. A caller that does `GetFiber` after completion gets "fiber not found", not the result. `Failure()` is only accessible via the live fiber pointer from `GetFiberDirect()`. There is no standard library pattern for fiber result capture.

**Fix:** Add a `SpawnWithResult[T](fn func() T, ...) (FiberID, <-chan T, error)` or a `Future[T]` type that can be awaited after `Stop()`.

---

#### API-5 · Major · `Yield()` not implemented; `YieldCount`/`TotalYields` always 0
**File:** Entire runtime package

There is no `Runtime.Yield()`. `Fiber.YieldCount` and `Metrics.TotalYields` exist but are never incremented. `EventYielded` is defined but never emitted. A fiber cannot voluntarily give up its worker slot. The "green threads" branding implies cooperative scheduling that does not exist.

**Fix:** Implement `Runtime.Yield()` that: sets the fiber to `StateReady`, increments `YieldCount`, notifies the execution loop, and parks the fiber's goroutine until re-dispatched. This requires slot-release semantics.

---

#### API-6 · Major · No per-fiber execution timeout
**File:** `internal/runtime/runtime.go`

`Stop(ctx)` bounds the global shutdown drain, but there is no per-fiber execution timeout. A fiber that sleeps forever holds one of the `numWorkers` slots permanently. With 64 workers and 64 hung fibers, the runtime admits no new work. There is no watchdog, no `SpawnWithTimeout`, and no way to cancel a running fiber function from outside.

**Fix:** Add a `SpawnWithTimeout(fn, name, timeout)` that wraps the fiber function in a context-cancellable deadline. Document that the timeout signals intent; a blocked goroutine cannot be hard-killed without goroutine-level preemption.

---

#### API-7 · Major · Live `*Fiber` pointer with no ownership contract; exported fields unguarded
**File:** `internal/runtime/runtime.go:531-538`; `internal/fiber/fiber.go:56-92`

`GetFiberDirect()` returns the live `*Fiber` stored in `rt.fibers`. All 20+ exported fields on `Fiber` can be written directly (`f.State = StateDead`) without the fiber's `sync.RWMutex`, causing data races that are silent without the race detector. The comment says "callers must use Fiber methods" — this is unenforceable.

**Fix:** Return only clones from any public-facing method. Expose a narrow `FiberHandle` interface (read-only) instead of the live struct pointer.

---

#### API-8 · Major · `rt.currentFiber` is meaningless with N workers; misleads deadlock logic
**File:** `internal/runtime/runtime.go:455-459`

`dispatch()` acquires `rt.mu.Lock()` and sets `rt.currentFiber = f`. With `numWorkers > 1`, multiple goroutines dispatch concurrently and each overwrites the field. The "last writer wins" field describes no coherent state. It is a remnant of a single-threaded mental model.

**Fix:** Remove `rt.currentFiber` or replace with an `atomic.Pointer[fiber.Fiber]`. Update any caller or documentation that references it.

---

#### API-9 · Minor · No functional options / config struct for `NewRuntime`
**File:** `internal/runtime/runtime.go:87-121`

`NewRuntime` takes only `schedulerType` and `numWorkers`. Stack size, deadlock detector settings, and other configuration must be set post-construction via separate calls. There is no way to atomically set a coherent initial configuration.

**Fix:** Adopt `func NewRuntime(opts ...RuntimeOption) *Runtime` with options like `WithStackSize`, `WithDetectorConfig`, `WithNumWorkers`.

---

#### API-10 · Minor · No structured concurrency primitive — `SpawnGroup` / fan-out missing
**File:** N/A (missing capability)

There is no `SpawnGroup` or ergonomic "spawn N fibers, wait for all, collect errors" primitive. `FiberWaitGroup` requires manual fiber ID tracking and races against `complete()` reaping. The common fan-out pattern is impossible to implement correctly without a race between completion reaping and observer polling.

**Fix:** Add `SpawnGroup` that spawns N fibers and exposes a `Wait() []error` method.

---

#### API-11 · Minor · `FiberChannel.Send(v, nil)` silently succeeds when buffer is available
**File:** `internal/sync/channel.go:59-61`

A nil-fiber `Lock()` now panics (correct), but `FiberChannel.Send(v, nil)` succeeds silently when the channel buffer has space (no fiber is needed for a non-blocking send). This inconsistency means a nil fiber can successfully send on a buffered channel but will error on a blocking send. Callers cannot handle this uniformly; the panic/error split is undocumented.

**Fix:** Document the nil-fiber semantics explicitly per method. Alternatively, require a non-nil fiber for all sync operations and panic on nil.

---

#### API-12 · Minor · `FiberWaitGroup.Done()` returns `error` that deferred callers systematically ignore
**File:** `internal/sync/waitgroup.go:57-62`

`Done()` calls `Add(-1)` which returns an error on negative counter. The standard library `sync.WaitGroup.Done()` has no error return. Callers frequently write `defer wg.Done()` and discard the return. A deferred `Done()` on an already-zeroed group silently discards an error.

**Fix:** Either panic on double-Done (matching stdlib behaviour) or return no error and only panic on counter underflow.

---

#### API-13 · Minor · `Start()` has no parent context injection
**File:** `internal/runtime/runtime.go:269`

The execution loop's run context is always derived from `context.Background()`. There is no way for an embedder to cancel the runtime via an application-level context, making graceful shutdown ordering harder in structured application lifetimes.

**Fix:** Add `StartWithContext(ctx context.Context) error` that derives the run context from the caller-supplied context.

---

#### API-14 · Minor · `Reset()` is a silent no-op when called on a running runtime
**File:** `internal/runtime/runtime.go:379-404`

`Reset()` returns nothing. When called while the runtime is running, it silently exits with no error. Callers have no machine-readable signal that `Reset` was a no-op.

**Fix:** Change `Reset()` to `Reset() error` returning `ErrRuntimeRunning` when the runtime is not stopped.

---

#### API-15 · Nit · `FiberSemaphore(0).Release()` is unbounded — semantics undocumented
**File:** `internal/sync/waitgroup.go:285-292`

For `NewFiberSemaphore(0)`, the `maxPermits==0` branch bypasses the clamp. Every `Release()` without a queued waiter increments `permits` without bound. Three `Release()` calls before any `Acquire()` allows 3 subsequent non-blocking acquisitions. This is the "signal-before-wait" pattern but the N-signal/N-acquire behaviour is not documented.

**Fix:** Document the behaviour explicitly: "A zero-initial-permit semaphore permits N Acquire()s for each N Release() calls made before any waiter enqueues."

---

### Concurrency Correctness

---

#### CON-1 · Major · Dispatch goroutine drops result silently on `runCtx.Done()` — `complete()` never called
**File:** `internal/runtime/runtime.go:477-484`

```go
select {
case resultChan <- result:
case <-runCtx.Done():   // complete() never runs when this arm fires
}
```

The comment at line 477 claims "complete() still runs so CPUTime is recorded." This is **false**. When `runCtx.Done()` is selected, the goroutine exits without delivering the result. Consequences: `AddCPUTime()` is skipped, `RecordFiberCompleted()` is skipped, `ActiveFibers` stays inflated, `scheduler.MarkCompleted()` is skipped, and the fiber map entry leaks (RT-5).

**Fix:** Call `complete(result)` unconditionally before the select, then send the (already-processed) result or discard. Correct the false comment.

---

#### CON-2 · Major · Drain loop exits before all fiber goroutines have sent their result
**File:** `internal/runtime/runtime.go:417-427`

After `runCtx.Done()` fires, the execution loop drains `resultChan` until `default:` fires. However, fiber goroutines may still be mid-execution at that moment. They complete and send to `resultChan` (buffered, capacity 1024 — send succeeds) *after* the drain exits. No one reads these results. `complete()` never runs. `fiberWG.Done()` is still called so `Stop()` can return, but fiber accounting is silently lost.

**Fix:** Move `resultChan` drain to *after* `fiberWG.Wait()`, not concurrently with the shutdown signal. Ensure every send to `resultChan` is paired with a read.

---

#### CON-3 · Minor · `result.epoch` is dead code — stale-run guard unimplemented
**File:** `internal/runtime/runtime.go:74, 479`

`fiberResult.epoch` is set in the dispatch goroutine but never read in `complete()`. The intended guard against stale-run delivery is not enforced. Currently safe due to `lifecycleMu` serialization, but any future weakening of that invariant leaves the guard absent.

**Fix:** Either read `result.epoch` in `complete()` and discard results from prior epochs, or remove the field and document that `lifecycleMu` is the sole protection.

---

#### CON-4 · Minor · `rt.mainFiber` has three inconsistent lock regimes
**File:** `internal/runtime/runtime.go:261-267, 394-395, 503-505`; `internal/runtime/deadlock.go:96-100`

| Location | Locks held when accessing `mainFiber` |
|---|---|
| `Start()` write | `rt.mu.Lock()` AND `rt.mainFiberMu.Lock()` |
| `complete()` read | `rt.mu.Lock()` AND `rt.mainFiberMu.RLock()` |
| `Reset()` write | `rt.mu.Lock()` ONLY — no `mainFiberMu` |
| `checkForDeadlock()` read | `rt.mu.RLock()` ONLY — no `mainFiberMu` |

`Reset()` writes `mainFiber` without `mainFiberMu`, so `complete()` holding `mainFiberMu.RLock` does not exclude `Reset()`'s write. Currently safe by `lifecycleMu` discipline; not safe by lock protocol alone.

**Fix:** Either protect all `mainFiber` accesses with `mainFiberMu`, or remove `mainFiberMu` and use only `rt.mu`. Document whichever invariant is chosen.

---

#### CON-5 · Minor · GC reference leak in all slice pop/remove operations
**File:** `internal/sync/mutex.go:140`; `channel.go:69, 108, 178, 218`; `waitgroup.go:282`; `internal/scheduler/workstealing.go:108`

`queue = queue[1:]` and `append(queue[:i], queue[i+1:]...)` do not nil the vacated slot. The backing array retains a live pointer to the old waiter struct and its `*fiber.Fiber`. The GC cannot collect the fiber or its 64KB stack until the backing array is reallocated. Under sustained contention, this pins O(queue-capacity) × fiber-size bytes.

**Fix:** After every pop: `queue[0] = nil; queue = queue[1:]`. After every mid-remove: `queue[len(queue)-1] = nil; queue = queue[:len(queue)-1]`.

---

#### CON-6 · Minor · `scheduler.Stop()` does not guard subsequent `Schedule()`/`Next()` calls
**File:** `internal/scheduler/scheduler.go:97` (no checks in `Schedule`, `Next`)

`BaseScheduler.Stop()` sets `s.running = false` but `Schedule()` and `Next()` never check `s.running`. After `Stop()`, fibers can still be enqueued and dequeued silently. The runtime's admission path protects against this via the re-check pattern, but any external holder of a `Scheduler` interface reference gets silent incorrect behaviour after `Stop()`.

**Fix:** Add `if !s.running { return ErrSchedulerStopped }` at the top of `Schedule()` and `Next()`.

---

### Performance

---

#### PERF-1 · Major · `filterFinished` allocates a new slice on every `Next()` call
**File:** `internal/scheduler/fifo.go:48`; `roundrobin.go:87`; `workstealing.go:360`; `priority.go:231`

Every `Next()` call unconditionally allocates a backing slice even when zero fibers are finished. With a 1 ms ticker and 64 workers: up to 64,000 allocations/sec. The WorkStealing scheduler calls `filterFinished` up to 3× per `Next()`.

**Fix:** In-place compaction: shift live entries to the front, truncate the slice. When no fibers are finished (the common case), zero allocations.

---

#### PERF-2 · Major · `PriorityScheduler.MarkCompleted` is O(n) scan + `heap.Init(O(n))`
**File:** `internal/scheduler/priority.go:87-95`

On every fiber completion, `MarkCompleted` rebuilds the heap from scratch. `heap.Remove(h, i)` is O(log n) and the standard fix. At 1,000 concurrent fibers and 1,000 completions/sec: ~1,000,000 comparisons/sec from this path alone. `UpdatePriority`, `Reheapify`, and `Remove` have the same pattern.

**Fix:** Store the heap index in each `PriorityItem` (via an `index int` field updated by `heap.Push`/`heap.Pop`). Use `heap.Remove(h, item.index)` for O(log n) removal.

---

#### PERF-3 · Major · `PriorityQueue.Less` acquires 2–4 `fiber.mu` locks per comparison
**File:** `internal/scheduler/priority.go:200-207`

`PriorityValue()` and `CreatedAtValue()` both acquire `f.mu.RLock()`. `Less()` is called O(log n) times per `heap.Push`/`heap.Pop` and O(n) times per `heap.Init`. Combined with PERF-2: ~4,000,000 fiber-level mutex operations/sec at 1,000 fibers and 1,000 completions/sec.

**Fix:** Copy `Priority` and `CreatedAt` into the `PriorityItem` wrapper struct at enqueue time. `Less()` then reads local fields — no locks needed.

---

#### PERF-4 · Major · `dispatch()` takes `rt.mu.Lock()` just to assign `rt.currentFiber`
**File:** `internal/runtime/runtime.go:457-459`

A full write lock on `rt.mu` serializes all concurrent `dispatch()` calls and all concurrent `rt.mu.RLock()` callers (`Spawn`, `IsRunning`, `GetFiber`). With N workers firing in parallel, this is a global serialization point on every 1 ms tick.

**Fix:** Replace `rt.currentFiber` with `atomic.Pointer[fiber.Fiber]`. Remove the `rt.mu.Lock()` from `dispatch()`.

---

#### PERF-5 · Major · `GetAllFibers()` clones all live fibers every 100 ms for broadcast
**File:** `web/server.go:774-776`; `internal/runtime/runtime.go:543-550`

The broadcast loop calls `GetAllFibers()` which allocates one `*Fiber` clone per live fiber. At 1,000 fibers and 10 broadcasts/sec: 10,000 heap allocations/sec (~2.5 MB/sec churn). At 10,000 fibers: 100,000 allocs/sec, ~25 MB/sec. `sort.Slice` runs on every broadcast.

**Fix:** Maintain a dirty-bit per fiber; only re-clone and re-sort when state changed since last broadcast. Or switch to delta streaming (only send changed fibers).

---

#### PERF-6 · Minor · Metrics records `time.Now()` + mutex lock on every lifecycle event
**File:** `internal/metrics/metrics.go:56-202`

Every fiber event (created, completed, context switch, schedule call) calls `time.Now()` and takes `m.mu.Lock()`. At 1,000 fibers/sec: ~8,000 mutex lock/unlock pairs and ~8,000 `time.Now()` calls/sec from metrics alone. `LastUpdateTime = time.Now()` inside the lock serves only observability.

**Fix:** Switch hot-path counters to `sync/atomic` operations. Read timestamps lazily in `GetSnapshot()`.

---

#### PERF-7 · Minor · `EventTracker` backing array never freed — 560 KB permanently pinned
**File:** `internal/metrics/metrics.go:393-403`

The trim `et.events = et.events[len-max:]` advances the slice header without releasing the backing array. At `maxEvents=10000` and `sizeof(FiberEvent)=56` bytes: 560 KB of heap is pinned until `Clear()`. Under steady-state operation the backing array is never recollected.

**Fix:** Replace with a circular ring buffer. The backing array is then fixed-size and always fully utilised.

---

#### PERF-8 · Minor · WorkStealing `steal()` allocates `[]int` victim list on every steal attempt
**File:** `internal/scheduler/workstealing.go:160-161`

`steal()` calls `make([]int, 0, s.numWorkers-1)` on every invocation. At idle with 64 workers and 1,000 ticks/sec: ~63,000 allocations/sec just for victim lists.

**Fix:** Pre-allocate `[maxWorkers]int` on the scheduler struct and slice it per-call.

---

#### PERF-9 · Minor · `Stack.Push`/`Pop` holds `sync.Mutex` — single-owner, no concurrent access
**File:** `internal/fiber/stack.go:33-64`

The fiber `Stack` is owned exclusively by its fiber's goroutine. No other goroutine writes to it. Every `Push` and `Pop` acquires a write lock that has no contender.

**Fix:** Replace `s.mu sync.Mutex` with `atomic.Int64` for `sp`. Read-side callers (observability) use the atomic load.

---

#### PERF-10 · Minor · `fmt.Errorf` in idle `Next()` allocates on every empty-queue tick
**File:** `internal/scheduler/fifo.go:33`; `roundrobin.go:44`; `priority.go:55`; `workstealing.go:154`

`fmt.Errorf("no fibers in run queue")` allocates two objects (error interface + string) on every tick when the queue is empty. At 64 workers × 1,000 ticks/sec: up to 64,000 allocations/sec when idle.

**Fix:** `var ErrNoFibers = errors.New("no fibers in run queue")` — a singleton returned by value, zero allocations.

---

#### PERF-11 · Critical (memory) · Default 64 KB stack × fiber count = 640 MB at 10,000 fibers
**File:** `internal/fiber/stack.go`; `internal/fiber/fiber.go:104`

Each fiber allocates a `DefaultStackSize` (64 KB) byte slice as its simulated stack. At 10,000 concurrent fibers: 640 MB of stack heap alone, before fiber struct overhead. This ceiling is not documented in the README or `NewFiber` godoc.

**Fix:** Document `DefaultStackSize` and its scaling implications. Consider lazy allocation (allocate only on first `Push`). Add a `MaxTotalStackMemory` option that caps the total stack allocation.

---

### Security

---

#### SEC-1 · Medium · WebSocket token validated only at handshake — no revalidation on rotation
**File:** `web/server.go:863-878` · CWE-613

The auth token is read from `GREENTHREADS_AUTH_TOKEN` at startup and validated once per WebSocket handshake. If the token is rotated without restarting the process, all existing connections remain indefinitely authorized. No per-message or periodic re-authentication exists.

**Fix:** Add a configurable `TokenRevalidationInterval` that closes connections if their token no longer matches the current server token.

---

#### SEC-2 · Medium · No per-session authorization — all authenticated clients share the runtime
**File:** `web/server.go:488-499` · CWE-285

Any authenticated WebSocket client can `init` (replace the shared runtime), `stop` (halt work for all other clients), or `reset` (destroy all metrics and state). There are no per-client privilege levels.

**Fix:** Add a `ReadOnly` client mode that disables destructive commands. Document the shared-runtime threat model.

---

#### SEC-3 · Medium · Fiber name accepts null bytes and control characters
**File:** `web/server.go:562-568` · CWE-20

Fiber name validation checks only byte length (≤128) and whitespace trimming. Null bytes (`\x00`), control characters (`\x01`–`\x1f`), ANSI escape sequences (`\x1b[`), and Unicode surrogates are accepted. These propagate into event `Details` strings broadcast to all clients and into server log output.

**Fix:** Reject names containing codepoints below U+0020 (except horizontal tab) and above U+FFFD. Strip or reject ANSI escape sequences.

---

#### SEC-4 · Medium · `AllowedOrigins` cross-host entries are silently non-functional
**File:** `web/server.go:410-413` · CWE-183

The server validates `Origin` before calling `gorilla/websocket.Upgrader.Upgrade()`. Gorilla's default `checkSameOrigin` then re-validates and rejects cross-host origins. A deployment that sets `AllowedOrigins` to include `https://trusted.example.com` will find that cross-host connections are silently blocked by the gorilla layer — the feature appears configured but is inert.

**Fix:** Set `upgrader.CheckOrigin = func(r *http.Request) bool { return true }` so gorilla defers to the server's pre-validated check.

---

#### SEC-5 · Medium · TOCTOU between fiber cap check and `Spawn`
**File:** `web/server.go:558-559` · CWE-362

`GetMetrics().ActiveFibers >= MaxFibersPerRuntime` is checked before `rt.Spawn()`. Between the check and the spawn, another concurrent WebSocket client can also pass the check. With 64 clients, `ActiveFibers` can overshoot `MaxFibersPerRuntime` by up to 64.

**Fix:** Enforce the cap atomically inside `rt.Spawn()` using a compare-and-swap on the active fiber counter.

---

#### SEC-6 · Low · Auth token in URL by default — leaked in server logs and browser history
**File:** `web/server.go:68, 81, 872-873` · CWE-312

`AllowTokenInQuery` defaults to `true`. The token appears in the WebSocket upgrade URL (`?token=<value>`), which is captured by HTTP access logs, browser history, and `Referer` headers. Any default deployment with `GREENTHREADS_AUTH_TOKEN` set leaks the token in URLs.

**Fix:** Change the default to `AllowTokenInQuery: false`. Require explicit opt-in in the configuration and document the risk.

---

#### SEC-7 · Low · Host header injection can bypass origin whitelist in same-host fallback
**File:** `web/server.go:898` · CWE-346

The `allowedOrigin()` same-host fallback compares `u.Host` to `r.Host`. If the server does not validate the `Host` header independently, an attacker can supply `Host: attacker.example.com` and `Origin: https://attacker.example.com`, making the comparison pass for an arbitrary origin.

**Fix:** Validate `r.Host` against the configured server address before using it in origin comparisons.

---

#### SEC-8 · Low · No minimum token entropy — single-character tokens accepted
**File:** `web/server.go:73` · CWE-521

`GREENTHREADS_AUTH_TOKEN` is accepted as-is. A token of `"1"` is accepted and the server starts normally.

**Fix:** At startup, validate that the token is at least 32 characters (≈128 bits entropy for alphanumeric). Exit with a clear error if the token is too short.

---

#### SEC-9 · Low · No TLS; proxy assumption undocumented
**File:** `web/server.go:240`; `Dockerfile:15`

The server binds `0.0.0.0:8080` with plaintext HTTP by default. The auth token and all WebSocket messages (fiber names, state, timestamps) are transmitted in cleartext. No README or RUNBOOK entry states that TLS termination at a proxy is required.

**Fix:** Document the TLS requirement prominently in the README and RUNBOOK. Add a `-tls-cert`/`-tls-key` flag with `ListenAndServeTLS`.

---

### Testing Quality

---

#### TEST-1 · Critical · No integration test using sync primitives through the live runtime
**File:** All `internal/sync/*_test.go`

Every sync primitive test exercises the primitive with raw `fiber.NewFiber` objects outside any runtime. No test spawns fibers via `Runtime.Spawn()`, has those fibers call `FiberMutex.Lock`/`FiberChannel.Send`/`FiberWaitGroup.Wait` on each other, and verifies the runtime dispatch loop correctly unblocks them. The SY-2 hard-deadlock scenario has never been exercised end-to-end.

**Fix:** Add a test package `integration_test` that starts a real runtime, spawns producer and consumer fibers using actual sync primitives, and asserts correct ordering and termination.

---

#### TEST-2 · Major · `time.Sleep` used as synchronization barrier — tests are flaky on loaded CI
**File:** `internal/runtime/runtime_test.go:116` (`time.Sleep(500ms)`); `internal/sync/mutex_test.go:49` (`time.Sleep(10ms)`)

`runtime_test.go:116` waits 500 ms for fibers to complete. On a loaded CI runner this can fail. `mutex_test.go:49` waits 10 ms for a goroutine to block on a mutex — this is the most fragile: it assumes scheduler latency is under 10 ms.

**Fix:** Replace `time.Sleep` barriers with polling loops under a `require.Eventually` or with channel-based synchronization (`waitForState`, `waitBlocked` helpers already available in `toctou_test.go`).

---

#### TEST-3 · Major · No regression test for concurrent Spawn+Stop under Priority or WorkStealing
**File:** `internal/runtime/regression_test.go:175`

`TestConcurrentStartStop` only uses `TypeFIFO`. The Priority heap uses `heap.Push` under concurrent Schedule+Stop; the WorkStealing per-worker locks use a different nesting. Neither combination has been race-tested.

**Fix:** Parameterise `TestConcurrentStartStop` over all four scheduler types with `t.Run`.

---

#### TEST-4 · Major · Fuzz: no concurrent `FiberMutex.Lock`/`Unlock` fuzz target
**File:** `internal/sync/sync_fuzz_test.go:33`

`FuzzFiberMutexLockUnlock` runs sequentially (single fiber, N lock/unlock cycles). No fuzz target exercises concurrent lock acquisition by multiple goroutines, which is where handoff races occur.

**Fix:** Add `FuzzFiberMutexConcurrent` that spawns M goroutines each racing to lock/unlock, with fuzzed M, N, and hold durations.

---

#### TEST-5 · Major · Fuzz: no concurrent `FiberChannel.Send`/`Receive`/`Close` fuzz target
**File:** `internal/sync/sync_fuzz_test.go:1-30`

`FuzzFiberChannelSendReceive` skips cap≤0 channels and runs single-threaded. No fuzz target exercises concurrent Send+Close or Receive+Close which are the races where channel invariants can break.

**Fix:** Add `FuzzFiberChannelConcurrent` that starts concurrent senders and receivers with fuzzed capacity, close timing, and sender count.

---

#### TEST-6 · Minor · Table-driven tests missing `t.Run` — failure identification is impossible
**File:** `web/stress_load_test.go:334`; `internal/runtime/runtime_test.go:249`

`TestStressBoundaryPayloads` iterates a cases slice without `t.Run`. `TestRuntimeDifferentSchedulers` iterates all 4 scheduler types without `t.Run`. A failure in one case stops the whole test with no indication of which case or scheduler type failed.

**Fix:** Wrap each iteration with `t.Run(caseName, func(t *testing.T) {...})`.

---

#### TEST-7 · Minor · Tests access unexported struct fields — coupling to implementation
**File:** `web/server_test.go:131-159`; `internal/sync/rwmutex_regression_test.go:67-74`

`server_test.go:131` directly instantiates `&Server{logger: slog.Default()}` and accesses `newClient(nil)` internals. `rwmutex_regression_test.go:67` accesses `frw.mu.Lock()` and reads `frw.readers` — an unexported field — which breaks if the internal layout changes.

**Fix:** Rewrite as black-box behavioural tests via the public API.

---

#### TEST-8 · Minor · No benchmark for `FiberMutex` contention under N concurrent fibers
**File:** `internal/sync/mutex_test.go` (benchmarks section)

The existing `BenchmarkMutexLockUnlock` uses a single fiber acquiring/releasing sequentially. No benchmark measures the contended path where N goroutines race for the same mutex — the only path where the waiter queue and channel-wake mechanism are exercised.

**Fix:** Add `BenchmarkFiberMutexContended(N)` that runs N goroutines concurrently acquiring the same `FiberMutex`.

---

#### TEST-9 · Minor · No benchmark for deadlock detector scan at scale
**File:** `internal/runtime/deadlock.go`

`checkForDeadlock` calls `rt.GetAllFibers()` (map copy + clone + sort) on every tick. No benchmark measures this scan cost at 100, 1,000, or 10,000 fibers. At 10,000 fibers and a 1 s interval, the scan cost determines whether the detector itself becomes a bottleneck.

**Fix:** Add `BenchmarkDeadlockDetectorScan(N)` in `runtime_test.go`.

---

#### TEST-10 · Nit · Web stress tests missing `t.Parallel()`
**File:** `web/e2e_test.go:22, 149`; `web/stress_load_test.go:278, 334, 412, 461`

These tests each bind a random loopback port via `randomAddr(t)` and can safely run in parallel. Without `t.Parallel()` the web test suite serialises unnecessarily, adding minutes to CI.

**Fix:** Add `t.Parallel()` as the first line of each test function.

---

### Observability & Operations

---

#### OBS-1 · Critical · Fiber panics never logged — silent failures in production
**File:** `internal/fiber/fiber.go:132-142`; `internal/runtime/runtime.go`

`fiber.Run()` recovers all panics and stores them as `f.failure`. Nothing logs the panic. A user fiber function that panics is counted as a normal completion. An operator has zero log signal — no ERROR line, no stack trace in server logs. The panic is only observable by polling `GetFiber(id).Failure()` via the WebSocket API before the fiber is reaped.

**Fix:** In `complete()`, after receiving the result, log at ERROR level if `result.fiber.Failure() != nil`, including the fiber ID, name, and `PanicStack()`.

---

#### OBS-2 · Major · `metrics.Reset()` decrements Prometheus counters — violates monotonicity invariant
**File:** `internal/metrics/metrics.go:261`

`Reset()` zeroes all counters including `TotalFibersCreated` and `TotalFibersCompleted`. A Prometheus server that scraped before the reset will compute a negative `increase()` rate and generate false alerts. Prometheus counters must never decrease between scrapes.

**Fix:** Use a generation-offset approach: maintain a `resetOffset` that is added to counters at scrape time, so the exported value appears monotonically increasing across resets. Or expose reset state as a separate `greenthreads_runtime_resets_total` counter and document that histogram/counter snapshots restart at reset.

---

#### OBS-3 · Major · No fiber execution latency histogram — SLOs on p95/p99 are impossible
**File:** `web/server.go:397-408` (metrics handler)

The `/metrics` endpoint has no histogram for fiber run time. `AverageRunTime` in `MetricsSnapshot` is computed as a rolling value but is not exported. An operator cannot alert on "p99 fiber execution time > 5 s" or set an SLO — averages mask the long tail.

**Fix:** Add `greenthreads_fiber_run_seconds` as a Prometheus histogram with buckets at 1ms, 10ms, 100ms, 1s, 10s, 60s. Record in `complete()`.

---

#### OBS-4 · Major · No Go runtime metrics at `/metrics` — memory leaks invisible
**File:** `web/server.go:371-408`

`/metrics` emits no `go_goroutines`, `go_gc_duration_seconds`, `go_memstats_alloc_bytes`, or `process_resident_memory_bytes`. An operator cannot distinguish a fiber stack memory leak (PERF-11 / RT-5) from normal operation by inspecting metrics alone.

**Fix:** Add standard Go runtime metrics using `runtime.ReadMemStats` and `runtime.NumGoroutine`, exposed as `go_memstats_alloc_bytes`, `go_goroutines`, `go_gc_pause_ns`, etc.

---

#### OBS-5 · Major · Fiber panics, failed spawns, and deadlock events have no Prometheus metrics
**File:** `web/server.go:371-408`

The following have no counter at `/metrics`:
- Fiber panics (`TotalFiberPanics`) — **RESOLVED:** `greenthreads_fiber_panics_total` is now emitted and incremented from `complete()`.
- Failed spawn attempts (`SpawnErrors`) — RESOLVED: `greenthreads_spawn_errors_total` is emitted.
- Dropped WebSocket broadcast messages — RESOLVED: `greenthreads_broadcast_messages_dropped_total` is emitted.
- Deadlock detector events — **RESOLVED:** `greenthreads_deadlocks_total` is now emitted, incremented once per detected episode (rising edge) via `RecordDeadlockDetected`.

**Fix:** Add counters for each. Increment them at the relevant code sites.

---

#### OBS-6 · Minor · Log level hardcoded to `INFO` — no runtime debug path in production
**File:** `cmd/server/main.go:38`

The log level is `slog.LevelInfo` with no override. An operator cannot enable `DEBUG`-level logging (dispatch, context switches, sync events) without rebuilding the binary.

**Fix:** Read `LOG_LEVEL` from the environment at startup (`slog.LevelDebug` if `LOG_LEVEL=debug`, etc.). Default to `INFO`.

---

#### OBS-7 · Minor · WebSocket client disconnects are not logged
**File:** `web/server.go:455-458`

When `conn.ReadMessage()` returns an error (client disconnects, network failure), the handler returns silently. There is no `s.logger.Info("client disconnected", ...)` or similar. Operators have no log signal when clients drop.

**Fix:** Log at `INFO` level on normal close (`websocket.IsCloseError`), `WARN` on unexpected disconnect, before returning from `handleWebSocket`.

---

#### OBS-8 · Minor · No env-var overrides for operational limits — requires code changes to tune
**File:** `web/server.go:47-83` (`defaultConfig`)

`MaxClients` (64), `MaxFibersPerRuntime` (10000), `StopTimeout` (5s), `MessagesPerSecond` (30), `PingInterval` (30s) are all hardcoded in `defaultConfig()` with no env-var or flag overrides. Operators cannot tune these without recompiling.

**Fix:** Read each setting from environment variables (`GREENTHREADS_MAX_CLIENTS`, etc.) with documented defaults.

---

#### OBS-9 · Minor · HTTP shutdown sequence is reversed — WebSocket closed before HTTP drained
**File:** `web/server.go:271-290`

`closeClients()` (which hard-closes all WebSocket connections) is called before `httpServer.Shutdown(ctx)`. Any in-flight HTTP request (metrics scrape during shutdown, health probe) is cut off. The standard order is: stop accepting new connections → drain in-flight HTTP → close WebSocket.

**Fix:** Reorder: `httpServer.Shutdown(ctx)` first (which stops `Accept` and drains in-flight HTTP handlers), then `closeClients()`.

---

#### OBS-10 · Minor · No Kubernetes manifests, no resource limit guidance
**File:** `Dockerfile`; `RUNBOOK.md`

No Deployment YAML, Service, HPA, or PodDisruptionBudget manifest is provided. Container memory limits are undocumented. With 10,000 fibers × 64 KB stack = 640 MB stack memory plus goroutine overhead, a default cluster memory limit (e.g., 256 MiB) will OOM-kill the container with no warning.

**Fix:** Add a `deploy/k8s/` directory with example manifests. Document minimum memory requests: `256Mi` base + `64Ki × MaxFibersPerRuntime`.

---

#### OBS-11 · Minor · No staging/production promotion gate — every merge to `main` auto-tags
**File:** `.github/workflows/release.yml`

`release.yml` auto-increments a patch version tag on every push to `main`. There is no human approval gate, no staging environment test, and no integration test against a live server before tagging. Every merge is silently "released."

**Fix:** Add a `workflow_dispatch` trigger (manual release) or a protected branch promotion gate. Run an integration test (start server, connect client, spawn fibers) as a required step before tagging.

---

#### OBS-12 · Minor · No pprof endpoint — profiling impossible in production
**File:** `cmd/server/main.go`

The previous dead pprof import was removed (WO-3, resolved). There is now no profiling escape hatch at all. Memory leak diagnosis requires attaching a debugger or rebuilding with pprof.

**Fix:** Add an optional `-pprof-addr` flag that starts a separate `net/http/pprof` listener on a localhost-only port. Document in RUNBOOK.

---

#### OBS-13 · Minor · RUNBOOK missing critical operational scenarios
**File:** `RUNBOOK.md`

The following scenarios are absent:
- **Graceful rolling restart** — the 10-second drain deadline is not mentioned; operators don't know what happens to in-flight fibers during rolling updates.
- **Fiber appears stuck / SY-2 hard-deadlock** — no guidance on identifying all-blocked states or using the deadlock detector history.
- **Fiber panic loop** — no guidance on detecting silently-failing fibers (connect to OBS-1).
- **High memory / RSS growth** — no mention of RT-5 fiber-map leak or PERF-11 stack memory scaling.
- **Alert thresholds** — no example Prometheus alert rules for any condition.
- **Clients not receiving updates** — no guidance for dropped broadcast messages.

**Fix:** Add a section for each scenario with diagnostic commands and recommended remediation.

---

### Documentation & Error Handling

---

#### DOC-1 · Major · `runtime.go:477` comment falsely claims `complete()` always runs
**File:** `internal/runtime/runtime.go:477`

The comment "complete() still runs so the fiber's CPUTime is recorded and any finalization paths run" is false. When `runCtx.Done()` fires in the `select`, the goroutine exits without calling `complete()`. This false invariant masks the impact of CON-1 and RT-5.

**Fix:** Change to: "If runCtx.Done() fires before the result is delivered, complete() does not run. CPUTime, metrics, and map cleanup are lost for this fiber; they will be recovered at the next Reset()."

---

#### DOC-2 · Major · `RoundRobinScheduler` struct comment says "rotated to the back of the queue"
**File:** `internal/scheduler/roundrobin.go:11`

"Each fiber gets a time quantum and is rotated to the back of the queue." Both claims are false: fibers are not re-enqueued after dispatch, and the quantum is never used by the runtime to trigger rotation. See also API-1.

**Fix:** Correct to: "RoundRobinScheduler is a FIFO scheduler with a configurable quantum field. ShouldPreempt() is available for callers implementing cooperative yield loops, but the runtime does not call it automatically."

---

#### DOC-3 · Major · README does not document SY-2 (blocking fibers hard-deadlock)
**File:** `README.md`

The most dangerous user-visible property — `numWorkers` fibers blocking on sync primitives whose unblocking fibers haven't been dispatched = permanent hard deadlock — is not mentioned in the README, package godoc, or any example. A user who naively uses `FiberMutex` or `FiberChannel` without understanding the slot limit will hit an undebuggable hang.

**Fix:** Add a prominent "Limitations" section documenting: "Blocking N fibers on a synchronization primitive with fewer than N+1 available worker slots produces an unrecoverable deadlock. Always ensure the runtime has more worker slots than the maximum number of simultaneously blocked fibers."

---

#### DOC-4 · Major · README does not document `*fiber.Fiber` requirement for sync primitives
**File:** `README.md`

`FiberMutex.Lock(currentFiber)`, `FiberChannel.Send(v, currentFiber)`, and all blocking sync calls require a live `*fiber.Fiber` pointer from `GetFiberDirect()`. This is the library's most surprising ergonomic constraint and is not documented anywhere in the README.

**Fix:** Add a "Sync Primitives" section in README explaining the fiber pointer requirement, how to obtain it, and that passing `nil` panics or returns an error depending on the method.

---

#### DOC-5 · Minor · No sentinel errors for runtime lifecycle states
**File:** `internal/runtime/runtime.go:170, 185, 237, 245`

`"nil runtime"`, `"runtime not started"`, `"runtime already running"`, `"runtime stopped during spawn"` are all inline `fmt.Errorf` strings. Callers cannot use `errors.Is` or `errors.As` to distinguish lifecycle errors from other errors. `ErrChannelClosed` exists in the sync package (good) but the runtime package has no equivalents.

**Fix:** Add `var (ErrNotStarted = errors.New(...); ErrAlreadyRunning = errors.New(...); ErrStoppedDuringSpawn = errors.New(...))` and use them in `fmt.Errorf("...: %w", ErrNotStarted)`.

---

#### DOC-6 · Minor · `FiberMutex.TryLock(nil)` returns `false` silently; `Lock(nil)` panics
**File:** `internal/sync/mutex.go:108`

`Lock(nil fiber)` panics (correct programmer-error signal). `TryLock(nil fiber)` returns `false` silently, which is indistinguishable from "lock is contended." A caller checking `TryLock` return value cannot distinguish "nil fiber" from "lock held by another fiber." This inconsistency is undocumented.

**Fix:** Document: "`TryLock` returns `false` for a nil fiber; this is distinct from a contended lock and does not mean the lock was acquired on any path." Or panic on nil for consistency with `Lock`.

---

#### DOC-7 · Minor · Panic error uses `%v` not `%w` — root cause not unwrappable
**File:** `internal/fiber/fiber.go:135`

`fmt.Errorf("fiber %d (%s) panicked: %v", f.ID, f.Name, recovered)` uses `%v`. If `recovered` is an `error`, `errors.Is`/`errors.As` cannot unwrap through `Failure()`. Callers who want to check the underlying error type must string-parse `f.Failure().Error()`.

**Fix:** Type-assert `recovered` to `error` and use `%w` if it is one; otherwise fall back to `fmt.Errorf("... panicked with non-error value: %v", ...)`.

---

#### DOC-8 · Minor · `AverageWaitTime` / `AverageRunTime` in `SchedulerStats` are always zero
**File:** `internal/scheduler/scheduler.go:97-98`; `internal/metrics/metrics.go:26`

These fields appear in `SchedulerStats` and `MetricsSnapshot`. Neither is ever assigned a non-zero value. `GetStats()` returns zero for both. Any consumer reading these fields from the WebSocket state update will see permanent zeroes with no explanation.

**Fix:** Either implement them or add godoc: "// AverageWaitTime is reserved for future implementation and is always zero."

---

#### DOC-9 · Minor · `examples/simple/main.go` uses `time.Sleep(2s)` as the join mechanism
**File:** `examples/simple/main.go:53`

The example waits 2 seconds then prints results. This is the textbook unreliable join pattern — the documented wrong idiom. The library provides `FiberWaitGroup` precisely for this purpose. The example teaches users the wrong pattern.

**Fix:** Rewrite to use `FiberWaitGroup` or a channel to detect completion. Add a demonstration of sync primitives, error checking, and `GetFiber` usage.

---

#### DOC-10 · Minor · `go.mod` requires `go 1.26.5` — only `go 1.21` is needed
**File:** `go.mod`

The code uses `log/slog` (introduced in Go 1.21) and no features requiring 1.22+. Declaring `go 1.26.5` as the minimum prevents use on any toolchain before 1.26.5, which is overly restrictive for a library. The `toolchain go1.26.5` directive mentioned in AUDIT.md is not present in the actual file.

**Fix:** Lower to `go 1.21` in `go.mod`. If 1.26.5 features are genuinely required, document which ones.

---

#### DOC-11 · Nit · No CHANGELOG
**File:** repository root

One annotated tag (`v0.1.0`) exists. Users have no way to understand what changed between versions other than reading `git log`. Standard practice for a released library.

**Fix:** Add `CHANGELOG.md` with a `[v0.1.0] - 2026-07-xx` entry listing the fixed issues from AUDIT.md and this audit.

---

#### DOC-12 · Nit · Execution model mislabeled as "green threads" / "M:N threading"
**File:** `README.md`; package godoc

The library spawns one goroutine per dispatched fiber, bounded by `numWorkers`. This is a goroutine pool with a priority queue front-end, not stackful coroutines or M:N green threads. A fiber that calls `time.Sleep` holds its OS-thread-backed goroutine for the full duration. Users expecting stackful context switching (like Lua coroutines, Python greenlets, or Go goroutines themselves) will be surprised.

**Fix:** Add to the README intro: "greenthreads is a bounded goroutine pool with priority scheduling, cooperative blocking, and a shared-memory observation plane. It is not stackful: each 'fiber' is a real Go goroutine, and no user-level context switching occurs between fibers."

---

## Resolved Issues

### Resolved: July 2026 First Audit (16 of 20 issues)

The following issues from the original 20-issue audit were confirmed fixed in commit `251c7d4` ("Production hardening: audit closure, 85% coverage, fuzz, benchmarks, 32-bit safety") and subsequent commits. Verified by code review of the actual file contents.

| ID | Description | Fix |
|---|---|---|
| RT-1 | Deadlock false positive: running fibers invisible to progress scan | `checkForDeadlock` now counts `StateRunning` as progress |
| RT-2 | Deadlock debounce resets only on runnable fibers | `dd.lastProgress` now refreshed unconditionally when `!noRunnable` |
| RT-3 | Spawn rollback silently inflates metrics | `RecordFiberCreated` moved after successful re-check; rollback skips it |
| FB-1 | `Block()` resurrects finished fibers | `if f.completion { return }` guard added to `Block()` |
| FB-2 | Unblocked fibers report `StateReady` while executing | `Unblock()` restores `StateRunning` when `runStarted` is set |
| SC-1 | `Remove()` broken for Priority and WorkStealing schedulers | `Remove()` overrides added to both subschedulers |
| SC-2 | `completed` map grows without bound | Bounded pruning at 4096 entries with random eviction to 2048 |
| SC-5 | `SchedulerStats.CurrentRunQueue` always 0 for Priority/WS | `GetStats()` overrides added to both subschedulers |
| SY-1 | `Lock(nil)` silently no-ops — mutual exclusion lost | `Lock(nil fiber)` now panics; `LockCtx(nil)` returns error |
| SY-3 | `FiberSemaphore(0)` lost-signal trap | `Release()` increments `permits` when `maxPermits==0` |
| SY-4 | `TrySend` conflates channel-closed with channel-full | `TrySend` now returns `(bool, error)`; `ErrChannelClosed` on closed channel |
| WO-1 | `release.yml` uses mutable `@v4` with `contents:write` | `actions/checkout` pinned to full 40-char SHA in `release.yml` |
| WO-2 | `/metrics` uptime is ~55 years after a reset | `GetSnapshot()` returns 0 when `StartTime.IsZero()` |
| WO-3 | Dead pprof import with misleading comment | Blank import and comment removed |
| WO-4 | CI coverage gate (45%) far below actual coverage | Gate raised to 80% in `ci.yml` and `Makefile` |
| WO-5 | CSP `connect-src` permits WebSocket to any host | Narrowed to `'self'` |

### Resolved: July 2026 Correctness Follow-up

A second scrutiny pass verified that several entries above the line were already
fixed in the code but never moved out of "Open", and closed four remaining
correctness bugs. All verified by code review and regression tests.

| ID | Description | Fix / evidence |
|---|---|---|
| RT-4 | Spawn/Stop race: admission gated on Stop *completed* | Already fixed — re-check uses `runCtx.Done()` (`runtime.go` Spawn re-check), gating on Stop *initiated* |
| RT-5 | Fiber-map leak + un-dispatched fibers survive Stop | Fixed — `Stop` now calls `reapPending`: clears the scheduler and reaps Ready (un-dispatched) fibers; `complete()` runs unconditionally in the dispatch goroutine so in-flight fibers no longer leak. Regression: `TestStopDoesNotRunUndispatchedFibersInNextRun` |
| RT-6 | Detector config discarded on Start | Already fixed — `Start` copies `enabled`/`checkInterval`/`timeout` from the prior detector |
| RT-7 | `WithMaxFibers` permanently exhausted across Stop/Reset (activeFiberCount leak) | Fixed — `reapPending` decrements `activeFiberCount` and the active-fiber metric for each reaped fiber via `metrics.RecordFiberCancelled`. Regression: `TestUndispatchedFibersDoNotLeakFiberCap` |
| SC-4 | Priority `AgeAll` dead code / starvation | Already fixed — `ageAllLocked()` runs inside `PriorityScheduler.Next()` every 100 pops |
| SC-6 | WorkStealing global queue write-dead | Already fixed — no `globalQueue` field remains; all fibers live in worker-local queues |
| SC-7 | `nextWorker` int64 wraparound | Already fixed — `atomic.AddUint64` with `% uint64(numWorkers)` |
| SC-8 | WorkStealing `blockedQueue` guarded by two different mutexes (data race) | Fixed — WS owns a `blocked` slice guarded solely by `globalMu`, with overridden `GetBlockedQueue`/`Clear`; base `blockedQueue` no longer used by WS |
| OBS-5 (panics) | `greenthreads_fiber_panics_total` documented but never emitted | Fixed — `complete()` calls `RecordFiberPanic`; `/metrics` emits the counter. Regression: `TestFiberPanicIncrementsPanicMetric` |
| SY-2 (detection) | Detector blind to the slot-exhaustion deadlock it exists for | Fixed — `checkForDeadlock` separates Ready/Running and flags when all worker slots are held by blocked fibers. Regression: `TestDetectorFlagsSlotExhaustionDeadlock` (the underlying non-preemptive *design* limitation remains — see SY-2 above) |

**Resolved — metrics-gauge drift:** the `ActiveFibers` gauge previously could
drift by a bounded amount in the fast-loop-during-Spawn race. `GetMetrics`/
`GetLifetimeMetrics` now source `ActiveFibers` directly from the exact
`activeFiberCount` (verified decremented exactly once per fiber across four
review rounds), so the gauge is always exact. (`TestActiveFibersGaugeMatchesExactCount`.)

### Resolved: Original AUDIT.md Issues (26 issues)

All 26 issues from the first-pass AUDIT.md are resolved. See `AUDIT.md` for their descriptions and delivery evidence. Key closures relevant to the current open issues:
- Gorilla WebSocket `CheckOrigin` correctly implemented.
- Auth token constant-time compare in place (`crypto/subtle`).
- WebSocket `ReadHeaderTimeout`/`WriteTimeout` set.
- `/metrics` auth-wrapped on non-loopback binds.
- All CI actions SHA-pinned in `ci.yml`.
- `MarkCompleted` idempotence correct.
- Fiber reaping from `rt.fibers` in `complete()` for the normal path (shutdown drain path is RT-5, still open).
