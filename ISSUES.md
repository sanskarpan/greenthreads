# Issues Registry

This file is the authoritative list of open, confirmed, and resolved defects
across the greenthreads library. Findings come from three sources: the original
author audit (AUDIT.md, 26 issues now closed), a July 2026 deep-dive by four
parallel reviewers (runtime/fiber, scheduler, sync, web/ops), and live
reproduction where noted.

Severities: **Critical** (data loss / exploitable) → **Major** (confirmed bug,
wrong behaviour in production) → **Minor** (limited blast radius, edge case) →
**Nit** (documentation / style).

---

## Open Issues

### Runtime & Deadlock Detector

---

#### RT-1 · Major · Deadlock false positive: running fibers invisible to progress scan
**File:** `internal/runtime/deadlock.go:104-131`  
**Confirmed:** Yes — reproduced empirically.

`checkForDeadlock` only counts fibers in `StateReady` as making progress. A
fiber in `StateRunning` is counted as neither runnable nor blocked. Scenario:
fiber A is executing (StateRunning) and will eventually unblock fiber B
(StateBlocked). The scan sees `runnable=0`, `blocked=1` → `noRunnable=true`.
After `timeout` elapses the detector records a false deadlock.

This is the common steady-state during dispatch: a fiber is observed in
`StateRunning` for nearly the entire duration of its execution; `StateReady` is
a transient state that lasts only until the execution loop ticks (1 ms).

**Fix:** Count `StateRunning` fibers as progress (treat as `runnable > 0` for
`lastProgress` refresh), or separately track running count in the scan.

---

#### RT-2 · Major · Deadlock debounce resets only on runnable fibers, not on idle runtime
**File:** `internal/runtime/deadlock.go:120-131`  
**Confirmed:** Yes — code-verified.

`dd.lastProgress` is only refreshed when `runnable > 0`. When the runtime is
idle (only the excluded main fiber, no user fibers), `noRunnable=false` so the
else branch that resolves the alert runs, but `lastProgress` is **not** updated.
If the runtime is idle for longer than `dd.timeout` (5 s) and then a new fiber
blocks immediately, the first tick after the block sees `time.Since(lastProgress)
>= timeout` and fires a false deadlock with zero debounce.

**Fix:** Also refresh `dd.lastProgress` in the branch where `!noRunnable` but no
fibers are present (full idle), or refresh unconditionally at the start of each
successful check when `!deadlockActive`.

---

#### RT-3 · Major · Spawn rollback silently inflates metrics forever
**File:** `internal/runtime/runtime.go:192-232`  
**Confirmed:** Yes — reproduced empirically.

`RecordFiberCreated` is called before `scheduler.Schedule`. Two rollback paths
exist (Schedule returns error; post-schedule stopped re-check). Both remove the
fiber from `rt.fibers` and return an error, but **neither** calls
`RecordFiberCompleted` or otherwise undoes the metrics. After a rolled-back
spawn: `ActiveFibers` and `TotalStackMemory` are permanently inflated by one
fiber's worth of state, and `TotalFibersCreated` is over-counted. The gauges
drift for the lifetime of the process (or until `Reset`).

**Fix:** Either call an explicit `RecordFiberCreationRollback` on rollback paths,
or move `RecordFiberCreated` to after the fiber is confirmed to be live (after
the re-check, just before returning the ID).

---

#### RT-4 · Major · Spawn/Stop race window not eliminated; code comment overclaims
**File:** `internal/runtime/runtime.go:163-167, 223-232`

The comment at line 163 states "This eliminates the Spawn/Stop race where Spawn
returned success against a stopped runtime." There is no CAS; the admission
check is: read `isStopped()` → call `Schedule()` → re-read `isStopped()`. The
`stopped` channel is closed at the **end** of `Stop` (after cancel, detector
stop, scheduler stop, and both WaitGroup waits). A Spawn whose final re-check
runs after Stop has begun but before `stopped` is closed returns a valid
`FiberID` for a fiber whose dispatcher goroutine has already exited. The fiber
never executes; any join waits forever.

**Fix:** Test `runCtx.Done()` in the re-check rather than `isStopped()`, so
admission is gated on "Stop initiated" rather than "Stop completed". Correct the
comment.

---

#### RT-5 · Major · Fiber-map leak: unreaped fibers accumulate across Stop
**File:** `internal/runtime/runtime.go:421-432, 483-487, 517-519`

`complete()` is the only place fibers are deleted from `rt.fibers`. On Stop:

- In-flight fibers that finish after the execution loop exits drop their result
  via the `runCtx.Done()` select arm — `complete()` never runs, so the fiber
  stays in `rt.fibers` with its full stack allocation.
- Fibers admitted to the scheduler but never dispatched at Stop time also
  persist (`scheduler.Clear` is only called from `Reset`, not `Stop`).

Across repeated Start/Stop cycles without `Reset`, `rt.fibers` grows
monotonically. With large queue depths this is an unbounded memory leak. Stale
entries also appear in `GetAllFibers()` snapshots of subsequent runs.

**Fix:** Drain and reap `rt.fibers` in `Stop` after `fiberWG.Wait` returns or
after the context deadline, calling `rt.scheduler.Clear()` and deleting stale
map entries.

---

#### RT-6 · Minor · Deadlock detector configuration discarded on every Start
**File:** `internal/runtime/runtime.go:275, 284`; `internal/runtime/deadlock.go:191-234`

`Start` always creates a fresh `DeadlockDetector` with hardcoded defaults
(`enabled=true`, 1 s interval, 5 s timeout). Any prior call to
`SetEnabled(false)`, `SetTimeout`, or `SetCheckInterval` is silently reverted.
Since `SetCheckInterval` is read once at `Start` of the detector, calling it
after `Start` is also a no-op.

**Fix:** Accept detector configuration as a `RuntimeOption` on `NewRuntime`, or
copy the prior detector's settings into the new one at `Start` time. Document
the per-Start lifecycle clearly.

---

### Fiber

---

#### FB-1 · Minor · `Fiber.Block()` resurrects finished fibers
**File:** `internal/fiber/fiber.go:171-177`

`Block()` unconditionally sets `State = StateBlocked`. Calling it on a fiber
that has already reached `StateFinished` (a timing window in any sync primitive
between fiber completion and the sync path observing the result) flips the state
backwards: `StateFinished → StateBlocked`. The fiber is now counted as blocked
by the deadlock detector and is no longer `IsFinished()`, until the runtime
reaps it.

`Unblock()` and `MarkScheduled()` both guard against this via `f.completion`,
but `Block()` does not.

**Fix:** Add `if f.completion { return }` at the top of `Block()`, mirroring the
guard already present in `Unblock()`.

---

#### FB-2 · Minor · Unblocked fibers report `StateReady` while their goroutine is executing
**File:** `internal/fiber/fiber.go:180-188`

`Unblock()` sets `State = StateReady`. In this runtime, a fiber that blocked
inside a sync primitive is still inside `f.Run()`. When the primitive calls
`Unblock()`, the fiber is executing, not awaiting re-scheduling. Any observer
(deadlock detector, visualizer, test) sees `IsRunnable() == true` for an
actively running fiber.

**Fix:** `Unblock()` should restore `StateRunning` when the fiber's `runStarted`
flag is set and `completion` is not set.

---

### Scheduler

---

#### SC-1 · Major · `Remove()` is broken for Priority and WorkStealing schedulers
**File:** `internal/scheduler/scheduler.go:155-179`; `priority.go` and
`workstealing.go` (no override)

`BaseScheduler.Remove` scans only `s.runQueue` and `s.blockedQueue`.
`PriorityScheduler` stores fibers in `s.pqueue` (a heap); `WorkStealingScheduler`
stores them in worker `localQueues`. Neither scheduler overrides `Remove()`, so
`Remove()` always returns "fiber not found" for both schedulers and removes
nothing.

The runtime's spawn-failure rollback path calls `rt.scheduler.Remove(f.ID)` and
then considers the fiber cleaned up. With Priority or WorkStealing, the fiber
remains queued in the scheduler. On the next `Start()`, the execution loop pops
and runs it — a fiber whose `Spawn` reported failure executes anyway, with no
`rt.fibers` entry.

**Fix:** Override `Remove()` in `PriorityScheduler` (scan and remove from
`s.pqueue`, then `heap.Init`) and in `WorkStealingScheduler` (scan all worker
local queues and the global queue). Add regression tests.

---

#### SC-2 · Major · `completed` map grows without bound — permanent memory leak
**File:** `internal/scheduler/scheduler.go:116, 331-340`; `priority.go:69-85`;
`workstealing.go:263-271`

Every completed fiber leaves a permanent entry in `s.completed` (keyed by
monotonically increasing `FiberID`). The map is only cleared by `Clear()`, which
is called exclusively from `Reset()` — never during normal operation or `Stop()`.
A server running with a steady fiber workload accumulates one map entry per
fiber for the life of the process.

**Fix:** Bound the map by adding a generation-based purge: after `Clear()` the
generation increments and the old completed set is discarded. Alternatively,
since `MarkCompleted` is called immediately after a fiber is removed from the
queue, only in-flight fibers need tracking; remove the entry once a GC cycle
passes (or use a bounded LRU with a safe size, e.g. 2× `maxWorkers`).

---

#### SC-3 · Major · `BlockFiber`/`UnblockFiber` are incoherent for Priority and WorkStealing
**File:** `internal/scheduler/scheduler.go:289-324`

`BaseScheduler.BlockFiber` removes the fiber from `s.runQueue` (empty for
Priority/WS) and appends to `s.blockedQueue`. The fiber remains in
`s.pqueue` / worker `localQueue`, so `Next()` will dispatch a "blocked" fiber.
`BaseScheduler.UnblockFiber` appends the fiber to `s.runQueue`, which
Priority/WS `Next()` never reads — the fiber is enqueued but can never dispatch
(lost fiber). Additionally `BlockFiber` does not deduplicate: calling it twice
puts two copies in `blockedQueue`.

No current non-test callers exist, but these are exported methods; any future
runtime blocking integration hits this immediately.

**Fix:** Override `BlockFiber`/`UnblockFiber` in both subschedulers (operating
on `pqueue` and `localQueues` respectively), or remove them from the exported
surface and document the limitation.

---

#### SC-4 · Minor · Priority starvation: `AgeAll` is dead code
**File:** `internal/scheduler/priority.go:195-203`

`AgeAll` exists as the anti-starvation mechanism for `PriorityScheduler` but is
never called outside a single package-internal test. Under continuous
high-priority spawns, low-priority fibers are never dispatched. The
`TypeRoundRobin` doc comment promises "fixed time quantum per dispatch" and the
struct comment says "rotated to the back of the queue" — neither is true; the
RoundRobin scheduler behaves identically to FIFO.

**Fix:** Wire `AgeAll` to be called periodically inside the priority scheduler's
`Next()` (e.g., every N pops), or remove the promise from the docs and document
the starvation property explicitly.

---

#### SC-5 · Minor · `SchedulerStats.CurrentRunQueue` always 0 for Priority and WorkStealing
**File:** `internal/scheduler/scheduler.go:273-286`

`GetStats()` reads `len(s.runQueue)` from the base struct. Priority/WS never
populate `s.runQueue` (they use `pqueue`/`localQueues`). Any consumer reading
`CurrentRunQueue` from `GetStats()` on those schedulers sees a permanently empty
queue while `Size()` returns the correct count.

**Fix:** Override `GetStats()` in both subschedulers to call `s.pqueue.Len()` /
`s.Size()` for `CurrentRunQueue`.

---

#### SC-6 · Minor · WorkStealing global queue is write-dead (never populated)
**File:** `internal/scheduler/workstealing.go:61-92`

`Schedule()` comment says "Schedule adds a fiber to the global queue" but
actually always places fibers into a worker local queue. `globalQueue` is never
written to by `Schedule()`. The global-queue branch in `Next()` and
`filterFinished` can never return a fiber. `MarkCompleted` and `Clear` scrub the
global queue unnecessarily.

**Fix:** Either wire `Schedule()` to actually overflow into `globalQueue` when
all worker queues exceed a threshold (the natural use of a global queue), or
remove `globalQueue` and the dead branches in `Next()` and clean up the
redundant scrub in `MarkCompleted`.

---

#### SC-7 · Nit · `nextWorker` wraparound at `int64` max
**File:** `internal/scheduler/workstealing.go:100`

After 2^63 `Next()` calls, the `atomic.AddInt64` counter wraps negative and
`% s.numWorkers` produces a negative index, causing a panic. Not reachable in
practice, but should be documented or guarded.

**Fix:** Use `atomic.AddUint64` and convert with `% uint64(s.numWorkers)`.

---

### Sync Primitives

---

#### SY-1 · Major · `Lock(nil)` silently no-ops — mutual exclusion is silently lost
**File:** `internal/sync/mutex.go:32-34`; `mutex.go:197-199`; `mutex.go:299-301`;
`waitgroup.go:66-68`; `waitgroup.go:183-185`

`if fm == nil || currentFiber == nil { return }` — a nil-fiber `Lock()` returns
immediately as if the lock was acquired. A buggy call site (nil context,
wrong-goroutine caller) enters the critical section without holding the mutex.
The problem surfaces far from the cause when `Unlock(nil)` panics. This
behaviour is inconsistent with:
- `LockCtx` which returns a clear error for nil fiber.
- `FiberChannel.Send` which returns an error for nil fiber.

**Fix:** Panic (not silent return) for nil-fiber `Lock()`, matching the panic
approach used in `Unlock()`, or return an error via a consistent `LockErr()`
signature. Document the nil-fiber contract explicitly.

---

#### SY-2 · Major · Blocking primitives pin worker slots; bounded dispatch can hard-deadlock
**File:** `internal/runtime/runtime.go:438-449` (bounded `active < rt.numWorkers`
gate); `internal/sync/mutex.go`, `channel.go`, `waitgroup.go` (blocking)

A fiber parked inside `Lock`/`Receive`/`Wait` keeps its dispatch-goroutine alive
and its worker slot occupied until its function returns. There is no "blocked
fiber releases its slot" pathway. Scenario: `numWorkers` fibers all block on a
`FiberChannel` receive whose sender fiber has not yet been dispatched → sender
can never be dispatched → permanent deadlock. The deadlock detector will flag
it, but nothing can break the cycle.

This is a design-level property: the execution model commits a goroutine per
dispatched fiber with no preemption or slot-release on block.

**Fix (design):** Document this fundamental limitation in the package-level
godoc and README. Add a runtime check: before parking a fiber, verify that at
least one non-blocked worker slot exists; if not, log a warning. Long-term:
consider an M:N dispatch model where blocked fibers release their slot.

---

#### SY-3 · Minor · `FiberSemaphore(0)` is a lost-signal trap
**File:** `internal/sync/waitgroup.go:170-179, 268-284`

`NewFiberSemaphore(0)` sets `maxPermits = 0`. A `Release()` that arrives before
the acquirer enqueues: `len(waitQueue) == 0` and `permits(0) >= maxPermits(0)`,
so the `Release` is a no-op — the signal is permanently lost. The subsequent
`Acquire` blocks forever.

**Fix:** Either reject `permits == 0` at construction (return an error or panic),
or document this ordering constraint with a prominent warning. A semaphore with
zero initial permits but the intent to signal is a common pattern that should
work.

---

#### SY-4 · Minor · `TrySend` conflates channel-closed with channel-full
**File:** `internal/sync/channel.go:271-294`

`TrySend` on a closed channel returns `false`, indistinguishable from
backpressure. A retry loop around `TrySend` on a closed channel will spin
forever. `Send` correctly returns `ErrChannelClosed`, but `TrySend` loses the
signal.

**Fix:** Change `TrySend` to return `(bool, error)` where the error is
`ErrChannelClosed` when the channel is closed, `false/nil` when full.
Alternatively document the limitation clearly and add a `TrySendErr() error` variant.

---

### Web / Ops

---

#### WO-1 · Major · `release.yml` uses mutable `@v4` tag with `contents: write` token
**File:** `.github/workflows/release.yml:14`

`ci.yml` pins every action to a full 40-char SHA and the `check_action_pins.sh`
script enforces it — but the script only **warns** (exit 0) on short refs like
`@v4`, and it only runs in `ci.yml`. `release.yml` uses `actions/checkout@v4`
(mutable) with `permissions: contents: write`. A supply-chain tag move or
upstream compromise executes attacker code with a repo-write token.

**Fix:** Pin `actions/checkout` in `release.yml` to the same full SHA as in
`ci.yml`. Update `check_action_pins.sh` to hard-fail (exit 1) on any ref
shorter than 40 chars, and run it from `release.yml` as well.

---

#### WO-2 · Minor · `/metrics` uptime is ~55 years after a reset
**File:** `internal/metrics/metrics.go:276`; `web/server.go:406`

`Reset()` sets `StartTime = time.Time{}` (the zero value). Any `/metrics` scrape
between reset and the next `Start` reports
`greenthreads_uptime_seconds ≈ time.Since(zero)` ≈ 56 years, corrupting
dashboards and alerts.

**Fix:** In `GetSnapshot()`, guard `Uptime = time.Since(m.StartTime)` with
`if m.StartTime.IsZero() { Uptime = 0 }`.

---

#### WO-3 · Minor · Dead pprof import with misleading comment
**File:** `cmd/server/main.go:8-10`

`_ "net/http/pprof"` registers handlers on `http.DefaultServeMux`, but the
server calls `ListenAndServe` with its own `s.Handler()` mux — `DefaultServeMux`
is never served. `/debug/pprof` is **not** reachable. The comment describes it
as "an intentional production observability feature… restricted via network
policy," which is false.

**Fix:** Remove the blank import and the misleading comment. If pprof is desired,
wire it to a separate, authenticated listener.

---

#### WO-4 · Minor · CI coverage gate (45%) is far below the claimed 85%
**File:** `.github/workflows/ci.yml:34`; commit message "85% coverage"

The enforced gate is 45% (`COVERAGE_MIN=45`). A regression to 50% passes CI.
The Makefile default is 40%. Actual aggregate coverage is 85.3% — the gate
doesn't protect this level.

**Fix:** Raise the CI gate to match actual coverage (e.g. `COVERAGE_MIN=80`).
Add per-package floors in a coverage config file to prevent silent rot in any
one package.

---

#### WO-5 · Nit · CSP `connect-src` permits WebSocket to any host
**File:** `web/server.go:329`

`connect-src 'self' ws: wss:` permits WebSocket connections to arbitrary
origins. The bundled app only dials same-host. Tighten to the served origin
(e.g. `connect-src 'self'`) so a future XSS or injected script cannot exfiltrate
data to an arbitrary WebSocket endpoint.

**Fix:** Replace `ws: wss:` with `'self'` in the CSP header.

---

#### WO-6 · Nit · Docker base images use floating tags, not digests
**File:** `Dockerfile:1, 9`

`golang:1.26.5-alpine` and `gcr.io/distroless/static-debian12:nonroot` are
mutable tags. The Trivy scan in CI mitigates blast radius, but a supply-chain
compromise of the base images is not detected until the image is built.

**Fix:** Pin both base images by `@sha256:<digest>` and document the pinned
digests with their semantic version as a comment.

---

## Resolved Issues (prior audit — AUDIT.md IDs 1-26)

All 26 issues from the AUDIT.md first/second/third pass are resolved. See
`AUDIT.md` for their descriptions and the delivery evidence.

Key closures relevant to this new issue list:
- Gorilla WebSocket `CheckOrigin` correctly implemented (AU-3 equiv.)
- Auth token constant-time compare in place (AU-19 equiv.)
- WebSocket `ReadHeaderTimeout`/`WriteTimeout` set (AU-4 equiv.)
- `/metrics` auth-wrapped on non-loopback binds (AU-6 equiv.)
- All CI actions SHA-pinned in `ci.yml` (AU-5 equiv.) — **but `release.yml`
  missed the pin audit (WO-1 above).**
- `MarkCompleted` idempotence correct (AU-12 equiv.)
- Fiber reaping from `rt.fibers` in `complete()` is present for the normal
  path — **but not for the shutdown drain path (RT-5 above).**
