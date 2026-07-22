# Production Readiness Audit

Audit date: 2026-07-21 (re-audit of current state after prior hardening pass)

## System model

```text
cmd/server/main.go -> web.Server (private HTTP mux + WS) -> runtime.Runtime
examples/simple    -> runtime.Runtime
runtime.Runtime  -> scheduler (FIFO | RoundRobin | Priority | WorkStealing)
                 -> fiber (bounded simulated stack, synchronized lifecycle)
                 -> metrics, event tracker, deadlock detector
internal/sync    -> fiber-aware channel, mutex, RWMutex, wait group, semaphore
web/static       -> browser visualization client (textContent-only rendering)
```

The runtime admits each fiber function exactly once to a runtime-owned goroutine
(`dispatch` -> `fiber.Run` -> `resultChan`). It is goroutine-backed and
non-preemptive (ADR 0001); it does not perform stackful context switching.

## Baseline evidence (current)

- `go build -trimpath ./...`: clean.
- `go vet ./...`: clean.
- `go test ./...`: all packages pass.
- `go test -race ./...`: clean (no data races).
- `golangci-lint run ./...`: 0 issues.
- Aggregate coverage: 45.2% (sits on the CI gate of 45%).
- `govulncheck`: not run locally; CI invokes `golang/govulncheck-action`.
- Single dependency: `github.com/gorilla/websocket v1.5.3` (direct).

The prior audit's Critical/High findings (timeout re-enqueue, unauthenticated
control plane, unchecked type assertions, rendezvous message loss,
close-of-closed-channel, `MarkCompleted` self-deadlock, polling sync
primitives, DOM XSS) are all resolved in the current code. The defects below
are the remaining, subtler issues found in the current state.

## Prioritized defect list

```
ID: 1
Severity: High
Category: Reliability
Location: internal/runtime/runtime.go:186-210, web/server.go:178-204
Root Cause: rt.Stop() calls rt.fiberWG.Wait() with no context/deadline.
server.Shutdown(ctx) passes a 10s context to httpServer.Shutdown(ctx) but
rt.Stop() ignores it. A fiber sleeping longer than the shutdown deadline (or
blocked on a library sync primitive, see ID 9) blocks graceful shutdown past
the configured deadline.
Blast Radius: Every controlled shutdown with in-flight fibers exceeds its
deadline; containers hit OOM-kill/timeout instead of draining.
Fix: Thread the shutdown context into rt.Stop so fiberWG.Wait is bounded by
ctx; report in-flight fibers that were abandoned.
```

```
ID: 2
Severity: High
Category: Performance / Reliability
Location: internal/runtime/runtime.go:128-130 (no removal path)
Root Cause: Completed fibers are never removed from rt.fibers. Spawn inserts;
nothing deletes (only Reset clears the whole map). Each *fiber.Fiber retains its
*Stack (64KB default, up to 8MB). Combined with the lifetime cap (ID 15), a
runtime retains up to 10000 x 64KB = 640MB of dead stacks for its lifetime.
Blast Radius: Steady memory growth; apparent "active=0" while RSS stays at
peak; misleading metrics.
Fix: Reap completed fibers from rt.fibers in complete() and free the stack;
assert map size returns to baseline after fibers finish.
```

```
ID: 3
Severity: High
Category: Reliability
Location: web/server.go:487-520
Root Cause: broadcastLoop sends to every client serially via sendJSON, which
takes cl.writeMu and blocks up to writeTimeout (2s) per client. One slow/stuck
client stalls the broadcast for all clients (64 x 2s = 128s worst case).
Blast Radius: All connected visualizers stop receiving updates whenever any
single client is slow; cascades into WS backpressure and read-deadline kills.
Fix: Give each client a bounded per-client send queue drained by its own
writer goroutine; drop on overflow. Decouple broadcast production from writes.
```

```
ID: 4
Severity: High
Category: Reliability
Location: web/server.go:281-283
Root Cause: A 60s read deadline is set and a PongHandler resets it, but the
server never sends Ping frames. With no inbound traffic the deadline fires
after 60s and the connection is closed for every idle client.
Blast Radius: Every idle WebSocket client is disconnected within 60s; the UI
reconnects every 3s (app.js), causing churn and dropped state updates.
Fix: Run a per-connection ping ticker in the write pump; reset the read
deadline on pong. Add a lifecycle test covering idle past the deadline.
```

```
ID: 5
Severity: High
Category: Security / Supply chain
Location: .github/workflows/ci.yml:41
Root Cause: securego/gosec@master is pinned to a mutable branch tip. A
compromise of the upstream action master branch executes arbitrary code in CI
with repository secrets. (Other actions are tag-pinned -- less severe, but
still mutable; tracked in ISSUES.md.)
Blast Radius: A single compromised action can exfiltrate the GH_TOKEN or
inject backdoors into builds across every PR and push.
Fix: Pin every third-party action to a commit SHA (full 40-char) with the
version in a comment; add a guard script that fails CI if any action uses
@master/@latest.
```

```
ID: 6
Severity: Medium
Category: Security
Location: web/server.go:217-247, 125, 560-572
Root Cause: authorized() is only invoked by handleWebSocket. /metrics,
/healthz, /readyz, and / (static) are unauthenticated even on non-loopback
binds where GREENTHREADS_AUTH_TOKEN is set. /metrics exposes operational
counters; / exposes the control UI.
Blast Radius: On any non-loopback deployment, an unauthenticated remote
attacker can read metrics and the UI without the token.
Fix: Apply authorized() (or a dedicated read/auth policy) to /metrics at
minimum, and decide explicitly whether /healthz,/readyz,/ are public.
```

```
ID: 7
Severity: Medium
Category: Reliability / Performance
Location: web/server.go:262-276
Root Cause: s.clientsMu.Lock() is acquired before upgrader.Upgrade and released
after. Upgrade performs the WebSocket handshake (network I/O). All connection
admissions are serialized by this lock; a slow handshake blocks new clients.
Blast Radius: A slow-handshaking or malicious client stalls all new WebSocket
connections; MaxClients admission is serialized.
Fix: Reserve a slot under the lock, release it, then Upgrade outside the lock,
then re-acquire to register; or use an atomic counter for the slot.
```

```
ID: 8
Severity: Medium
Category: Correctness / Maintainability
Location: internal/runtime/runtime.go:353-382, 382
Root Cause: broadcastUpdates spawns a lifecycle goroutine that fills
rt.updateChan (buffered 100) every 100ms, but no consumer reads it (the web
server polls rt.GetAllFibers/GetMetrics directly). GetUpdateChannel() exposes
a channel that close()s on Stop. Dead/wasteful goroutine + misleading API.
Blast Radius: One always-on goroutine and 100-slot buffer per runtime doing
nothing; external callers using GetUpdateChannel see closed-channel zero values
after Stop.
Fix: Remove broadcastUpdates/updateChan and GetUpdateChannel, or make the web
server consume it. Pick one ownership model.
```

```
ID: 9
Severity: Medium
Category: Correctness
Location: internal/fiber/fiber.go:163-170, internal/runtime/runtime.go:285-300
Root Cause: Fiber.Yield() sets State=Ready while the fiber's goroutine is still
executing; rt.Yield() reads rt.currentFiber (the last dispatched fiber, not
necessarily the caller) and calls Fiber.Yield(). The state now lies (Ready
while running), and the deadlock detector counts it as runnable, hiding a
stuck runtime.
Blast Radius: Any caller of Yield (or reader of State after Yield) gets
incorrect state; deadlock detection becomes unreliable.
Fix: Remove the public Yield API (ADR 0001 disclaims preemption) or have it
record a yield event without changing State while the goroutine is still live.
```

```
ID: 10
Severity: Medium
Category: Observability / Correctness
Location: internal/runtime/deadlock.go:87-126, 196-205
Root Cause: dd.timeout (default 5s) is settable but never read. lastProgress is
updated but never compared to time.Now(). The detector only flags the
instantaneous "all blocked && none runnable" condition, so a runtime with one
runnable fiber that infinite-loops is never flagged. Also, only fibers blocked
on the library's own sync primitives are ever StateBlocked, so a fiber blocked
on a native channel/sleep is invisible.
Blast Radius: Real no-progress conditions are missed; operators have no alert
for a livelocked runtime.
Fix: Use dd.timeout: flag dead when time.Since(lastProgress) > timeout AND no
runnable fiber made progress. Document the limited detection coverage honestly.
```

```
ID: 11
Severity: Medium
Category: Observability
Location: internal/metrics/metrics.go:98-116, 128-160; internal/runtime (no callers)
Root Cause: RecordFiberBlocked/RecordFiberUnblocked/RecordStealAttempt are
never invoked. BlockedFibers is always 0, AverageWaitTime is never computed,
TotalStealAttempts/StealSuccessRate are always 0. The web UI shows steal rate
as 0% forever.
Blast Radius: Dashboards report blocked=0 and steal=0% regardless of reality.
Fix: Wire fiber.Block/Unblock to metrics from the runtime, and have the
work-stealing scheduler call RecordStealAttempt (or expose its counters in the
snapshot).
```

```
ID: 12
Severity: Medium
Category: Correctness
Location: internal/runtime/runtime.go:264-282, internal/scheduler/scheduler.go:292-298
Root Cause: complete() never calls scheduler.MarkCompleted. filterFinished
(inside FIFO/RR/Next) only sees finished fibers that are still in the queue,
but dispatch already removed the fiber via Next. SchedulerStats.TotalCompleted
is always 0.
Blast Radius: Scheduler stats are wrong; any dashboard reading TotalCompleted
sees 0.
Fix: Call rt.scheduler.MarkCompleted(f.ID) in complete(); add a test asserting
TotalCompleted increments across all four schedulers.
```

```
ID: 13
Severity: Medium
Category: Correctness
Location: web/server.go:398-400, internal/scheduler/priority.go:47-63, 116-126
Root Cause: handleSpawn uses GetFiberDirect (live fiber) and SetPriority AFTER
Spawn has already pushed the fiber into the priority heap. SetPriority mutates
priority under fiber.mu without scheduler coordination. A racing SetPriority
between heap.Init and heap.Pop can leave the heap inconsistent and pop a
non-max element.
Blast Radius: Priority ordering is not guaranteed under concurrent control-plane
traffic; high-priority work can be skipped.
Fix: Make priority update scheduler-owned (PriorityScheduler.UpdatePriority that
re-heaps under s.mu), and stop exposing live fibers across package boundaries.
```

```
ID: 14
Severity: Medium
Category: Reliability
Location: internal/sync/channel.go:57-91, mutex.go:29-45, waitgroup.go:64-78, 122-137
Root Cause: The library's blocking primitives wait on a private channel with no
context, deadline, or runtime lifecycle hook. A fiber blocked on
FiberChannel.Send / FiberMutex.Lock / FiberSemaphore.Acquire cannot be
interrupted, and rt.Stop() (fiberWG.Wait) cannot complete until something else
calls Close/Unlock/Release. Compounds ID 1.
Blast Radius: Any fiber using the library's own sync primitives can hang
shutdown forever.
Fix: Add ctx-aware variants (SendCtx/LockCtx/AcquireCtx) that select on
rt.ctx.Done(), or document the non-cancellable contract as a hard limitation.
```

```
ID: 15
Severity: Medium
Category: Reliability
Location: web/server.go:375-378
Root Cause: The fiber cap checks TotalFibersCreated (cumulative, never
decremented). After 10000 fibers have ever been created -- even if all
completed -- spawns are permanently rejected until reset. Combined with ID 2,
this is a memory-leak ceiling, not a concurrency guard.
Blast Radius: A long-lived runtime exhausts its spawn quota after 10000
lifetime fibers and silently stops accepting work.
Fix: Cap on active fibers, or document explicitly that the limit is lifetime.
Reflect the choice in the README table.
```

```
ID: 16
Severity: Medium
Category: Observability
Location: web/server.go:232-247
Root Cause: /metrics is hand-rolled with fmt.Fprintf and no # HELP / # TYPE
lines, no histograms (latency p50/p95/p99), and omits steal stats, blocked
fibers, peak fiber count, stack memory, and uptime that exist in
MetricsSnapshot. Content-Type uses the legacy "version=0.0.4" string.
Blast Radius: Prometheus scraping lacks documentation and type safety; real
utilization and latency are unobservable.
Fix: Generate exposition from the MetricsSnapshot fields with HELP/TYPE; add a
latency histogram for fiber RunTime; expose steal/blocked/peak.
```

```
ID: 17
Severity: Medium
Category: Performance
Location: internal/runtime/runtime.go:353-379, web/server.go:487-520
Root Cause: Two goroutines (rt.broadcastUpdates and web.broadcastLoop) each
call rt.GetAllFibers()+GetMetrics()+GetEvents(100) every 100ms. GetAllFibers
clones every fiber under lock. With 10000 fibers that is 20000 clones/sec of
pure waste (one of the two producing into a channel nobody reads -- see ID 8).
Blast Radius: Wasted CPU and allocations proportional to fiber count; GC
pressure under load.
Fix: Remove the runtime-side broadcastUpdates (ID 8) and have a single
producer.
```

```
ID: 18
Severity: Low
Category: Observability
Location: web/server.go:206-215
Root Cause: requestID middleware sets X-Request-ID but s.logger calls never
include the request ID field. The header is set but useless for log
correlation.
Blast Radius: Operators cannot correlate a failing WebSocket action to a log
line.
Fix: Inject the request ID into the per-request context and pull it into every
structured log call in the handler path.
```

```
ID: 19
Severity: Low
Category: Security
Location: web/server.go:560-572
Root Cause: Auth token accepted via ?token= query parameter (leaks into access
logs, browser history, Referer). len(token)!=len(config) early-return before
constant-time compare leaks token length via timing.
Blast Radius: Token leakage from proxy/ingress logs; minor length oracle.
Fix: Prefer Bearer header; if query param must remain, document the leak risk
and gate it behind a config flag.
```

```
ID: 20
Severity: Low
Category: Maintainability
Location: cmd/server/main.go:68-78, web/server.go:164-174
Root Cause: isLoopbackAddress is duplicated with different logic. main.go's
`if host=="localhost" || host==""` branch returns false for host=="" by
accident (correct, since :port is a wildcard bind). web/server.go's version
does not handle host=="" explicitly.
Blast Radius: Subtle divergence on edge cases (IPv6, bracketed hosts).
Fix: Extract one canonical helper and test it for localhost/127.0.0.1/::1/
[::1]/:port/0.0.0.0:port.
```

```
ID: 21
Severity: Low
Category: Security
Location: web/server.go:125, web/static/index.html
Root Cause: No Content-Security-Policy, X-Content-Type-Options,
X-Frame-Options, or Cache-Control headers on static or API responses. app.js
uses textContent (XSS mitigated at the sink), but defense-in-depth is absent.
Blast Radius: Any future DOM sink regression immediately becomes XSS; static
assets are cacheable indefinitely with no versioning.
Fix: Add a small header-setting middleware (CSP default-src 'self',
X-Content-Type-Options nosniff, frame-ancestors 'none') and a cache-busting
strategy.
```

```
ID: 22
Severity: Low
Category: Maintainability / Correctness
Location: internal/fiber/context.go:104-128, internal/fiber/stack.go:168-172
Root Cause: SwitchContext, ContextManager, Context, ContextSwitchInfo, Yield(),
and GetSystemStackSize are exposed as public API but perform no real context
switching (ADR 0001 disclaims stackful switching). GetSystemStackSize allocates
a 64-byte buffer it discards. Misleading surface area.
Blast Radius: Consumers build against a fake context-switch API and encounter
silent semantic failure.
Fix: Remove the vestigial public API (or move it to an internal/diagnostics
package) and keep only what the owned-goroutine model supports.
```

```
ID: 23
Severity: Low
Category: Correctness
Location: internal/sync/waitgroup.go:154-170
Root Cause: FiberSemaphore.Release increments permits without bound (capped
only at maxInt). Over-releasing silently grows the permit count; no max-permits
is recorded.
Blast Radius: Misused semaphore permits unbounded resource access; a bug in
pairing Acquire/Release is invisible.
Fix: Track maxPermits and clamp/no-op (or panic in debug) on over-release.
```

```
ID: 24
Severity: Low
Category: Test Quality / CI
Location: Makefile:31, .github/workflows/ci.yml
Root Cause: make fuzz runs FuzzStackPopNeverPanics and FuzzPayloadIntNeverPanics
but not FuzzPayloadStringNeverPanics. CI does not run fuzz in a long-running
corpus mode. Coverage gate is exactly 45% -- the current aggregate -- so any
minor drop fails CI but there is no per-package floor.
Blast Radius: Fuzz coverage is inconsistent; coverage rots between packages
unnoticed.
Fix: Enumerate all fuzz targets in make fuzz; add a CI step that runs each fuzz
target for a bounded time; add per-package coverage floors.
```

```
ID: 25
Severity: Low
Category: Maintainability
Location: examples/simple/main.go, bin/*, coverage.out, .dockerignore
Root Cause: examples/simple uses fmt.Println and time.Sleep-join (inconsistent
with the library's structured-logging stance, though an example). The working
tree contains generated artifacts bin/greenthreads-server and bin/simple-example
(Mach-O binaries, gitignored but present) and a stale coverage.out. .dockerignore
excludes bin but not examples/ or docs/, so the image carries unnecessary files.
Blast Radius: Minor image bloat, audit confusion, non-reproducible local state.
Fix: Add examples/, docs/, AUDIT.md, RUNBOOK.md to .dockerignore; document that
examples intentionally use plain I/O.
```

```
ID: 26
Severity: Low
Category: Correctness
Location: internal/runtime/runtime.go:393-405
Root Cause: Reset() mutates rt.mainFiber and rt.currentFiber without rt.mu. It
only checks IsRunning() (takes rt.mu.RLock then releases). Safe only because the
runtime is stopped, but inconsistent with how these fields are guarded elsewhere.
Blast Radius: Latent race if Reset is ever callable concurrently with a future
lifecycle path.
Fix: Take rt.mu.Lock for the mutation, or document and assert the
single-threaded-stopped invariant.
```

## Delivery status

All 26 findings have been addressed. Each fix is guarded by a regression
test that would have caught the original defect. Verification baseline after
this pass: `go build` / `go vet` / `go test` / `go test -race` clean,
`golangci-lint` 0 issues, aggregate coverage ~54.7% (up from 45.2%),
`scripts/check_action_pins.sh` passes.

Resolved in the first pass (High): IDs 1-5.
Resolved in the second pass:
- ID 6, 7, 15, 16, 18, 19, 21: web server hardening (auth on /metrics,
  non-locking WS admission, active-fiber cap, Prometheus exposition, request
  ID in logs, gated query-token, security headers).
- ID 8, 17: removed dead broadcastUpdates / updateChan / GetUpdateChannel.
- ID 9, 22: removed vestigial Context / ContextManager / SwitchContext /
  Yield / GetSystemStackSize; runtime no longer lies about fiber state.
- ID 10: deadlock detector now uses dd.timeout as a debounce and sets the
  blocked-fiber gauge.
- ID 11: wired BlockedFibers gauge (deadlock detector) and steal metrics
  (sourced live from the work-stealing scheduler; fixed steal-attempt
  semantics so StealSuccessRate is meaningful).
- ID 12: Scheduler interface gains MarkCompleted; complete() records it so
  scheduler stats are accurate.
- ID 13: PriorityScheduler.UpdatePriority re-heaps under the scheduler lock;
  web handleSpawn uses it instead of mutating a live fiber.
- ID 14, 23: context-aware blocking variants (SendCtx/LockCtx/AcquireCtx/
  WaitCtx/RLockCtx) and semaphore over-release clamp.
- ID 20: single canonical IsLoopbackAddress shared by cmd and web.
- ID 24: all fuzz targets enumerated in `make fuzz`; CI runs a bounded fuzz
  smoke step.
- ID 25: .dockerignore excludes examples/docs/scripts/markdown.
- ID 26: Reset() mutates state under rt.mu.

