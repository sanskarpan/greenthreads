# ADR 0001: Owned Goroutine Execution Model

**Status:** Accepted  
**Date:** 2026-07

---

## Problem

Go does not expose a stable public API for saving and restoring arbitrary
goroutine register state or call stacks. Without this, there is no portable way
to implement true green-thread context switching (save state at yield point,
resume from the same instruction later) inside a normal Go program.

An earlier prototype attempted to simulate time slicing by restarting the same
fiber function from the top when a scheduler quantum expired. This caused
duplicate execution of side effects and introduced data races on the fiber's
internal state fields because the goroutine from the previous quantum was still
running.

---

## Decision

Each fiber function is admitted exactly once to a runtime-owned goroutine.
The goroutine runs to completion or until the function returns normally or
panics. Lifecycle state (`Created → Runnable → Running → Blocked → Done`) is
maintained with synchronized snapshots. Blocking is implemented by parking the
goroutine on a Go channel or condition variable inside the sync primitive, not
by preempting or re-enqueuing it.

This means greenthreads is a **cooperative, non-preemptive** scheduler layered
on top of real goroutines, not a replacement for the Go runtime scheduler.

---

## Rationale

- **Correctness over cleverness.** Any approach using `unsafe` or
  assembly-level context switching would be OS/architecture-specific, depend on
  undocumented Go runtime internals, and break on the next runtime release. The
  owned-goroutine model is correct by construction.

- **Testability.** The scheduler contract (`Add`, `Next`, `Len`) is a pure
  interface. Because each fiber runs once and terminates, tests can assert exact
  completion counts without race conditions caused by a fiber running multiple
  times.

- **Clean shutdown.** Because the runtime owns each goroutine, `rt.Stop` can
  join all in-flight work with a deadline. There is no background goroutine
  that might escape the test or process boundary.

- **Panic isolation.** Wrapping each fiber in a `recover()` call is trivial
  in the owned-goroutine model. A panicking fiber does not crash the process;
  it transitions to the `Panicked` state and the worker slot is released.

---

## Consequences

**Positive:**

- No duplicate execution of fiber functions.
- Shutdown is deterministic — `rt.Stop(ctx)` drains all goroutines before
  returning.
- Race detector clean — no concurrent mutation of shared fiber state without
  a lock.
- Panic capture is automatic and per-fiber.

**Negative:**

- **No preemption.** A long-running fiber cannot be forcibly interrupted. Users
  who need preemptive cancellation must add explicit `ctx.Done()` checks inside
  the fiber function.

- **A blocked fiber holds a worker slot.** Unlike a true green-thread runtime
  where a blocking call yields the execution context, a fiber blocked on
  `time.Sleep` or a system call ties up one of the `numWorkers` goroutines for
  the full duration. Users must size `numWorkers` accordingly.

- **No stackful continuations.** You cannot yield in the middle of a fiber
  function and resume from the same point later. The fiber either runs to
  completion or parks on a sync primitive until the primitive unblocks.

---

## Alternatives considered

| Alternative | Reason rejected |
|---|---|
| Assembly context switching (`SAVE`/`RESTORE` registers) | OS/arch-specific; relies on undocumented Go runtime ABI; breaks on runtime upgrades |
| `reflect` or `unsafe`-based stack manipulation | Undefined behaviour; breaks escape analysis and GC |
| Timeout-based re-enqueue (earlier prototype) | Caused duplicate side effects and data races on fiber state |
| Wrapping `goroutine` with `cgo` setjmp/longjmp | Mixes Go and C stack frames; incompatible with `go test -race` |
