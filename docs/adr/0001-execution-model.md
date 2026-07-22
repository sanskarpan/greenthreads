# ADR 0001: Owned Goroutine Execution Model

## Context

Go does not expose a portable public API for saving and restoring arbitrary Go
registers and call stacks. The previous implementation simulated a time slice
by starting a goroutine and re-enqueuing the same fiber while it was still
running.

## Decision

Admit each fiber function once to a runtime-owned goroutine, record lifecycle
state with synchronized snapshots, and use explicit wait notifications for
synchronization. Do not claim preemption or stackful continuation support.

## Consequences

Functions cannot be forcibly interrupted and a blocking function uses a Go
goroutine. In exchange, one function cannot be executed twice concurrently,
shutdown can join owned work, and the scheduler contract is testable.

## Alternatives considered

Assembly context switching was rejected as OS/architecture-specific and outside
the supported Go API. Timeout-based re-enqueue was rejected because it caused
duplicate execution and data races.
