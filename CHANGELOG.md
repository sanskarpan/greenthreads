# Changelog

## [Unreleased]
### Fixed
- CON-1/CON-2: complete() now called unconditionally for every dispatched fiber
- RT-4: Spawn re-check now gates on Stop initiation, not completion
- RT-5: Fiber map cleaned up on Stop
- RT-6: Deadlock detector configuration preserved across Start/Stop cycles
- SC-3: BlockFiber/UnblockFiber now correctly override in Priority and WorkStealing schedulers
- SC-7: nextWorker counter changed to uint64 to prevent wraparound panic
- OBS-1: Fiber panics now logged to stderr via the runtime
- OBS-2: metrics.Reset() no longer causes Prometheus counter decreases
- OBS-9: HTTP shutdown now drains in-flight requests before closing WebSocket connections
- CON-5: GC reference leaks in sync waiter slice pop operations fixed
- PERF-9: Stack.Push/Pop now use atomic operations instead of mutex (single-owner)
- PERF-2/3: PriorityScheduler MarkCompleted is now O(log n); heap comparisons use cached values
- PERF-1: filterFinished uses in-place compaction, zero allocations when no fibers are finished
- PERF-10: Empty-queue error is now a sentinel (no allocation per idle tick)
- SEC-3: Fiber names now validated for control characters and invalid UTF-8
- SEC-4: AllowedOrigins cross-host entries now function correctly
- SEC-6: AllowTokenInQuery now defaults to false
- DOC-1/DOC-2: False comments corrected in runtime.go and roundrobin.go

### Added
- Go runtime metrics (goroutines, memory, GC) at /metrics
- greenthreads_fiber_panics_total metric
- LOG_LEVEL environment variable for configurable log level
- -pprof-addr flag for optional pprof endpoint
- CHANGELOG.md

## [v0.1.0] - 2026-07-01
Initial release with 26 issues resolved from first audit.
