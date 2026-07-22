# Known Limitations and Deferred Work

- The execution model is goroutine-backed and non-preemptive. A function that
  never returns cannot be forcibly stopped; callers must own cancellation in
  their function body.
- The package is under `internal/`, so it is not a consumable external Go
  library. A stable public package boundary needs a separate compatibility
  design.
- The deadlock detector reports no-progress suspicion and does not prove
  arbitrary wait-for cycles.
- Distributed tracing is not included because this repository has no outbound
  service calls. An embedding service should inject tracing around its own
  request and runtime boundaries.
- CI action references should be pinned to commit SHAs as part of the first
  release pipeline review.
