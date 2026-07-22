// Package fiber models bounded, goroutine-backed fiber work items and their
// observable lifecycle state.
package fiber

// This file previously exposed a simulated execution-context API (Context,
// ContextManager, SwitchContext, Yield) that did not perform real stackful
// context switching. Per ADR 0001 the runtime is goroutine-backed and
// non-preemptive; the vestigial public API was removed to stop implying
// capabilities the library does not provide. Lifecycle state lives on Fiber
// directly and is updated through synchronized methods.
