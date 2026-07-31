// Package greenthreads is the stable public API for the greenthreads
// user-level fiber scheduler.
//
// It is the only package external code should import. Everything under
// internal/ is an implementation detail and may change between releases; this
// package is the compatibility surface and follows semantic versioning.
//
// # Quick start
//
//	rt := greenthreads.New(greenthreads.WorkStealing, 4)
//	if err := rt.Start(); err != nil {
//	    log.Fatal(err)
//	}
//	defer rt.Stop(context.Background())
//
//	done := make(chan struct{})
//	if _, err := rt.Spawn(func() {
//	    fmt.Println("hello from a fiber")
//	    close(done)
//	}, "greeter"); err != nil {
//	    log.Fatal(err)
//	}
//	<-done // wait for the fiber to finish (or use a SpawnGroup for many)
//
// # Scheduling
//
// Choose a scheduling policy at construction with one of [FIFO], [RoundRobin],
// [Priority], or [WorkStealing]. See [New] and [NewWithOptions].
//
// # Synchronization
//
// Fiber-aware primitives ([FiberMutex], [FiberRWMutex], [FiberChannel],
// [FiberWaitGroup], [FiberSemaphore], and the generic [NewFiberChannelOf]) let
// fibers block without consuming a worker while parked. Blocking calls take the
// running fiber, obtained from inside the fiber via [Runtime.GetFiberDirect].
//
// # Observability
//
// [Runtime.GetMetrics] returns a point-in-time [MetricsSnapshot]. The cmd/server
// binary additionally exposes a Prometheus endpoint, a WebSocket control plane,
// and opt-in OpenTelemetry tracing.
package greenthreads

import (
	"context"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/metrics"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
	fibersync "github.com/sanskarpan/greenthreads/internal/sync"
)

// ---------------------------------------------------------------------------
// Value/data types — exported as aliases (no wrapping overhead). These are
// data-transfer objects, an interface, an opaque token, or primitives whose
// only surface is their methods.
// ---------------------------------------------------------------------------

// FiberID is the unique identifier assigned to every spawned fiber.
type FiberID = fiber.FiberID

// FiberFunc is the function signature accepted by Spawn and related calls.
type FiberFunc = fiber.FiberFunc

// Fiber is an opaque token for a live fiber. Obtain a pointer via
// [Runtime.GetFiberDirect] from inside the running fiber and pass it to the
// blocking sync primitives. It is owned by the runtime: do not mutate its
// fields or retain it past the fiber's completion. Use [FiberHandle] (from
// [Runtime.GetFiber]) for read-only inspection.
type Fiber = fiber.Fiber

// FiberState is the lifecycle state of a fiber.
type FiberState = fiber.FiberState

// FiberHandle is a read-only view of a fiber's state.
type FiberHandle = runtime.FiberHandle

// SchedulerType selects the scheduling algorithm used by a Runtime.
type SchedulerType = scheduler.SchedulerType

// SchedulerStats holds scheduler counters, from [Runtime.SchedulerStats].
type SchedulerStats = scheduler.SchedulerStats

// MetricsSnapshot is an immutable point-in-time view of runtime statistics.
type MetricsSnapshot = metrics.MetricsSnapshot

// LatencyHistogram is the fiber run-time distribution held by a MetricsSnapshot.
type LatencyHistogram = metrics.LatencyHistogram

// LatencyBucket is one bucket of a [LatencyHistogram].
type LatencyBucket = metrics.LatencyBucket

// FiberEvent is a single lifecycle event, from [Runtime.GetEvents].
type FiberEvent = metrics.FiberEvent

// EventType classifies a [FiberEvent].
type EventType = metrics.EventType

// DeadlockInfo describes one detected (or resolved) deadlock episode.
type DeadlockInfo = runtime.DeadlockInfo

// Fiber-aware synchronization primitives (their surface is their methods).
type (
	// FiberMutex is a fiber-aware mutual-exclusion lock with a FIFO wait queue.
	FiberMutex = fibersync.FiberMutex
	// FiberRWMutex is a fiber-aware reader/writer lock.
	FiberRWMutex = fibersync.FiberRWMutex
	// FiberChannel is an untyped buffered or rendezvous channel between fibers.
	FiberChannel = fibersync.FiberChannel
	// FiberWaitGroup waits for a set of fibers to finish.
	FiberWaitGroup = fibersync.FiberWaitGroup
	// FiberSemaphore is a counting semaphore limiting concurrent access.
	FiberSemaphore = fibersync.FiberSemaphore
)

// Option configures a Runtime built with [NewWithOptions].
type Option = runtime.RuntimeOption

// ---------------------------------------------------------------------------
// Constants.
// ---------------------------------------------------------------------------

// Scheduling algorithms accepted by [New] and [WithSchedulerType].
const (
	// FIFO runs fibers in strict arrival order; simple and fair for short tasks.
	FIFO SchedulerType = scheduler.TypeFIFO
	// RoundRobin treats ready fibers equally (currently equivalent to FIFO
	// until preemptive quantum scheduling lands).
	RoundRobin SchedulerType = scheduler.TypeRoundRobin
	// Priority runs higher-priority fibers first, with anti-starvation aging.
	Priority SchedulerType = scheduler.TypePriority
	// WorkStealing distributes fibers across per-worker queues and steals work
	// when a worker runs idle.
	WorkStealing SchedulerType = scheduler.TypeWorkStealing
)

// Event types reported by [Runtime.GetEvents].
const (
	EventCreated       EventType = metrics.EventCreated
	EventScheduled     EventType = metrics.EventScheduled
	EventRunning       EventType = metrics.EventRunning
	EventYielded       EventType = metrics.EventYielded
	EventBlocked       EventType = metrics.EventBlocked
	EventUnblocked     EventType = metrics.EventUnblocked
	EventCompleted     EventType = metrics.EventCompleted
	EventContextSwitch EventType = metrics.EventContextSwitch
)

// Fiber lifecycle states, as reported by a [FiberHandle].
const (
	// StateReady means the fiber is queued and eligible for dispatch.
	StateReady FiberState = fiber.StateReady
	// StateRunning means the fiber function is currently executing.
	StateRunning FiberState = fiber.StateRunning
	// StateBlocked means the fiber is parked on a synchronization primitive.
	StateBlocked FiberState = fiber.StateBlocked
	// StateFinished means the fiber function returned normally.
	StateFinished FiberState = fiber.StateFinished
	// StateDead means the fiber has been torn down and cannot be rescheduled.
	StateDead FiberState = fiber.StateDead
)

// ---------------------------------------------------------------------------
// Sentinel errors. Test with errors.Is.
// ---------------------------------------------------------------------------

var (
	// ErrNotStarted is returned by Spawn when the runtime has not been started.
	ErrNotStarted = runtime.ErrNotStarted
	// ErrAlreadyRunning is returned by Start/Reset when the runtime is running.
	ErrAlreadyRunning = runtime.ErrAlreadyRunning
	// ErrStoppedDuringSpawn is returned when Spawn races a concurrent Stop.
	ErrStoppedDuringSpawn = runtime.ErrStoppedDuringSpawn
	// ErrNilRuntime is returned when a method is called on a nil runtime.
	ErrNilRuntime = runtime.ErrNilRuntime
	// ErrMaxFibersReached is returned by Spawn when the WithMaxFibers cap is hit.
	ErrMaxFibersReached = runtime.ErrMaxFibersReached
)

// ---------------------------------------------------------------------------
// Runtime — the central object. Wrapped (not aliased) so its method
// documentation is visible under this package on pkg.go.dev and the public
// contract is decoupled from the internal implementation.
// ---------------------------------------------------------------------------

// Runtime admits fibers and drives the scheduler. Construct one with [New] or
// [NewWithOptions]; it is safe for concurrent use.
type Runtime struct {
	rt *runtime.Runtime
}

// New creates a Runtime with the given scheduler type and worker count.
// Call [Runtime.Start] before spawning fibers.
func New(schedulerType SchedulerType, numWorkers int) *Runtime {
	return &Runtime{rt: runtime.NewRuntime(schedulerType, numWorkers)}
}

// NewWithOptions creates a Runtime using the functional-options pattern for
// fine-grained control over workers, stack size, fiber limits, and deadlock
// detection. See [WithNumWorkers] and friends.
func NewWithOptions(opts ...Option) *Runtime {
	return &Runtime{rt: runtime.NewRuntimeWithOptions(opts...)}
}

// Start begins scheduler admission and the runtime's lifecycle goroutines.
// It must be called before Spawn.
func (r *Runtime) Start() error { return r.rt.Start() }

// StartWithContext is like [Runtime.Start] but ties the run to ctx: when ctx is
// cancelled the runtime stops as if Stop were called.
func (r *Runtime) StartWithContext(ctx context.Context) error { return r.rt.StartWithContext(ctx) }

// Stop cancels admission and drains in-flight fibers, bounded by ctx. If ctx
// expires it returns the context error without joining still-running fibers.
func (r *Runtime) Stop(ctx context.Context) error { return r.rt.Stop(ctx) }

// Reset clears per-run state and counters (lifetime metrics survive). The
// runtime must be stopped; otherwise it returns [ErrAlreadyRunning].
func (r *Runtime) Reset() error { return r.rt.Reset() }

// IsRunning reports whether the runtime is currently admitting work.
func (r *Runtime) IsRunning() bool { return r.rt.IsRunning() }

// SetStackSize sets the default stack-size hint (bytes) for subsequently
// spawned fibers.
func (r *Runtime) SetStackSize(size int) { r.rt.SetStackSize(size) }

// Spawn admits a fiber with a display name and returns its ID. It returns
// [ErrNotStarted] before Start, [ErrMaxFibersReached] at the WithMaxFibers cap,
// or [ErrStoppedDuringSpawn] when racing a concurrent Stop.
func (r *Runtime) Spawn(fn FiberFunc, name string) (FiberID, error) { return r.rt.Spawn(fn, name) }

// SpawnWithResult spawns a fiber whose return value is delivered on the returned
// channel when it finishes.
func (r *Runtime) SpawnWithResult(fn func() interface{}, name string) (FiberID, <-chan interface{}, error) {
	return r.rt.SpawnWithResult(fn, name)
}

// SpawnWithTimeout spawns a fiber and returns once it finishes or timeout
// elapses. The caller unblocks after timeout; the fiber runs to completion.
func (r *Runtime) SpawnWithTimeout(fn FiberFunc, name string, timeout time.Duration) (FiberID, error) {
	return r.rt.SpawnWithTimeout(fn, name, timeout)
}

// NewSpawnGroup creates a [SpawnGroup] for structured fan-out.
func (r *Runtime) NewSpawnGroup() *SpawnGroup { return &SpawnGroup{sg: r.rt.NewSpawnGroup()} }

// GetMetrics returns a snapshot of the current run's statistics.
func (r *Runtime) GetMetrics() MetricsSnapshot { return r.rt.GetMetrics() }

// GetLifetimeMetrics returns cumulative statistics that survive Reset, suitable
// for monotonic Prometheus-style counters.
func (r *Runtime) GetLifetimeMetrics() MetricsSnapshot { return r.rt.GetLifetimeMetrics() }

// GetEvents returns up to n most-recent lifecycle events, newest last.
func (r *Runtime) GetEvents(n int) []FiberEvent { return r.rt.GetEvents(n) }

// GetFiber returns a read-only [FiberHandle] for the given fiber, and false if
// it is not currently tracked.
func (r *Runtime) GetFiber(id FiberID) (FiberHandle, bool) { return r.rt.GetFiberHandle(id) }

// GetAllFibers returns read-only snapshots (safe copies) of all tracked fibers.
func (r *Runtime) GetAllFibers() []*Fiber { return r.rt.GetAllFibers() }

// GetFiberDirect returns the live [Fiber] token for id, for passing to the
// blocking sync primitives from inside the running fiber.
func (r *Runtime) GetFiberDirect(id FiberID) (*Fiber, error) { return r.rt.GetFiberDirect(id) }

// DeadlockDetector returns the runtime's deadlock detector.
func (r *Runtime) DeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{dd: r.rt.DeadlockDetector()}
}

// SchedulerStats returns the active scheduler's counters.
func (r *Runtime) SchedulerStats() SchedulerStats { return r.rt.GetScheduler().GetStats() }

// ---------------------------------------------------------------------------
// SpawnGroup — structured fan-out.
// ---------------------------------------------------------------------------

// SpawnGroup fans out a batch of fibers and lets the caller wait for all of
// them. Obtain one with [Runtime.NewSpawnGroup].
type SpawnGroup struct {
	sg *runtime.SpawnGroup
}

// Spawn admits a fiber into the group and returns its ID.
func (g *SpawnGroup) Spawn(fn FiberFunc, name string) (FiberID, error) { return g.sg.Spawn(fn, name) }

// Wait blocks until every fiber in the group finishes, returning any spawn
// errors collected during the fan-out.
func (g *SpawnGroup) Wait() []error { return g.sg.Wait() }

// IDs returns the IDs of the fibers admitted into the group.
func (g *SpawnGroup) IDs() []FiberID { return g.sg.IDs() }

// ---------------------------------------------------------------------------
// DeadlockDetector — surfaces suspected deadlocks. Only the operator-facing
// controls are exposed; the detector's Start/Stop lifecycle is owned by the
// runtime.
// ---------------------------------------------------------------------------

// DeadlockDetector reports suspected deadlocks. It is nil-safe: methods may be
// called even on a detector obtained before the first Start. Obtain one via
// [Runtime.DeadlockDetector].
type DeadlockDetector struct {
	dd *runtime.DeadlockDetector
}

// SetEnabled turns background deadlock detection on or off.
func (d *DeadlockDetector) SetEnabled(enabled bool) { d.dd.SetEnabled(enabled) }

// IsEnabled reports whether detection is enabled.
func (d *DeadlockDetector) IsEnabled() bool { return d.dd.IsEnabled() }

// SetCheckInterval sets how often the detector scans for deadlocks.
func (d *DeadlockDetector) SetCheckInterval(interval time.Duration) { d.dd.SetCheckInterval(interval) }

// SetTimeout sets how long a no-progress state must persist before it is
// flagged as a deadlock.
func (d *DeadlockDetector) SetTimeout(timeout time.Duration) { d.dd.SetTimeout(timeout) }

// GetDeadlocks returns the detected (and resolved) deadlock episodes.
func (d *DeadlockDetector) GetDeadlocks() []DeadlockInfo { return d.dd.GetDeadlocks() }

// ClearDeadlocks discards the recorded deadlock history.
func (d *DeadlockDetector) ClearDeadlocks() { d.dd.ClearDeadlocks() }

// ---------------------------------------------------------------------------
// Option constructors.
// ---------------------------------------------------------------------------

var (
	// WithNumWorkers sets the number of worker goroutines backing the runtime.
	WithNumWorkers = runtime.WithNumWorkers
	// WithStackSize sets the default stack-size hint (bytes) for new fibers.
	WithStackSize = runtime.WithStackSize
	// WithSchedulerType selects the scheduling algorithm.
	WithSchedulerType = runtime.WithSchedulerType
	// WithMaxFibers caps the number of concurrently live fibers; Spawn returns
	// [ErrMaxFibersReached] past the cap. The cap is enforced atomically.
	WithMaxFibers = runtime.WithMaxFibers
	// WithDetectorConfig enables or disables the deadlock detector and tunes its
	// check interval and blocked-fiber timeout.
	WithDetectorConfig = runtime.WithDetectorConfig
)

// ---------------------------------------------------------------------------
// Sync primitive constructors.
// ---------------------------------------------------------------------------

var (
	// NewFiberMutex returns a new FIFO-ordered fiber-aware mutual-exclusion lock.
	NewFiberMutex = fibersync.NewFiberMutex
	// NewFiberRWMutex returns a new fiber-aware reader/writer lock.
	NewFiberRWMutex = fibersync.NewFiberRWMutex
	// NewFiberChannel returns a buffered fiber channel (capacity 0 = rendezvous).
	NewFiberChannel = fibersync.NewFiberChannel
	// NewFiberWaitGroup returns a new fiber-aware wait group.
	NewFiberWaitGroup = fibersync.NewFiberWaitGroup
	// NewFiberSemaphore returns a counting semaphore with the given permits.
	NewFiberSemaphore = fibersync.NewFiberSemaphore
)

// NewFiberChannelOf returns a type-safe buffered fiber channel carrying values
// of type T. capacity 0 produces a rendezvous channel.
func NewFiberChannelOf[T any](capacity int) *fibersync.FiberChannelOf[T] {
	return fibersync.NewFiberChannelOf[T](capacity)
}
