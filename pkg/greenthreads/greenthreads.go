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
// running fiber, obtained from inside the fiber via Runtime.GetFiberDirect.
//
// # Observability
//
// [Runtime.GetMetrics] returns a point-in-time [MetricsSnapshot]. The cmd/server
// binary additionally exposes a Prometheus endpoint, a WebSocket control plane,
// and opt-in OpenTelemetry tracing.
package greenthreads

import (
	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/metrics"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
	fibersync "github.com/sanskarpan/greenthreads/internal/sync"
)

// ---------------------------------------------------------------------------
// Core types — exported as aliases so there is no wrapping overhead and values
// pass transparently to and from the runtime.
// ---------------------------------------------------------------------------

// Runtime is the central object that admits fibers and drives the scheduler.
// Construct one with [New] or [NewWithOptions]; it is safe for concurrent use.
type Runtime = runtime.Runtime

// Option configures a Runtime built with [NewWithOptions].
type Option = runtime.RuntimeOption

// FiberID is the unique identifier assigned to every spawned fiber.
type FiberID = fiber.FiberID

// FiberFunc is the function signature accepted by Spawn and related calls.
type FiberFunc = fiber.FiberFunc

// Fiber is a live fiber. A pointer to one is returned by
// Runtime.GetFiberDirect and passed to the blocking sync primitives from
// inside the running fiber. Treat it as owned by the runtime: do not mutate its
// exported fields or retain it past the fiber's completion.
type Fiber = fiber.Fiber

// FiberState is the lifecycle state of a fiber.
type FiberState = fiber.FiberState

// FiberHandle is a read-only view of a live fiber's state, returned by the
// inspection methods.
type FiberHandle = runtime.FiberHandle

// SchedulerType selects the scheduling algorithm used by a Runtime.
type SchedulerType = scheduler.SchedulerType

// SpawnGroup fans out a batch of fibers and collects their errors. Obtain one
// with Runtime.NewSpawnGroup.
type SpawnGroup = runtime.SpawnGroup

// MetricsSnapshot is an immutable point-in-time view of runtime statistics.
type MetricsSnapshot = metrics.MetricsSnapshot

// DeadlockDetector surfaces suspected deadlocks. Access it via
// Runtime.DeadlockDetector (nil-safe).
type DeadlockDetector = runtime.DeadlockDetector

// DeadlockInfo describes a single detected (or resolved) deadlock episode.
type DeadlockInfo = runtime.DeadlockInfo

// Fiber-aware synchronization primitives.
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
	// ErrNilRuntime is returned when a method is called on a nil *Runtime.
	ErrNilRuntime = runtime.ErrNilRuntime
	// ErrMaxFibersReached is returned by Spawn when the WithMaxFibers cap is hit.
	ErrMaxFibersReached = runtime.ErrMaxFibersReached
)

// ---------------------------------------------------------------------------
// Constructors.
// ---------------------------------------------------------------------------

// New creates a Runtime with the given scheduler type and worker count.
// Call Start before spawning fibers.
func New(schedulerType SchedulerType, numWorkers int) *Runtime {
	return runtime.NewRuntime(schedulerType, numWorkers)
}

// NewWithOptions creates a Runtime using the functional-options pattern for
// fine-grained control over workers, stack size, fiber limits, and deadlock
// detection. See [WithNumWorkers] and friends.
func NewWithOptions(opts ...Option) *Runtime {
	return runtime.NewRuntimeWithOptions(opts...)
}

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
	// ErrMaxFibersReached past the cap. The cap is enforced atomically.
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
