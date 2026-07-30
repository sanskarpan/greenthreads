// Package greenthreads is the public API surface for the greenthreads
// user-level threading library.
//
// All heavy lifting lives under internal/; this package re-exports the
// types and constructors that external importers need, keeping the public
// surface thin and stable.
//
// Quick-start:
//
//	rt := greenthreads.New(greenthreads.RoundRobin, 4)
//	if err := rt.Start(); err != nil {
//	    log.Fatal(err)
//	}
//	defer rt.Stop(context.Background())
//
//	done := make(chan struct{})
//	rt.Spawn(func() {
//	    fmt.Println("hello from a fiber")
//	    close(done)
//	}, "hello")
//	<-done
package greenthreads

import (
	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/metrics"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
	fibersync "github.com/sanskarpan/greenthreads/internal/sync"
)

// ---------------------------------------------------------------------------
// Type aliases — transparent to callers; no wrapping overhead.
// ---------------------------------------------------------------------------

// FiberID is the unique identifier assigned to every spawned fiber.
type FiberID = fiber.FiberID

// FiberFunc is the function signature accepted by Spawn and related calls.
type FiberFunc = fiber.FiberFunc

// SchedulerType selects the scheduling algorithm used by a Runtime.
type SchedulerType = scheduler.SchedulerType

// FiberHandle is a read-only view of a live fiber's state.
type FiberHandle = runtime.FiberHandle

// SpawnGroup fans out a batch of fibers and collects their errors.
type SpawnGroup = runtime.SpawnGroup

// MetricsSnapshot is an immutable point-in-time view of runtime statistics.
type MetricsSnapshot = metrics.MetricsSnapshot

// Runtime is the central object that manages fibers and the scheduler.
// Use New or NewWithOptions to construct one.
type Runtime = runtime.Runtime

// ---------------------------------------------------------------------------
// Scheduler-type constants.
// ---------------------------------------------------------------------------

const (
	// FIFO runs fibers in arrival order; simple and fair for short tasks.
	FIFO SchedulerType = scheduler.TypeFIFO

	// RoundRobin gives each ready fiber an equal time quantum.
	RoundRobin SchedulerType = scheduler.TypeRoundRobin

	// Priority runs higher-priority fibers first; ties broken by arrival order.
	Priority SchedulerType = scheduler.TypePriority

	// WorkStealing distributes fibers across per-worker queues and steals
	// work when a worker runs idle.
	WorkStealing SchedulerType = scheduler.TypeWorkStealing
)

// ---------------------------------------------------------------------------
// Constructors.
// ---------------------------------------------------------------------------

// New creates and returns a Runtime configured with the given scheduler type
// and worker count.  Call rt.Start() before spawning fibers.
func New(schedulerType SchedulerType, numWorkers int) *Runtime {
	return runtime.NewRuntime(schedulerType, numWorkers)
}

// NewWithOptions creates a Runtime using the functional-options pattern,
// allowing fine-grained control over stack size, fiber limits, deadlock
// detection, and more.
func NewWithOptions(opts ...runtime.RuntimeOption) *Runtime {
	return runtime.NewRuntimeWithOptions(opts...)
}

// ---------------------------------------------------------------------------
// Option constructors — surface the full set of runtime knobs.
// ---------------------------------------------------------------------------

var (
	// WithNumWorkers sets the number of OS-thread workers backing the runtime.
	WithNumWorkers = runtime.WithNumWorkers

	// WithStackSize sets the default stack size (in bytes) for new fibers.
	WithStackSize = runtime.WithStackSize

	// WithSchedulerType selects the scheduling algorithm.
	WithSchedulerType = runtime.WithSchedulerType

	// WithMaxFibers caps the number of live fibers the runtime will accept.
	WithMaxFibers = runtime.WithMaxFibers

	// WithDetectorConfig enables or disables the deadlock detector and tunes
	// its check interval and blocked-fiber timeout.
	WithDetectorConfig = runtime.WithDetectorConfig
)

// ---------------------------------------------------------------------------
// Sync primitive constructors.
// ---------------------------------------------------------------------------

var (
	// NewFiberMutex returns a new FIFO-ordered fiber-aware mutual exclusion lock.
	NewFiberMutex = fibersync.NewFiberMutex

	// NewFiberRWMutex returns a new fiber-aware reader/writer lock.
	NewFiberRWMutex = fibersync.NewFiberRWMutex

	// NewFiberChannel returns a new untyped buffered fiber channel with the
	// given capacity (0 = rendezvous / unbuffered).
	NewFiberChannel = fibersync.NewFiberChannel

	// NewFiberWaitGroup returns a new fiber-aware wait group.
	NewFiberWaitGroup = fibersync.NewFiberWaitGroup

	// NewFiberSemaphore returns a new counting semaphore with permits slots.
	NewFiberSemaphore = fibersync.NewFiberSemaphore
)

// NewFiberChannelOf returns a new type-safe buffered fiber channel that
// carries values of type T.  capacity=0 produces a rendezvous channel.
func NewFiberChannelOf[T any](capacity int) *fibersync.FiberChannelOf[T] {
	return fibersync.NewFiberChannelOf[T](capacity)
}
