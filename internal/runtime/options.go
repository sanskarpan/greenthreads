package runtime

import (
	gort "runtime"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/metrics"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// RuntimeOption configures a Runtime before it is started.
type RuntimeOption func(*Runtime)

// WithNumWorkers sets the number of concurrent fiber dispatch goroutines.
func WithNumWorkers(n int) RuntimeOption {
	return func(rt *Runtime) {
		if n > 0 {
			rt.numWorkers = n
		}
	}
}

// WithStackSize sets the simulated stack size for each new fiber.
func WithStackSize(size int) RuntimeOption {
	return func(rt *Runtime) {
		if size > 0 {
			rt.stackSize = size
		}
	}
}

// WithSchedulerType sets the scheduler implementation.
func WithSchedulerType(t scheduler.SchedulerType) RuntimeOption {
	return func(rt *Runtime) { rt.scheduler = scheduler.NewScheduler(t) }
}

// WithMaxFibers caps the total number of simultaneously admitted fibers.
// Spawn returns an error when the cap is reached. 0 means unlimited.
func WithMaxFibers(n int64) RuntimeOption {
	return func(rt *Runtime) { rt.maxFibers = n }
}

// WithDetectorConfig configures the deadlock detector.
func WithDetectorConfig(enabled bool, checkInterval, timeout time.Duration) RuntimeOption {
	return func(rt *Runtime) {
		rt.deadlockDetector = NewDeadlockDetector()
		rt.deadlockDetector.enabled = enabled
		rt.deadlockDetector.checkInterval = checkInterval
		rt.deadlockDetector.timeout = timeout
	}
}

// NewRuntimeWithOptions creates a Runtime with functional options.
// The scheduler type defaults to TypeFIFO and numWorkers defaults to
// runtime.NumCPU() if not overridden.
func NewRuntimeWithOptions(opts ...RuntimeOption) *Runtime {
	rt := &Runtime{
		scheduler:    scheduler.NewScheduler(scheduler.TypeFIFO),
		metrics:      metrics.NewMetrics(),
		eventTracker: metrics.NewEventTracker(1000),
		fibers:       make(map[fiber.FiberID]*fiber.Fiber),
		admitted:     make(map[fiber.FiberID]struct{}),
		stopped:      closedChan(),
		stackSize:    fiber.DefaultStackSize,
		numWorkers:   defaultNumWorkers(),
	}
	rt.deadlockDetector = NewDeadlockDetector()
	for _, opt := range opts {
		opt(rt)
	}
	return rt
}

func defaultNumWorkers() int {
	n := gort.NumCPU()
	if n < 2 {
		return 2
	}
	return n
}
