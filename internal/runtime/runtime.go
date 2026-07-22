// Package runtime owns fiber admission, lifecycle, snapshots, and diagnostics.
package runtime

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
	"github.com/sanskar/greenthreads/internal/metrics"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

const (
	maxWorkers = 64
)

// Runtime owns scheduler admission, fiber lifecycle, metrics, and update
// publication. A fiber function is admitted once and runs in one owned
// goroutine; the package does not promise preemptive or stackful switching.
type Runtime struct {
	scheduler        scheduler.Scheduler
	metrics          *metrics.Metrics
	eventTracker     *metrics.EventTracker
	deadlockDetector *DeadlockDetector

	fibers       map[fiber.FiberID]*fiber.Fiber
	fibersMu     sync.RWMutex
	currentFiber *fiber.Fiber
	mainFiber    *fiber.Fiber

	running bool
	ctx     context.Context
	cancel  context.CancelFunc

	stackSize  int
	numWorkers int

	lifecycleWG sync.WaitGroup
	fiberWG     sync.WaitGroup
	resultChan  chan fiberResult
	mu          sync.RWMutex
}

type fiberResult struct {
	fiber    *fiber.Fiber
	duration time.Duration
}

// RuntimeUpdate is an immutable-by-convention snapshot for observers.
type RuntimeUpdate struct {
	Fibers        []*fiber.Fiber
	Metrics       metrics.MetricsSnapshot
	Events        []metrics.FiberEvent
	SchedulerType string
	Timestamp     time.Time
}

// NewRuntime creates a stopped runtime with a selected scheduler.
func NewRuntime(schedulerType scheduler.SchedulerType, numWorkers int) *Runtime {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}
	var sched scheduler.Scheduler
	switch schedulerType {
	case scheduler.TypeFIFO:
		sched = scheduler.NewFIFOScheduler()
	case scheduler.TypeRoundRobin:
		sched = scheduler.NewRoundRobinScheduler(10 * time.Millisecond)
	case scheduler.TypePriority:
		sched = scheduler.NewPriorityScheduler()
	case scheduler.TypeWorkStealing:
		sched = scheduler.NewWorkStealingScheduler(numWorkers)
	default:
		sched = scheduler.NewFIFOScheduler()
	}

	mainFiber := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "main")
	return &Runtime{
		scheduler:        sched,
		metrics:          metrics.NewMetrics(),
		eventTracker:     metrics.NewEventTracker(10000),
		deadlockDetector: NewDeadlockDetector(),
		fibers:           map[fiber.FiberID]*fiber.Fiber{mainFiber.ID: mainFiber},
		mainFiber:        mainFiber,
		currentFiber:     mainFiber,
		stackSize:        fiber.DefaultStackSize,
		numWorkers:       numWorkers,
	}
}

// Spawn validates, admits, and records one fiber. It is transactional with
// respect to scheduler admission: a failed schedule creates no ghost fiber
// or metric event.
func (rt *Runtime) Spawn(fn fiber.FiberFunc, name string) (fiber.FiberID, error) {
	if rt == nil {
		return 0, fmt.Errorf("nil runtime")
	}
	if fn == nil {
		return 0, fmt.Errorf("fiber function must not be nil")
	}

	rt.mu.RLock()
	if !rt.running {
		rt.mu.RUnlock()
		return 0, fmt.Errorf("runtime not started")
	}
	stackSize := rt.stackSize
	if len(name) > 128 {
		rt.mu.RUnlock()
		return 0, fmt.Errorf("fiber name exceeds 128 characters")
	}
	f := fiber.NewFiber(fn, stackSize, name)
	if err := rt.scheduler.Schedule(f); err != nil {
		rt.mu.RUnlock()
		return 0, fmt.Errorf("schedule fiber: %w", err)
	}
	rt.fibersMu.Lock()
	rt.fibers[f.ID] = f
	rt.fibersMu.Unlock()
	rt.mu.RUnlock()

	rt.metrics.RecordFiberCreated(f.StackSize)
	rt.eventTracker.RecordEvent(metrics.FiberEvent{
		FiberID: f.ID, EventType: metrics.EventCreated, Timestamp: time.Now(),
		Details: fmt.Sprintf("Created fiber: %s", name),
	})
	rt.metrics.RecordScheduleCall()
	rt.eventTracker.RecordEvent(metrics.FiberEvent{
		FiberID: f.ID, EventType: metrics.EventScheduled, Timestamp: time.Now(),
		Details: "Fiber scheduled",
	})
	return f.ID, nil
}

// Start begins scheduler admission and owned lifecycle goroutines.
func (rt *Runtime) Start() error {
	if rt == nil {
		return fmt.Errorf("nil runtime")
	}
	rt.mu.Lock()
	if rt.running {
		rt.mu.Unlock()
		return fmt.Errorf("runtime already running")
	}
	if err := rt.scheduler.Start(); err != nil {
		rt.mu.Unlock()
		return fmt.Errorf("start scheduler: %w", err)
	}
	if rt.mainFiber == nil {
		rt.mainFiber = fiber.NewFiber(func() {}, fiber.DefaultStackSize, "main")
	}
	rt.fibersMu.Lock()
	rt.fibers[rt.mainFiber.ID] = rt.mainFiber
	rt.fibersMu.Unlock()
	rt.ctx, rt.cancel = context.WithCancel(context.Background())
	rt.resultChan = make(chan fiberResult, rt.numWorkers)
	rt.deadlockDetector = NewDeadlockDetector()
	rt.running = true
	rt.mu.Unlock()

	rt.lifecycleWG.Add(2)
	go rt.executionLoop()
	go func() {
		defer rt.lifecycleWG.Done()
		rt.deadlockDetector.Start(rt)
	}()
	return nil
}

// Stop cancels admission, waits for lifecycle goroutines, and waits for
// in-flight fiber functions to finish. The supplied context bounds the wait
// for in-flight fibers; if it expires, Stop reports the context error and
// returns without having joined the still-running fiber goroutines. Fiber
// functions that never return cannot be forcibly stopped by any Go API; the
// context lets a caller bound how long shutdown blocks on them.
func (rt *Runtime) Stop(ctx context.Context) error {
	if rt == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rt.mu.Lock()
	if !rt.running {
		rt.mu.Unlock()
		return nil
	}
	rt.running = false
	cancel := rt.cancel
	detector := rt.deadlockDetector
	rt.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if detector != nil {
		detector.Stop()
	}
	schedulerErr := rt.scheduler.Stop()
	rt.lifecycleWG.Wait()

	// The lifecycle goroutines have returned. In-flight fiber functions run in
	// their own goroutines and may outlive the shutdown deadline; bound the
	// join by ctx so a long-running fiber cannot hold shutdown indefinitely.
	waitDone := make(chan struct{})
	go func() {
		rt.fiberWG.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		return schedulerErr
	case <-ctx.Done():
		return fmt.Errorf("runtime stop: %w", ctx.Err())
	}
}

func (rt *Runtime) executionLoop() {
	defer rt.lifecycleWG.Done()
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	active := 0
	for {
		select {
		case <-rt.ctx.Done():
			return
		case result := <-rt.resultChan:
			if active > 0 {
				active--
			}
			rt.complete(result)
		case <-ticker.C:
			for active < rt.numWorkers {
				f, err := rt.scheduler.Next()
				if err != nil {
					break
				}
				if f == nil || !f.IsRunnable() {
					continue
				}
				rt.dispatch(f)
				active++
			}
		}
	}
}

func (rt *Runtime) dispatch(f *fiber.Fiber) {
	f.MarkScheduled()
	rt.mu.Lock()
	rt.currentFiber = f
	rt.mu.Unlock()
	rt.metrics.RecordContextSwitch()
	rt.eventTracker.RecordEvent(metrics.FiberEvent{
		FiberID: f.ID, EventType: metrics.EventRunning, Timestamp: time.Now(), Details: "Fiber running",
	})

	start := time.Now()
	rt.fiberWG.Add(1)
	go func() {
		defer rt.fiberWG.Done()
		f.Run()
		rt.resultChan <- fiberResult{fiber: f, duration: time.Since(start)}
	}()
}

func (rt *Runtime) complete(result fiberResult) {
	if result.fiber == nil {
		return
	}
	f := result.fiber
	f.AddCPUTime(result.duration)
	rt.metrics.RecordFiberCompleted(f)
	rt.scheduler.MarkCompleted(f.ID)
	details := "Fiber completed"
	if err := f.Failure(); err != nil {
		details = "Fiber failed"
	}
	rt.eventTracker.RecordEvent(metrics.FiberEvent{
		FiberID: f.ID, EventType: metrics.EventCompleted, Timestamp: time.Now(), Details: details,
	})
	rt.mu.Lock()
	if rt.currentFiber == f {
		rt.currentFiber = rt.mainFiber
	}
	rt.mu.Unlock()

	// Reap the finished fiber so its bounded stack and state are released.
	// The main observer fiber is never run and never finishes, so it stays.
	if f.ID != rt.mainFiber.ID {
		rt.fibersMu.Lock()
		delete(rt.fibers, f.ID)
		rt.fibersMu.Unlock()
	}
}

// GetFiber returns an immutable snapshot by ID.
func (rt *Runtime) GetFiber(id fiber.FiberID) (*fiber.Fiber, error) {
	rt.fibersMu.RLock()
	defer rt.fibersMu.RUnlock()
	f, exists := rt.fibers[id]
	if !exists {
		return nil, fmt.Errorf("fiber %d not found", id)
	}
	return f.Clone(), nil
}

// GetFiberDirect returns a live fiber for internal integrations. Callers must
// use Fiber methods for mutations while the runtime is active.
func (rt *Runtime) GetFiberDirect(id fiber.FiberID) (*fiber.Fiber, error) {
	rt.fibersMu.RLock()
	defer rt.fibersMu.RUnlock()
	f, exists := rt.fibers[id]
	if !exists {
		return nil, fmt.Errorf("fiber %d not found", id)
	}
	return f, nil
}

// GetAllFibers returns deterministic snapshots ordered by ID.
func (rt *Runtime) GetAllFibers() []*fiber.Fiber {
	rt.fibersMu.RLock()
	fibers := make([]*fiber.Fiber, 0, len(rt.fibers))
	for _, f := range rt.fibers {
		fibers = append(fibers, f.Clone())
	}
	rt.fibersMu.RUnlock()
	sort.Slice(fibers, func(i, j int) bool { return fibers[i].ID < fibers[j].ID })
	return fibers
}

// GetMetrics returns a consistent metrics snapshot. Steal statistics are
// sourced live from the work-stealing scheduler when it is active, so they
// reflect current state rather than the always-zero metrics counters.
func (rt *Runtime) GetMetrics() metrics.MetricsSnapshot {
	snap := rt.metrics.GetSnapshot()
	if ws, ok := rt.scheduler.(*scheduler.WorkStealingScheduler); ok {
		attempts, successes := ws.GetStealStats()
		snap.TotalStealAttempts = attempts
		snap.TotalStealSuccesses = successes
		if attempts > 0 {
			snap.StealSuccessRate = float64(successes) / float64(attempts)
		} else {
			snap.StealSuccessRate = 0
		}
	}
	return snap
}

// GetEvents returns up to n most recent lifecycle events.
func (rt *Runtime) GetEvents(n int) []metrics.FiberEvent { return rt.eventTracker.GetRecentEvents(n) }

// GetScheduler returns the scheduler implementation used by this runtime.
func (rt *Runtime) GetScheduler() scheduler.Scheduler { return rt.scheduler }

// IsRunning reports whether the runtime admits work.
func (rt *Runtime) IsRunning() bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.running
}

// SetStackSize configures the stack allocated to future fibers.
func (rt *Runtime) SetStackSize(size int) {
	rt.mu.Lock()
	rt.stackSize = fiber.ValidateStackSize(size)
	rt.mu.Unlock()
}

// Reset clears stopped runtime state. Start recreates the main observer fiber
// when necessary for the next run. The caller must ensure the runtime is
// stopped; this method guards the mutation with rt.mu so the field-guarding
// invariant is consistent with the rest of the lifecycle.
func (rt *Runtime) Reset() {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	if rt.running {
		rt.mu.Unlock()
		return
	}
	rt.fibersMu.Lock()
	rt.fibers = make(map[fiber.FiberID]*fiber.Fiber)
	rt.fibersMu.Unlock()
	rt.scheduler.Clear()
	rt.metrics.Reset()
	rt.eventTracker.Clear()
	rt.mainFiber = nil
	rt.currentFiber = nil
	rt.mu.Unlock()
}
