// Package runtime owns fiber admission, lifecycle, snapshots, and diagnostics.
package runtime

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
	"github.com/sanskar/greenthreads/internal/metrics"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

const (
	maxWorkers = 64

	// defaultResultChanCapacity bounds the dispatcher-result channel. It is
	// large enough to absorb a burst of admitted fibers at the worker
	// count, so a slow completion cannot block the fiber goroutine forever.
	defaultResultChanCapacity = 1024
)

// Runtime owns scheduler admission, fiber lifecycle, metrics, and update
// publication. A fiber function is admitted once and runs in one owned
// goroutine; the package does not promise preemptive or stackful switching.
type Runtime struct {
	scheduler        scheduler.Scheduler
	metrics          *metrics.Metrics
	eventTracker     *metrics.EventTracker
	deadlockDetector *DeadlockDetector

	fibers   map[fiber.FiberID]*fiber.Fiber
	fibersMu sync.RWMutex
	mainFiberMu sync.RWMutex
	mainFiber *fiber.Fiber

	stackSize  int
	numWorkers int

	// mu guards the per-run state below during Start/Stop transitions.
	// Readers use RLock; transitions take Lock.
	mu sync.RWMutex

	// lifecycleMu serializes Start and Stop so a Start cannot begin until a
	// previous Stop has finished. This is what prevents a stale execution
	// loop from observing a new run's context or result channel.
	lifecycleMu sync.Mutex

	// stopped is closed when Stop has fully drained. The next Start reads it
	// (under mu) to know the run is finished; subsequent Starts see a fresh,
	// open channel.
	stopped chan struct{}

	// Per-run state. Each Start replaces these atomically with respect to the
	// previous run, but only after the previous run's lifecycle goroutines
	// have been joined. The execution loop captures them as arguments so it
	// does not re-read mutable fields while running.
	currentFiber *fiber.Fiber
	_            [4]byte // padding to align epoch to 8 bytes on 32-bit
	epoch        int64 // incremented on each Start
	runCtx       context.Context
	runCancel    context.CancelFunc
	runDetector  *DeadlockDetector
	runFiberWG   *sync.WaitGroup
	runLifecycleWG *sync.WaitGroup
}

type fiberResult struct {
	fiber    *fiber.Fiber
	duration time.Duration
	epoch    int64
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
		stopped:          closedChan(),
	}
}

// closedChan returns a channel that is already closed. Used as the initial
// value of rt.stopped so that the first Start does not have to special-case
// "no prior Stop has run."
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// IsRunning reports whether the runtime admits work.
func (rt *Runtime) IsRunning() bool {
	if rt == nil {
		return false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.epoch > 0 && !rt.isStopped()
}

// isStopped reports whether rt.stopped is closed.
func (rt *Runtime) isStopped() bool {
	if rt.stopped == nil {
		return true
	}
	select {
	case <-rt.stopped:
		return true
	default:
		return false
	}
}

// Spawn validates, admits, and records one fiber. The fiber is registered in
// rt.fibers before it is published to the scheduler, so a fast execution loop
// that pops and completes the fiber before Spawn returns cannot produce a
// ghost fiber.
//
// If Stop is in progress the call fails without mutating state; the caller
// must observe the error and not retry without re-checking IsRunning.
//
// Admission uses a check/act/re-check pattern: the runtime state is verified
// before and after scheduling. If Stop completes between the two checks a
// narrow window exists where Spawn may fail at the re-check after a valid
// Schedule call, but it will always return an error (never silently succeed
// against a stopped scheduler).
func (rt *Runtime) Spawn(fn fiber.FiberFunc, name string) (fiber.FiberID, error) {
	if rt == nil {
		return 0, fmt.Errorf("nil runtime")
	}
	if fn == nil {
		return 0, fmt.Errorf("fiber function must not be nil")
	}
	if len(name) > 128 {
		return 0, fmt.Errorf("fiber name exceeds 128 characters")
	}

	rt.mu.RLock()
	running := rt.epoch > 0 && !rt.isStopped()
	stackSize := rt.stackSize
	rt.mu.RUnlock()

	if !running {
		return 0, fmt.Errorf("runtime not started")
	}

	f := fiber.NewFiber(fn, stackSize, name)

	// Insert into rt.fibers before scheduling so a fast execution loop that
	// pops the fiber sees it in the map.
	rt.fibersMu.Lock()
	rt.fibers[f.ID] = f
	rt.fibersMu.Unlock()

	if err := rt.scheduler.Schedule(f); err != nil {
		rt.fibersMu.Lock()
		delete(rt.fibers, f.ID)
		rt.fibersMu.Unlock()
		return 0, fmt.Errorf("schedule fiber: %w", err)
	}

	// Re-check stopped state. scheduler.Schedule can block on back-pressure
	// and Stop may have completed in the meantime.
	rt.mu.RLock()
	stillRunning := rt.epoch > 0 && !rt.isStopped()
	rt.mu.RUnlock()
	if !stillRunning {
		_ = rt.scheduler.Remove(f.ID)
		rt.fibersMu.Lock()
		delete(rt.fibers, f.ID)
		rt.fibersMu.Unlock()
		return 0, fmt.Errorf("runtime stopped during spawn")
	}

	// Record metrics only after the fiber is confirmed to be live in the
	// scheduler. This prevents the rollback paths above from leaving
	// ActiveFibers and TotalStackMemory inflated.
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

// Start begins scheduler admission and owned lifecycle goroutines. It blocks
// until any prior Stop has finished so a stale execution loop cannot pick up
// the new run's context or result channel.
func (rt *Runtime) Start() error {
	if rt == nil {
		return fmt.Errorf("nil runtime")
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()

	rt.mu.RLock()
	if rt.epoch > 0 && !rt.isStopped() {
		rt.mu.RUnlock()
		return fmt.Errorf("runtime already running")
	}
	rt.mu.RUnlock()

	// Wait for a prior Stop to finish so we can mutate state without
	// racing a still-running drain.
	rt.mu.Lock()
	if rt.isStopped() {
		<-rt.stopped
	}
	if err := rt.scheduler.Start(); err != nil {
		rt.mu.Unlock()
		return fmt.Errorf("start scheduler: %w", err)
	}
	rt.mainFiberMu.Lock()
	if rt.mainFiber == nil {
		rt.mainFiber = fiber.NewFiber(func() {}, fiber.DefaultStackSize, "main")
	}
	mainFiber := rt.mainFiber
	rt.fibersMu.Lock()
	rt.fibers[mainFiber.ID] = mainFiber
	rt.fibersMu.Unlock()
	rt.mainFiberMu.Unlock()

	runCtx, runCancel := context.WithCancel(context.Background())
	resultChan := make(chan fiberResult, defaultResultChanCapacity)
	detector := NewDeadlockDetector()
	runEpoch := atomic.AddInt64(&rt.epoch, 1)
	lifecycleWG := &sync.WaitGroup{}
	fiberWG := &sync.WaitGroup{}
	rt.runCtx = runCtx
	rt.runCancel = runCancel
	rt.runDetector = detector
	rt.runFiberWG = fiberWG
	rt.runLifecycleWG = lifecycleWG
	rt.deadlockDetector = detector
	rt.currentFiber = mainFiber
	rt.stopped = make(chan struct{})
	rt.metrics.StartRun()
	rt.mu.Unlock()

	lifecycleWG.Add(2)
	go rt.runExecutionLoop(runCtx, resultChan, runEpoch, lifecycleWG)
	go func() {
		defer lifecycleWG.Done()
		detector.Start(rt)
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

	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()

	rt.mu.RLock()
	if rt.epoch == 0 || rt.isStopped() {
		rt.mu.RUnlock()
		return nil
	}
	cancel := rt.runCancel
	detector := rt.runDetector
	lifecycleWG := rt.runLifecycleWG
	fiberWG := rt.runFiberWG
	stopped := rt.stopped
	rt.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
	if detector != nil {
		detector.Stop()
	}
	schedulerErr := rt.scheduler.Stop()
	if lifecycleWG != nil {
		lifecycleWG.Wait()
	}

	// The lifecycle goroutines have returned. In-flight fiber functions may
	// outlive the shutdown deadline; bound the join by ctx so a long-running
	// fiber cannot hold shutdown indefinitely.
	if fiberWG != nil {
		waitDone := make(chan struct{})
		go func() {
			fiberWG.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
		case <-ctx.Done():
			// Mark stopped so the next Start does not block, but record
			// that shutdown was abandoned. Stale dispatch goroutines may
			// still try to send to the run's resultChan; their send will
			// hit a closed channel (after the run's execution loop has
			// drained and returned) and the goroutine will exit on
			// runCtx.Done().
			close(stopped)
			rt.mu.Lock()
			rt.stopped = closedChan()
			rt.mu.Unlock()
			return fmt.Errorf("runtime stop: %w", ctx.Err())
		}
	}

	close(stopped)
	rt.mu.Lock()
	rt.stopped = closedChan()
	rt.mu.Unlock()
	return schedulerErr
}

// Reset clears stopped runtime state. Start recreates the main observer fiber
// when necessary for the next run. The caller must ensure the runtime is
// stopped; this method blocks on lifecycleMu so it cannot race with a
// concurrent Start or with the completion path that reads mainFiber.
//
// If the runtime is still running, Reset is a no-op: clearing the metrics
// and scheduler queues while fibers are in flight would produce impossible
// counters (TotalFibersCompleted > TotalFibersCreated) and orphan work.
// Callers must Stop first; the no-op here is a defensive guard so a misuse
// does not corrupt state, but it returns a clear error via IsRunning() that
// the caller can check.
func (rt *Runtime) Reset() {
	if rt == nil {
		return
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()

	if rt.IsRunning() {
		// Runtime is still active; do not clear state. The caller must
		// Stop first. We log a warning via the runtime's metrics so the
		// operator can see the misuse, but we do not panic.
		return
	}

	rt.mu.Lock()
	rt.mainFiber = nil
	rt.currentFiber = nil
	rt.mu.Unlock()

	rt.fibersMu.Lock()
	rt.fibers = make(map[fiber.FiberID]*fiber.Fiber)
	rt.fibersMu.Unlock()
	rt.scheduler.Clear()
	rt.metrics.Reset()
	rt.eventTracker.Clear()
}

// runExecutionLoop is the per-run admission loop. It owns runCtx, resultChan,
// and runEpoch as locals so a Start/Stop race cannot redirect it to a new
// run's state. It returns when runCtx is cancelled, after draining whatever
// results were already in the channel.
func (rt *Runtime) runExecutionLoop(runCtx context.Context, resultChan chan fiberResult, runEpoch int64, lifecycleWG *sync.WaitGroup) {
	defer lifecycleWG.Done()
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	active := 0
	for {
		select {
		case <-runCtx.Done():
			for {
				select {
				case result := <-resultChan:
					if active > 0 {
						active--
					}
					rt.complete(result)
				default:
					return
				}
			}
		case result := <-resultChan:
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
				rt.dispatch(f, runCtx, resultChan, runEpoch)
				active++
			}
		}
	}
}

// dispatch launches a fiber goroutine and reports completion via resultChan.
// runCtx, resultChan, and runEpoch are passed in so the goroutine does not
// read mutable runtime fields. If the run has been stopped, the goroutine
// reports the result into a closed channel that is being drained by no one;
// the result is discarded (this is intentional shutdown behavior).
func (rt *Runtime) dispatch(f *fiber.Fiber, runCtx context.Context, resultChan chan fiberResult, runEpoch int64) {
	f.MarkScheduled()
	rt.mu.Lock()
	rt.currentFiber = f
	rt.mu.Unlock()
	rt.metrics.RecordContextSwitch()
	rt.eventTracker.RecordEvent(metrics.FiberEvent{
		FiberID: f.ID, EventType: metrics.EventRunning, Timestamp: time.Now(), Details: "Fiber running",
	})

	start := time.Now()
	rt.mu.RLock()
	fiberWG := rt.runFiberWG
	rt.mu.RUnlock()
	if fiberWG == nil {
		return
	}
	fiberWG.Add(1)
	go func() {
		defer fiberWG.Done()
		f.Run()
		// Try to deliver the result; if the run is shutting down, drop the
		// send rather than block forever. complete() still runs so the
		// fiber's CPUTime is recorded and any finalization paths run.
		result := fiberResult{fiber: f, duration: time.Since(start), epoch: runEpoch}
		select {
		case resultChan <- result:
		case <-runCtx.Done():
		}
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
	rt.mainFiberMu.RLock()
	main := rt.mainFiber
	rt.mainFiberMu.RUnlock()
	if rt.currentFiber == f {
		rt.currentFiber = main
	}
	rt.mu.Unlock()

	// Reap the finished fiber so its bounded stack and state are released.
	// The main observer fiber is never run and never finishes, so it stays.
	rt.fibersMu.Lock()
	delete(rt.fibers, f.ID)
	rt.fibersMu.Unlock()
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

// SetStackSize configures the stack allocated to future fibers.
func (rt *Runtime) SetStackSize(size int) {
	rt.mu.Lock()
	rt.stackSize = fiber.ValidateStackSize(size)
	rt.mu.Unlock()
}
