// Package runtime owns fiber admission, lifecycle, snapshots, and diagnostics.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/metrics"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// Sentinel errors returned by Runtime methods. Callers may test with errors.Is.
var (
	ErrNotStarted         = errors.New("runtime not started")
	ErrAlreadyRunning     = errors.New("runtime already running")
	ErrStoppedDuringSpawn = errors.New("runtime stopped during spawn")
	ErrNilRuntime         = errors.New("nil runtime")
	// ErrMaxFibersReached is returned by Spawn when the live fiber count is at
	// the WithMaxFibers cap. It is enforced atomically (a counter CAS), so the
	// cap holds exactly even under concurrent spawners — callers should test
	// with errors.Is rather than pre-checking a gauge.
	ErrMaxFibersReached = errors.New("fiber cap exceeded")
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

	fibers map[fiber.FiberID]*fiber.Fiber
	// admitted holds the IDs of fibers whose Spawn passed the stopped re-check
	// (i.e. RecordFiberCreated ran and activeFiberCount is committed to them).
	// It disambiguates a fully-admitted Ready fiber (owned by complete()/
	// reapPending) from a fiber still mid-Spawn that its own rollback will
	// decrement — without it, Stop's reap and Spawn's abort race to double-
	// decrement activeFiberCount. Guarded by fibersMu.
	admitted map[fiber.FiberID]struct{}
	// draining is set true by reapPending (under fibersMu) once Stop begins its
	// reap, and gates Spawn's admission commit so a fiber cannot be committed
	// after the reap scan has already passed it (which would leak the counter).
	// Cleared when a new run starts and on Reset. Guarded by fibersMu.
	draining    bool
	fibersMu    sync.RWMutex
	mainFiberMu sync.RWMutex
	mainFiber   *fiber.Fiber

	stackSize  int
	numWorkers int
	maxFibers  int64 // 0 means unlimited; enforced atomically via activeFiberCount

	activeFiberCount int64 // accessed via atomic; mirrors metrics.ActiveFibers but cap-enforced

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
	currentFiber   *fiber.Fiber
	_              [4]byte // padding to align epoch to 8 bytes on 32-bit
	epoch          int64   // incremented on each Start
	runCtx         context.Context
	runCancel      context.CancelFunc
	runDetector    *DeadlockDetector
	runFiberWG     *sync.WaitGroup
	runLifecycleWG *sync.WaitGroup
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
		admitted:         make(map[fiber.FiberID]struct{}),
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
// The re-check uses runCtx.Done() so it gates on Stop being initiated, not
// completed — a fiber whose Spawn races against Stop.cancel() will fail here.
func (rt *Runtime) Spawn(fn fiber.FiberFunc, name string) (fiber.FiberID, error) {
	if rt == nil {
		return 0, fmt.Errorf("%w", ErrNilRuntime)
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
		return 0, ErrNotStarted
	}

	// Atomically check and enforce the fiber cap before allocating state.
	newCount := atomic.AddInt64(&rt.activeFiberCount, 1)
	if rt.maxFibers > 0 && newCount > rt.maxFibers {
		atomic.AddInt64(&rt.activeFiberCount, -1)
		return 0, fmt.Errorf("%w: limit %d", ErrMaxFibersReached, rt.maxFibers)
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
		atomic.AddInt64(&rt.activeFiberCount, -1)
		return 0, fmt.Errorf("schedule fiber: %w", err)
	}

	// Re-check stopped state. scheduler.Schedule can block on back-pressure and
	// Stop may have begun in the meantime.
	rt.mu.RLock()
	runCtx := rt.runCtx
	rt.mu.RUnlock()
	stopping := false
	if runCtx != nil {
		select {
		case <-runCtx.Done():
			stopping = true
		default:
		}
	}

	// Commit admission (or roll back) under fibersMu, atomically ordered against
	// Stop's reap. reapPending sets rt.draining under this same lock before it
	// scans, so every fiber is either committed-before-the-flag (visible to the
	// scan and reaped exactly once) or blocked-by-the-flag (rolled back exactly
	// once). Without this ordering a Spawn could commit AFTER reapPending scanned
	// and leak activeFiberCount permanently. Marking admitted only on commit lets
	// the reap distinguish this fiber from one still mid-Spawn.
	rt.fibersMu.Lock()
	if stopping || rt.draining {
		// The fiber may already have been dispatched and completed in the
		// Schedule -> re-check window (see the fast-loop note above), in which
		// case complete() already removed it and owns the decrement. Only the
		// path that actually removes f from rt.fibers decrements the counter, so
		// exactly one of {complete, reapPending, this rollback} does — never two.
		_, present := rt.fibers[f.ID]
		delete(rt.fibers, f.ID)
		delete(rt.admitted, f.ID)
		rt.fibersMu.Unlock()
		_ = rt.scheduler.Remove(f.ID)
		if present {
			atomic.AddInt64(&rt.activeFiberCount, -1)
		}
		return 0, ErrStoppedDuringSpawn
	}
	rt.admitted[f.ID] = struct{}{}
	rt.fibersMu.Unlock()

	// Record metrics only after the fiber is confirmed to be admitted. This
	// prevents the rollback paths above from leaving ActiveFibers and
	// TotalStackMemory inflated.
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
	return rt.StartWithContext(context.Background())
}

// StartWithContext is like Start but derives the run context from ctx. When ctx
// is cancelled the runtime behaves as if Stop were called with a background
// context, allowing callers to tie runtime lifetime to an outer context.
func (rt *Runtime) StartWithContext(ctx context.Context) error {
	if rt == nil {
		return fmt.Errorf("%w", ErrNilRuntime)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()

	rt.mu.RLock()
	if rt.epoch > 0 && !rt.isStopped() {
		rt.mu.RUnlock()
		return ErrAlreadyRunning
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
	// Clear the drain gate so the new run can admit fibers again.
	rt.draining = false
	rt.fibersMu.Unlock()
	rt.mainFiberMu.Unlock()

	runCtx, runCancel := context.WithCancel(ctx)
	resultChan := make(chan fiberResult, defaultResultChanCapacity)
	detector := NewDeadlockDetector()
	// Preserve configuration from prior detector so that SetEnabled,
	// SetCheckInterval, and SetTimeout survive a Stop/Start cycle.
	if rt.deadlockDetector != nil {
		rt.deadlockDetector.mu.RLock()
		detector.enabled = rt.deadlockDetector.enabled
		detector.checkInterval = rt.deadlockDetector.checkInterval
		detector.timeout = rt.deadlockDetector.timeout
		rt.deadlockDetector.mu.RUnlock()
	}
	rt.epoch++ // rt.mu write lock is held from line ~293 to ~335; no atomic needed
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
	go rt.runExecutionLoop(runCtx, resultChan, lifecycleWG)
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

	// The lifecycle goroutines have returned, so the execution loop is no
	// longer dispatching and the scheduler queues are stable. Reap fibers that
	// were admitted but never dispatched before Stop: clear the scheduler so
	// they cannot execute in a subsequent Start (F1), and reverse their
	// admission bookkeeping so WithMaxFibers and ActiveFibers are not leaked
	// across Stop/Reset cycles (F2). This runs before the in-flight join below
	// so it also cleans up when the join times out.
	rt.mainFiberMu.RLock()
	mainFiber := rt.mainFiber
	rt.mainFiberMu.RUnlock()
	var mainID fiber.FiberID
	if mainFiber != nil {
		mainID = mainFiber.ID
	}
	rt.reapPending(mainID)

	// In-flight fiber functions may outlive the shutdown deadline; bound the
	// join by ctx so a long-running fiber cannot hold shutdown indefinitely.
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

// reapPending discards fibers that were admitted (Spawn incremented the cap
// counter, the active-fiber metric, and inserted them into rt.fibers) but were
// never dispatched before the run stopped. It must be called only after the
// execution loop has returned, so no fiber can transition out of Ready
// concurrently.
//
// A fiber still in StateReady at this point was never dispatched: no dispatch
// goroutine exists for it, so its complete() will never run and it would
// otherwise (a) survive in the scheduler queue and execute in the next run, and
// (b) leak activeFiberCount forever. In-flight fibers are Running or Blocked (or
// already removed by complete()); they are left untouched and finish via their
// own complete() call, which keeps the accounting exactly-once.
func (rt *Runtime) reapPending(mainID fiber.FiberID) {
	// Empty the scheduler queues so no un-dispatched fiber survives into a
	// subsequent Start.
	rt.scheduler.Clear()

	rt.fibersMu.Lock()
	// Setting draining under the same lock that guards the admission commit,
	// and BEFORE the scan below, orders every Spawn commit against this reap:
	// a commit either lands before this and is visible to the scan, or lands
	// after and is refused by Spawn's draining check. Exactly-once in both
	// directions.
	rt.draining = true
	stackSizes := make([]int, 0)
	for id, f := range rt.fibers {
		if id == mainID {
			continue
		}
		// Only reap fibers whose admission is committed. A fiber that is Ready
		// but NOT in `admitted` is still mid-Spawn; its own rollback owns the
		// activeFiberCount decrement, and reaping it here would double-count
		// (drives the counter negative — see the Spawn/Stop race). Deleting the
		// admitted entry under the same lock claims the decrement exactly once.
		if _, ok := rt.admitted[id]; !ok {
			continue
		}
		if f.IsRunnable() {
			stackSizes = append(stackSizes, f.StackSize)
			delete(rt.fibers, id)
			delete(rt.admitted, id)
		}
	}
	rt.fibersMu.Unlock()

	// Reverse admission bookkeeping outside fibersMu (metrics take their own
	// lock). Each reaped fiber decrements the cap counter exactly once.
	for _, ss := range stackSizes {
		atomic.AddInt64(&rt.activeFiberCount, -1)
		rt.metrics.RecordFiberCancelled(ss)
	}
}

// Reset clears stopped runtime state. Start recreates the main observer fiber
// when necessary for the next run. The caller must ensure the runtime is
// stopped; this method blocks on lifecycleMu so it cannot race with a
// concurrent Start or with the completion path that reads mainFiber.
//
// If the runtime is still running, Reset returns ErrAlreadyRunning. Clearing
// the metrics and scheduler queues while fibers are in flight would produce
// impossible counters (TotalFibersCompleted > TotalFibersCreated) and orphan
// work. Callers must Stop first.
func (rt *Runtime) Reset() error {
	if rt == nil {
		return nil
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()

	// isStopped reads rt.stopped; hold rt.mu consistently with all other callers.
	rt.mu.RLock()
	running := rt.epoch > 0 && !rt.isStopped()
	rt.mu.RUnlock()
	if running {
		return ErrAlreadyRunning
	}

	rt.mainFiberMu.Lock()
	rt.mainFiber = nil
	rt.mainFiberMu.Unlock()
	rt.mu.Lock()
	rt.currentFiber = nil
	rt.mu.Unlock()

	rt.fibersMu.Lock()
	rt.fibers = make(map[fiber.FiberID]*fiber.Fiber)
	rt.admitted = make(map[fiber.FiberID]struct{})
	rt.draining = false
	rt.fibersMu.Unlock()
	rt.scheduler.Clear()
	rt.metrics.Reset()
	rt.eventTracker.Clear()
	return nil
}

// runExecutionLoop is the per-run admission loop. It owns runCtx and resultChan
// as locals so a Start/Stop race cannot redirect it to a new run's state. It
// returns when runCtx is cancelled, after draining whatever results were already
// in the channel.
func (rt *Runtime) runExecutionLoop(runCtx context.Context, resultChan chan fiberResult, lifecycleWG *sync.WaitGroup) {
	defer lifecycleWG.Done()
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	active := 0
	for {
		select {
		case <-runCtx.Done():
			for {
				select {
				case <-resultChan:
					if active > 0 {
						active--
					}
				default:
					return
				}
			}
		case result := <-resultChan:
			if active > 0 {
				active--
			}
			_ = result // complete() already ran in dispatch goroutine
		case <-ticker.C:
			for active < rt.numWorkers {
				f, err := rt.scheduler.Next()
				if err != nil {
					break
				}
				if f == nil || !f.IsRunnable() {
					continue
				}
				rt.dispatch(f, runCtx, resultChan)
				active++
				// ShouldPreempt() is available via the Scheduler interface for callers
				// implementing cooperative yield. The runtime execution loop does not
				// call it because goroutine preemption requires explicit fiber cooperation
				// (see Runtime.Yield which is not yet implemented).
			}
		}
	}
}

// dispatch launches a fiber goroutine and reports completion via resultChan.
// runCtx and resultChan are passed in so the goroutine does not read mutable
// runtime fields. If the run has been stopped, the goroutine reports the result
// into a closed channel that is being drained by no one; the result is discarded
// (this is intentional shutdown behavior).
func (rt *Runtime) dispatch(f *fiber.Fiber, runCtx context.Context, resultChan chan fiberResult) {
	f.MarkScheduled()
	// currentFiber is not updated per-dispatch: with numWorkers > 1, multiple
	// fibers execute concurrently and "current fiber" is not a meaningful concept.
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
		result := fiberResult{fiber: f, duration: time.Since(start)}
		rt.complete(result)
		// complete() is called unconditionally above; this send signals the
		// execution loop to decrement its active counter. On shutdown, the
		// signal may be dropped — the active count no longer matters after
		// runCtx is cancelled.
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
		// Count the panic so greenthreads_fiber_panics_total reflects reality.
		rt.metrics.RecordFiberPanic()
		// Write to stderr so operators see panics in server logs.
		// A proper logger is not available here; use the event tracker and stderr.
		fmt.Fprintf(os.Stderr, "greenthreads: fiber %d (%s) panicked: %v\npanic stack:\n%s\n",
			f.ID, f.Name, err, f.PanicStack())
	}
	rt.eventTracker.RecordEvent(metrics.FiberEvent{
		FiberID: f.ID, EventType: metrics.EventCompleted, Timestamp: time.Now(), Details: details,
	})

	// Reap the finished fiber so its bounded stack and state are released.
	// The main observer fiber is never run and never finishes, so it stays.
	// Only the path that actually removes f from rt.fibers decrements the
	// counter: if a concurrent Spawn-rollback (during Stop) already removed this
	// fiber, it owns the decrement and we must not double-count.
	rt.fibersMu.Lock()
	_, present := rt.fibers[f.ID]
	delete(rt.fibers, f.ID)
	delete(rt.admitted, f.ID)
	rt.fibersMu.Unlock()
	if present {
		atomic.AddInt64(&rt.activeFiberCount, -1)
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

// DeadlockDetector returns the runtime's deadlock detector, used to enable
// detection and query suspected deadlocks. The returned detector is nil-safe:
// its methods may be called even when this returns nil (before the first
// Start). The current detector is replaced on each Start, preserving the
// prior enabled/interval/timeout configuration.
func (rt *Runtime) DeadlockDetector() *DeadlockDetector {
	if rt == nil {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.deadlockDetector
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

// GetMetrics returns a consistent metrics snapshot for the current run.
// Counters reset to zero after each Reset() call. For Prometheus-compatible
// monotonically-increasing counters use GetLifetimeMetrics.
func (rt *Runtime) GetMetrics() metrics.MetricsSnapshot {
	snap := rt.metrics.GetSnapshot()
	rt.injectStealStats(&snap)
	rt.injectActiveCount(&snap)
	return snap
}

// injectActiveCount overwrites the ActiveFibers gauge with the authoritative
// admission counter. metrics.ActiveFibers is maintained by RecordFiberCreated/
// Completed/Cancelled, whose ordering can drift by a bounded amount in the rare
// fast-loop-during-Spawn race. activeFiberCount is decremented exactly once per
// fiber (verified) so it is the exact live count; use it directly.
func (rt *Runtime) injectActiveCount(snap *metrics.MetricsSnapshot) {
	n := atomic.LoadInt64(&rt.activeFiberCount)
	if n < 0 {
		n = 0
	}
	snap.ActiveFibers = n
}

// GetLifetimeMetrics returns a snapshot where the five monotonic counters
// (FibersCreated, FibersCompleted, ContextSwitches, Yields, ScheduleCalls)
// are cumulative across all Reset() calls — suitable for Prometheus exposition
// so counters never decrease between scrapes (OBS-2).
func (rt *Runtime) GetLifetimeMetrics() metrics.MetricsSnapshot {
	snap := rt.metrics.GetLifetimeSnapshot()
	rt.injectStealStats(&snap)
	rt.injectActiveCount(&snap)
	return snap
}

// injectStealStats overwrites the steal counters in snap with live values
// from the work-stealing scheduler when it is active.
func (rt *Runtime) injectStealStats(snap *metrics.MetricsSnapshot) {
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
