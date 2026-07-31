package runtime

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

const maxDeadlockHistory = 100

// DeadlockDetector records transitions into and out of a no-progress state.
// It reports suspicion, not proof: the runtime does not expose a complete
// wait-for graph for arbitrary user synchronization objects.
type DeadlockDetector struct {
	enabled        bool
	checkInterval  time.Duration
	timeout        time.Duration
	stopChan       chan struct{}
	stopOnce       sync.Once
	mu             sync.RWMutex
	started        bool
	lastProgress   time.Time
	deadlockActive bool
	deadlocks      []DeadlockInfo
}

// DeadlockInfo is an immutable-by-convention snapshot of one detected state.
type DeadlockInfo struct {
	DetectedAt    time.Time
	BlockedFibers []*fiber.Fiber
	Description   string
	Resolved      bool
}

// NewDeadlockDetector creates a detector with conservative defaults.
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		enabled:       true,
		checkInterval: time.Second,
		timeout:       5 * time.Second,
		stopChan:      make(chan struct{}),
		lastProgress:  time.Now(),
		deadlocks:     make([]DeadlockInfo, 0),
	}
}

// Start runs the detector loop in the caller-owned goroutine until Stop. It
// does not create an unowned nested goroutine.
func (dd *DeadlockDetector) Start(rt *Runtime) {
	if dd == nil {
		return
	}
	dd.mu.Lock()
	if !dd.enabled || dd.started {
		dd.mu.Unlock()
		return
	}
	dd.started = true
	interval := dd.checkInterval
	if interval <= 0 {
		interval = time.Second
	}
	dd.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-dd.stopChan:
			return
		case <-ticker.C:
			dd.checkForDeadlock(rt)
		}
	}
}

// Stop is idempotent and wakes a running detector loop.
func (dd *DeadlockDetector) Stop() {
	if dd == nil {
		return
	}
	dd.stopOnce.Do(func() { close(dd.stopChan) })
}

func (dd *DeadlockDetector) checkForDeadlock(rt *Runtime) {
	if rt == nil || !rt.IsRunning() {
		return
	}
	// The observer main fiber is never admitted for execution; it sits in
	// StateReady for the lifetime of the runtime. If we counted it as
	// runnable, every runtime would look partially unblocked forever and
	// no deadlock suspicion would ever fire. Exclude it explicitly.
	mainID := fiber.FiberID(0)
	rt.mainFiberMu.RLock()
	if rt.mainFiber != nil {
		mainID = rt.mainFiber.ID
	}
	rt.mainFiberMu.RUnlock()
	fibers := rt.GetAllFibers()
	blocked := make([]*fiber.Fiber, 0)
	ready := 0
	running := 0
	for _, f := range fibers {
		if f.ID == mainID {
			continue
		}
		switch {
		case f.IsBlocked():
			blocked = append(blocked, f)
		case f.State == fiber.StateRunning:
			running++
		case f.IsRunnable():
			ready++
		}
	}
	// Determine whether the system can still make progress. A Running fiber will
	// eventually finish and free its worker slot, so any running fiber counts as
	// progress. When nothing is running, a Ready fiber can only advance if a
	// worker slot is free to dispatch it into. Under the non-preemptive model a
	// Blocked fiber holds its worker slot until another fiber unblocks it, so
	// once every slot is occupied by a blocked fiber, waiting Ready fibers can
	// never be dispatched either — the slot-exhaustion deadlock. The previous
	// check counted any Ready fiber as progress and was therefore blind to that
	// exact case (F3): the queued-but-undispatchable fiber masked the hang.
	now := time.Now()
	slotsAllBlocked := rt.numWorkers > 0 && len(blocked) >= rt.numWorkers
	noRunnable := running == 0 && len(blocked) > 0 && (ready == 0 || slotsAllBlocked)

	dd.mu.Lock()
	defer dd.mu.Unlock()
	if !noRunnable {
		dd.lastProgress = now
	}
	// Update the blocked-fiber gauge from the authoritative fiber scan so
	// metrics reflect current state even though sync primitives do not call
	// back into the runtime.
	if rt.metrics != nil {
		rt.metrics.SetBlockedFibers(int64(len(blocked)))
	}
	deadlocked := noRunnable && now.Sub(dd.lastProgress) >= dd.timeout
	if deadlocked && !dd.deadlockActive {
		dd.deadlockActive = true
		dd.deadlocks = append(dd.deadlocks, DeadlockInfo{
			DetectedAt:    now,
			BlockedFibers: cloneFibers(blocked),
			Description:   fmt.Sprintf("all %d fibers blocked for %s", len(blocked), now.Sub(dd.lastProgress).Round(time.Second)),
		})
		if len(dd.deadlocks) > maxDeadlockHistory {
			dd.deadlocks = dd.deadlocks[len(dd.deadlocks)-maxDeadlockHistory:]
		}
	} else if !noRunnable && dd.deadlockActive {
		dd.deadlockActive = false
		dd.lastProgress = now
		dd.deadlocks = append(dd.deadlocks, DeadlockInfo{
			DetectedAt:  now,
			Description: "previous blocked state resolved",
			Resolved:    true,
		})
		if len(dd.deadlocks) > maxDeadlockHistory {
			dd.deadlocks = dd.deadlocks[len(dd.deadlocks)-maxDeadlockHistory:]
		}
	}
}

func cloneFibers(in []*fiber.Fiber) []*fiber.Fiber {
	out := make([]*fiber.Fiber, len(in))
	for i, f := range in {
		out[i] = f.Clone()
	}
	return out
}

// GetDeadlocks returns independent snapshots of detector history.
func (dd *DeadlockDetector) GetDeadlocks() []DeadlockInfo {
	if dd == nil {
		return []DeadlockInfo{}
	}
	dd.mu.RLock()
	defer dd.mu.RUnlock()
	out := make([]DeadlockInfo, len(dd.deadlocks))
	for i, info := range dd.deadlocks {
		out[i] = info
		out[i].BlockedFibers = cloneFibers(info.BlockedFibers)
	}
	return out
}

// ClearDeadlocks clears detector history and active state.
func (dd *DeadlockDetector) ClearDeadlocks() {
	if dd == nil {
		return
	}
	dd.mu.Lock()
	dd.deadlocks = nil
	dd.deadlockActive = false
	dd.mu.Unlock()
}

// SetEnabled controls whether a future Start runs detection.
func (dd *DeadlockDetector) SetEnabled(enabled bool) {
	if dd == nil {
		return
	}
	dd.mu.Lock()
	dd.enabled = enabled
	dd.mu.Unlock()
}

// IsEnabled reports detector configuration.
func (dd *DeadlockDetector) IsEnabled() bool {
	if dd == nil {
		return false
	}
	dd.mu.RLock()
	defer dd.mu.RUnlock()
	return dd.enabled
}

// SetCheckInterval configures the interval for the next Start.
func (dd *DeadlockDetector) SetCheckInterval(interval time.Duration) {
	if dd == nil {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	dd.mu.Lock()
	dd.checkInterval = interval
	dd.mu.Unlock()
}

// SetTimeout configures the diagnostic long-block threshold.
func (dd *DeadlockDetector) SetTimeout(timeout time.Duration) {
	if dd == nil {
		return
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	dd.mu.Lock()
	dd.timeout = timeout
	dd.mu.Unlock()
}
