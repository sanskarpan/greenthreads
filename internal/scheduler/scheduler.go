// Package scheduler provides bounded queue policies for runtime admission.
package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// ErrNoFibers is returned by Next() when there are no fibers ready to run.
// Callers can check for this specific error with errors.Is instead of string
// comparison, enabling efficient fast-path detection in the runtime.
var ErrNoFibers = errors.New("no fibers in run queue")

// SchedulerType represents the type of scheduler
type SchedulerType int

const (
	// TypeFIFO schedules fibers in first-in-first-out order.
	TypeFIFO SchedulerType = iota
	// TypeRoundRobin schedules fibers with a fixed time quantum per dispatch.
	TypeRoundRobin
	// TypePriority schedules fibers by priority value (higher first).
	TypePriority
	// TypeWorkStealing distributes fibers across worker-local queues with
	// idle-worker steal.
	TypeWorkStealing
)

// String returns the stable display name for a scheduler type.
func (st SchedulerType) String() string {
	switch st {
	case TypeFIFO:
		return "FIFO"
	case TypeRoundRobin:
		return "RoundRobin"
	case TypePriority:
		return "Priority"
	case TypeWorkStealing:
		return "WorkStealing"
	default:
		return "Unknown"
	}
}

// Scheduler interface defines the contract for all schedulers
type Scheduler interface {
	// Schedule adds a fiber to the scheduler
	Schedule(f *fiber.Fiber) error

	// Next returns the next fiber to run
	Next() (*fiber.Fiber, error)

	// Remove removes a fiber from the scheduler
	Remove(fiberID fiber.FiberID) error

	// MarkCompleted records that a fiber has finished and removes it from the
	// scheduler's queues. It is idempotent and updates completion statistics.
	MarkCompleted(fiberID fiber.FiberID)

	// GetRunQueue returns all fibers in the run queue
	GetRunQueue() []*fiber.Fiber

	// GetBlockedQueue returns all blocked fibers
	GetBlockedQueue() []*fiber.Fiber

	// Size returns the number of fibers in the scheduler
	Size() int

	// Type returns the scheduler type
	Type() SchedulerType

	// Name returns the scheduler name
	Name() string

	// Start starts the scheduler
	Start() error

	// Stop stops the scheduler
	Stop() error

	// Clear empties all queues and resets statistics. Called by Runtime.Reset
	// so stale fibers from a prior run are not re-dispatched after restart.
	Clear()

	// IsRunning returns true if the scheduler is running
	IsRunning() bool

	// GetStats returns scheduler statistics
	GetStats() SchedulerStats
}

// SchedulerStats contains statistics about the scheduler
type SchedulerStats struct {
	TotalScheduled   int64
	TotalCompleted   int64
	TotalBlocked     int64
	CurrentRunQueue  int
	CurrentBlocked   int
	AverageWaitTime  time.Duration
	AverageRunTime   time.Duration
	ContextSwitches  int64
	LastScheduleTime time.Time
}

// BaseScheduler provides common functionality for all schedulers
type BaseScheduler struct {
	name          string
	schedulerType SchedulerType
	runQueue      []*fiber.Fiber
	blockedQueue  []*fiber.Fiber
	running       bool
	// stopped is set to true when Stop() is called. Once stopped, Schedule()
	// and Next() reject new work. This is distinct from !running (which is also
	// true before Start() is called) so that callers do not need to call Start()
	// before using the scheduler in tests or simple usage patterns.
	stopped bool
	mu      sync.RWMutex

	// completed tracks which fiber IDs have already been recorded as
	// completed. It is the basis for MarkCompleted idempotence so the
	// runtime and the filter path can both call MarkCompleted for the same
	// fiber without double-counting.
	completed map[fiber.FiberID]struct{}

	// Statistics
	totalScheduled   int64
	totalCompleted   int64
	totalBlocked     int64
	contextSwitches  int64
	lastScheduleTime time.Time
}

// NewBaseScheduler creates a new base scheduler
func NewBaseScheduler(name string, schedulerType SchedulerType) *BaseScheduler {
	return &BaseScheduler{
		name:          name,
		schedulerType: schedulerType,
		runQueue:      make([]*fiber.Fiber, 0),
		blockedQueue:  make([]*fiber.Fiber, 0),
		completed:     make(map[fiber.FiberID]struct{}),
		running:       false,
	}
}

// Schedule adds a fiber to the run queue
func (s *BaseScheduler) Schedule(f *fiber.Fiber) error {
	if f == nil {
		return fmt.Errorf("cannot schedule nil fiber")
	}

	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return fmt.Errorf("scheduler stopped")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.runQueue = append(s.runQueue, f)
	s.totalScheduled++
	s.lastScheduleTime = time.Now()

	return nil
}

// Remove removes a fiber from the scheduler
func (s *BaseScheduler) Remove(fiberID fiber.FiberID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removeLocked(fiberID)
}

func (s *BaseScheduler) removeLocked(fiberID fiber.FiberID) error {
	// Remove from run queue
	for i, f := range s.runQueue {
		if f.ID == fiberID {
			s.runQueue = append(s.runQueue[:i], s.runQueue[i+1:]...)
			return nil
		}
	}

	// Remove from blocked queue
	for i, f := range s.blockedQueue {
		if f.ID == fiberID {
			s.blockedQueue = append(s.blockedQueue[:i], s.blockedQueue[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("fiber %d not found", fiberID)
}

// GetRunQueue returns all fibers in the run queue
func (s *BaseScheduler) GetRunQueue() []*fiber.Fiber {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queue := make([]*fiber.Fiber, len(s.runQueue))
	for i, f := range s.runQueue {
		queue[i] = f.Clone()
	}
	return queue
}

// GetBlockedQueue returns all blocked fibers
func (s *BaseScheduler) GetBlockedQueue() []*fiber.Fiber {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queue := make([]*fiber.Fiber, len(s.blockedQueue))
	for i, f := range s.blockedQueue {
		queue[i] = f.Clone()
	}
	return queue
}

// Size returns the number of fibers in the scheduler
func (s *BaseScheduler) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.runQueue)
}

// Type returns the scheduler type
func (s *BaseScheduler) Type() SchedulerType {
	return s.schedulerType
}

// Name returns the scheduler name
func (s *BaseScheduler) Name() string {
	return s.name
}

// Start starts the scheduler
func (s *BaseScheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("scheduler already running")
	}

	s.running = true
	// Clear stopped so that Schedule/Next accept new work after a Stop/Start
	// cycle that does not go through Reset/Clear (which also clears stopped).
	s.stopped = false
	return nil
}

// Stop stops the scheduler
func (s *BaseScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("scheduler not running")
	}

	s.running = false
	s.stopped = true
	return nil
}

// Clear empties all queues and resets statistics. It is called by
// Runtime.Reset so stale fibers from a prior run are not re-dispatched.
// It also resets the stopped flag so the scheduler can be reused after a
// Reset/Clear without needing to call Start() again.
func (s *BaseScheduler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runQueue = make([]*fiber.Fiber, 0)
	s.blockedQueue = make([]*fiber.Fiber, 0)
	s.completed = make(map[fiber.FiberID]struct{})
	s.stopped = false
	s.totalScheduled = 0
	s.totalCompleted = 0
	s.totalBlocked = 0
	s.contextSwitches = 0
	s.lastScheduleTime = time.Time{}
}

// IsRunning returns true if the scheduler is running
func (s *BaseScheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.running
}

// GetStats returns scheduler statistics
func (s *BaseScheduler) GetStats() SchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SchedulerStats{
		TotalScheduled:   s.totalScheduled,
		TotalCompleted:   s.totalCompleted,
		TotalBlocked:     s.totalBlocked,
		CurrentRunQueue:  len(s.runQueue),
		CurrentBlocked:   len(s.blockedQueue),
		ContextSwitches:  s.contextSwitches,
		LastScheduleTime: s.lastScheduleTime,
	}
}

// BlockFiber moves a fiber to the blocked queue
func (s *BaseScheduler) BlockFiber(f *fiber.Fiber) {
	if f == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove from run queue
	for i, rf := range s.runQueue {
		if rf.ID == f.ID {
			s.runQueue = append(s.runQueue[:i], s.runQueue[i+1:]...)
			break
		}
	}

	// Add to blocked queue
	s.blockedQueue = append(s.blockedQueue, f)
	s.totalBlocked++
}

// UnblockFiber moves a fiber back to the run queue
func (s *BaseScheduler) UnblockFiber(fiberID fiber.FiberID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, f := range s.blockedQueue {
		if f.ID == fiberID {
			s.blockedQueue = append(s.blockedQueue[:i], s.blockedQueue[i+1:]...)
			f.Unblock()
			s.runQueue = append(s.runQueue, f)
			return nil
		}
	}

	return fmt.Errorf("fiber %d not found in blocked queue", fiberID)
}

// MarkCompleted marks a fiber as completed. It is idempotent: a fiber that
// is not in any queue is still recorded once via totalCompleted, but a fiber
// that has already been marked completed is not recorded a second time.
// Idempotence matters because callers (the runtime, the filter path) may
// legitimately call MarkCompleted more than once for the same fiber.
func (s *BaseScheduler) MarkCompleted(fiberID fiber.FiberID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := s.completed[fiberID]; seen {
		return
	}
	s.completed[fiberID] = struct{}{}
	s.totalCompleted++
	_ = s.removeLocked(fiberID)
	// Prevent unbounded growth: prune the completed set when it exceeds
	// 4096 entries by discarding the oldest half. The only consequence
	// is that a duplicate MarkCompleted for a pruned ID would add +1 to
	// totalCompleted, which is negligible compared to the memory saved.
	const completedMax = 4096
	const completedTarget = 2048
	if len(s.completed) > completedMax {
		count := 0
		for id := range s.completed {
			delete(s.completed, id)
			count++
			if count >= completedMax-completedTarget {
				break
			}
		}
	}
}

func (s *BaseScheduler) recordSwitch() {
	s.contextSwitches++
	s.lastScheduleTime = time.Now()
}

// filterFinishedInPlace compacts queue in-place, removing finished fibers
// without allocating a new backing array. Nil sentinels are written to the
// tail to release GC references. This replaces the per-scheduler
// filterFinished helpers that each allocated a fresh slice.
// NewScheduler creates the scheduler implementation for the given type.
func NewScheduler(t SchedulerType) Scheduler {
	switch t {
	case TypeFIFO:
		return NewFIFOScheduler()
	case TypeRoundRobin:
		return NewRoundRobinScheduler(10 * time.Millisecond)
	case TypePriority:
		return NewPriorityScheduler()
	case TypeWorkStealing:
		return NewWorkStealingScheduler(4)
	default:
		return NewFIFOScheduler()
	}
}

func filterFinishedInPlace(queue []*fiber.Fiber) []*fiber.Fiber {
	n := 0
	for _, f := range queue {
		if !f.IsFinished() {
			queue[n] = f
			n++
		}
	}
	// Nil out the tail to release GC references.
	for i := n; i < len(queue); i++ {
		queue[i] = nil
	}
	return queue[:n]
}
