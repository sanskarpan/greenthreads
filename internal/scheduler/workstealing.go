package scheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// maxWorkers is the upper bound on s.numWorkers; used to size the stack-local
// victim buffer in steal() so the slice header never escapes to the heap.
const maxWorkers = 256

// WorkStealingScheduler distributes fibers across per-worker local queues.
// Fibers are load-balanced at Schedule time; idle workers steal from busy
// workers' tails. No global queue: all fibers live in worker-local queues.
type WorkStealingScheduler struct {
	*BaseScheduler
	workers    []*Worker
	numWorkers int
	// globalMu protects blockedQueue and is used as a struct-level lock for
	// operations that must be atomic across all worker queues (BlockFiber,
	// UnblockFiber).
	globalMu      sync.RWMutex
	stealAttempts int64
	stealSuccess  int64
	nextWorker    uint64
}

// Worker represents a worker in the work-stealing scheduler
type Worker struct {
	ID         int
	localQueue []*fiber.Fiber
	mu         sync.Mutex
	stealsFrom int64
	stealsTo   int64
}

// NewWorkStealingScheduler creates a new work-stealing scheduler
func NewWorkStealingScheduler(numWorkers int) *WorkStealingScheduler {
	if numWorkers <= 0 {
		numWorkers = 4 // Default to 4 workers
	}

	s := &WorkStealingScheduler{
		BaseScheduler: NewBaseScheduler("WorkStealing", TypeWorkStealing),
		numWorkers:    numWorkers,
		workers:       make([]*Worker, numWorkers),
	}

	// Create workers
	for i := 0; i < numWorkers; i++ {
		s.workers[i] = &Worker{
			ID:         i,
			localQueue: make([]*fiber.Fiber, 0),
		}
	}

	return s
}

// Schedule adds a fiber to the worker with the smallest local queue.
func (s *WorkStealingScheduler) Schedule(f *fiber.Fiber) error {
	if f == nil {
		return fmt.Errorf("cannot schedule nil fiber")
	}

	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return fmt.Errorf("scheduler stopped")
	}

	// Try to find a worker with the smallest queue
	minWorker := 0
	minSize := int(^uint(0) >> 1)

	for i := 0; i < s.numWorkers; i++ {
		s.workers[i].mu.Lock()
		size := len(s.workers[i].localQueue)
		s.workers[i].mu.Unlock()

		if size < minSize {
			minSize = size
			minWorker = i
		}
	}

	// Add to worker's local queue
	s.workers[minWorker].mu.Lock()
	s.workers[minWorker].localQueue = append(s.workers[minWorker].localQueue, f)
	s.workers[minWorker].mu.Unlock()

	s.mu.Lock()
	s.totalScheduled++
	s.lastScheduleTime = time.Now()
	s.mu.Unlock()

	return nil
}

// Next returns the next fiber to run
// It first checks the local queue, then tries to steal from other workers
func (s *WorkStealingScheduler) Next() (*fiber.Fiber, error) {
	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return nil, fmt.Errorf("scheduler stopped")
	}

	// Round-robin through workers using uint64 arithmetic (SC-7: no overflow,
	// no G115 on 32-bit builds). Subtract 1 after the Add so the first call
	// maps to worker 0 (zero-based), matching the original signed behaviour.
	workerIdx := int((atomic.AddUint64(&s.nextWorker, 1) - 1) % uint64(s.numWorkers)) // #nosec G115 -- numWorkers bounded by maxWorkers constant (1024)
	worker := s.workers[workerIdx]

	// Try local queue first
	worker.mu.Lock()
	worker.localQueue = filterFinishedInPlace(worker.localQueue)
	if len(worker.localQueue) > 0 {
		f := worker.localQueue[0]
		worker.localQueue = worker.localQueue[1:]
		worker.mu.Unlock()

		s.mu.Lock()
		s.recordSwitch()
		s.mu.Unlock()

		return f, nil
	}
	worker.mu.Unlock()

	// globalQueue is not used by Schedule; no scan needed.

	// Try to steal from other workers. stealAttempts counts every steal
	// attempt (success or failure) so StealSuccessRate = successes/attempts
	// is meaningful; stealSuccess counts only successful steals.
	s.mu.Lock()
	s.stealAttempts++
	s.mu.Unlock()

	f, err := s.steal(workerIdx)
	if err == nil {
		worker.mu.Lock()
		worker.stealsFrom++
		worker.mu.Unlock()
		s.mu.Lock()
		s.stealSuccess++
		s.recordSwitch()
		s.mu.Unlock()
		return f, nil
	}

	return nil, ErrNoFibers
}

// steal attempts to steal work from other workers
func (s *WorkStealingScheduler) steal(fromWorker int) (*fiber.Fiber, error) {
	// Use a stack-local array to avoid heap allocation for the victim list
	// (PERF-8). maxWorkers caps the size so the array is always large enough.
	var victimArr [maxWorkers]int
	victims := victimArr[:0]
	for i := 0; i < s.numWorkers; i++ {
		if i != fromWorker {
			victims = append(victims, i)
		}
	}

	// Shuffle victims
	rand.Shuffle(len(victims), func(i, j int) {
		victims[i], victims[j] = victims[j], victims[i]
	})

	// Try each victim
	for _, victimID := range victims {
		victim := s.workers[victimID]

		victim.mu.Lock()
		victim.localQueue = filterFinishedInPlace(victim.localQueue)
		if len(victim.localQueue) > 0 {
			// Steal from the end (LIFO for better cache locality)
			f := victim.localQueue[len(victim.localQueue)-1]
			victim.localQueue = victim.localQueue[:len(victim.localQueue)-1]
			victim.stealsTo++
			victim.mu.Unlock()

			return f, nil
		}
		victim.mu.Unlock()
	}

	return nil, fmt.Errorf("no work to steal")
}

// BlockFiber removes the fiber from whichever worker queue contains it and
// appends it to the blocked queue. This overrides BaseScheduler.BlockFiber
// which only scans the base runQueue (unused by WorkStealingScheduler).
func (s *WorkStealingScheduler) BlockFiber(f *fiber.Fiber) {
	s.globalMu.Lock()
	defer s.globalMu.Unlock()
	for _, w := range s.workers {
		w.mu.Lock()
		for i, qf := range w.localQueue {
			if qf.ID == f.ID {
				w.localQueue = append(w.localQueue[:i], w.localQueue[i+1:]...)
				w.mu.Unlock()
				s.blockedQueue = append(s.blockedQueue, f)
				return
			}
		}
		w.mu.Unlock()
	}
	// Not found in worker queues; append to blocked anyway to keep accounting
	// consistent with callers that call BlockFiber before Schedule returns.
	s.blockedQueue = append(s.blockedQueue, f)
}

// UnblockFiber removes the fiber from the blocked queue and re-schedules it
// on the worker with the smallest local queue. This overrides
// BaseScheduler.UnblockFiber.
func (s *WorkStealingScheduler) UnblockFiber(fiberID fiber.FiberID) error {
	s.globalMu.Lock()
	defer s.globalMu.Unlock()

	// Find and remove from blocked queue.
	var bf *fiber.Fiber
	for i, f := range s.blockedQueue {
		if f.ID == fiberID {
			bf = f
			s.blockedQueue = append(s.blockedQueue[:i], s.blockedQueue[i+1:]...)
			break
		}
	}
	if bf == nil {
		return fmt.Errorf("fiber %d not found in blocked queue", fiberID)
	}
	bf.Unblock()

	// Re-schedule: pick the worker with the smallest queue.
	minLen := -1
	minIdx := 0
	for i, w := range s.workers {
		w.mu.Lock()
		l := len(w.localQueue)
		w.mu.Unlock()
		if minLen < 0 || l < minLen {
			minLen = l
			minIdx = i
		}
	}
	s.workers[minIdx].mu.Lock()
	s.workers[minIdx].localQueue = append(s.workers[minIdx].localQueue, bf)
	s.workers[minIdx].mu.Unlock()
	return nil
}

// Remove removes a fiber from any worker local queue.
// It overrides BaseScheduler.Remove which only scans the base run queue
// (unused by WorkStealingScheduler).
func (s *WorkStealingScheduler) Remove(fiberID fiber.FiberID) error {
	// Check worker local queues
	for _, w := range s.workers {
		w.mu.Lock()
		for i, f := range w.localQueue {
			if f.ID == fiberID {
				w.localQueue = append(w.localQueue[:i], w.localQueue[i+1:]...)
				w.mu.Unlock()
				return nil
			}
		}
		w.mu.Unlock()
	}
	return fmt.Errorf("fiber %d not found in work-stealing queues", fiberID)
}

// GetStats returns scheduler statistics with the correct queue depth
// across all worker queues. Overrides BaseScheduler.GetStats which reads
// the unused base runQueue.
func (s *WorkStealingScheduler) GetStats() SchedulerStats {
	s.mu.RLock()
	total := s.totalScheduled
	completed := s.totalCompleted
	blocked := s.totalBlocked
	switches := s.contextSwitches
	lastSched := s.lastScheduleTime
	s.mu.RUnlock()

	queueDepth := s.Size()
	return SchedulerStats{
		TotalScheduled:   total,
		TotalCompleted:   completed,
		TotalBlocked:     blocked,
		CurrentRunQueue:  queueDepth,
		ContextSwitches:  switches,
		LastScheduleTime: lastSched,
	}
}

// Size returns the total number of fibers across all worker queues
func (s *WorkStealingScheduler) Size() int {
	total := 0
	for _, worker := range s.workers {
		worker.mu.Lock()
		total += len(worker.localQueue)
		worker.mu.Unlock()
	}
	return total
}

// GetRunQueue returns a best-effort snapshot of every fiber across all worker
// queues. Worker-to-worker steal migrations can produce a fiber that appears in
// two queues or none, but this has no correctness impact for the visualizer (it
// is a one-frame glitch in the browser).
func (s *WorkStealingScheduler) GetRunQueue() []*fiber.Fiber {
	fibers := make([]*fiber.Fiber, 0)
	for _, worker := range s.workers {
		worker.mu.Lock()
		for _, f := range worker.localQueue {
			fibers = append(fibers, f.Clone())
		}
		worker.mu.Unlock()
	}
	return fibers
}

// GetWorkerQueues returns the state of all worker queues
func (s *WorkStealingScheduler) GetWorkerQueues() [][]fiber.FiberID {
	queues := make([][]fiber.FiberID, s.numWorkers)

	for i, worker := range s.workers {
		worker.mu.Lock()
		queues[i] = make([]fiber.FiberID, len(worker.localQueue))
		for j, f := range worker.localQueue {
			queues[i][j] = f.ID
		}
		worker.mu.Unlock()
	}

	return queues
}

// GetStealStats returns work-stealing statistics
func (s *WorkStealingScheduler) GetStealStats() (attempts, successes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.stealAttempts, s.stealSuccess
}

// MarkCompleted records a completion and removes the fiber from the
// worker's local queue that currently holds it. It is idempotent so the
// runtime can call it after a fiber terminates and the filter path can
// also call it without double-counting.
func (s *WorkStealingScheduler) MarkCompleted(fiberID fiber.FiberID) {
	s.mu.Lock()
	if _, seen := s.completed[fiberID]; seen {
		s.mu.Unlock()
		return
	}
	s.completed[fiberID] = struct{}{}
	s.totalCompleted++
	if len(s.completed) > 4096 {
		count := 0
		for id := range s.completed {
			delete(s.completed, id)
			count++
			if count >= 2048 {
				break
			}
		}
	}
	s.mu.Unlock()

	// Search worker local queues under each worker's lock. The worker
	// locks are not held under s.mu, so this is a safe nesting order.
	for _, w := range s.workers {
		w.mu.Lock()
		filtered := w.localQueue[:0]
		for _, f := range w.localQueue {
			if f.ID != fiberID {
				filtered = append(filtered, f)
			}
		}
		w.localQueue = filtered
		w.mu.Unlock()
	}
	// globalQueue is not used by Schedule; no scan needed.
}

// GetNumWorkers returns the number of workers
func (s *WorkStealingScheduler) GetNumWorkers() int {
	return s.numWorkers
}

// Clear empties all worker queues and resets statistics.
func (s *WorkStealingScheduler) Clear() {
	for _, w := range s.workers {
		w.mu.Lock()
		w.localQueue = make([]*fiber.Fiber, 0)
		w.mu.Unlock()
	}
	s.mu.Lock()
	s.stealAttempts = 0
	s.stealSuccess = 0
	s.mu.Unlock()
	s.BaseScheduler.Clear()
}
