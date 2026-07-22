package scheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// WorkStealingScheduler implements a work-stealing scheduler
// Similar to Go's runtime scheduler, each worker has a local queue
// and can steal work from other workers when idle
type WorkStealingScheduler struct {
	*BaseScheduler
	workers       []*Worker
	numWorkers    int
	globalQueue   []*fiber.Fiber
	globalMu      sync.RWMutex
	stealAttempts int64
	stealSuccess  int64
	nextWorker    int64
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
		globalQueue:   make([]*fiber.Fiber, 0),
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

// Schedule adds a fiber to the global queue
func (s *WorkStealingScheduler) Schedule(f *fiber.Fiber) error {
	if f == nil {
		return fmt.Errorf("cannot schedule nil fiber")
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
	// Round-robin through workers
	// Round-robin through workers using signed arithmetic so no uint64→int
	// narrowing is needed (avoids G115 on 32-bit builds).
	workerID := int(atomic.AddInt64(&s.nextWorker, 1)-1) % s.numWorkers
	worker := s.workers[workerID]

	// Try local queue first
	worker.mu.Lock()
	worker.localQueue = s.filterFinished(worker.localQueue)
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

	// Try global queue
	s.globalMu.Lock()
	s.globalQueue = s.filterFinished(s.globalQueue)
	if len(s.globalQueue) > 0 {
		f := s.globalQueue[0]
		s.globalQueue = s.globalQueue[1:]
		s.globalMu.Unlock()

		s.mu.Lock()
		s.recordSwitch()
		s.mu.Unlock()

		return f, nil
	}
	s.globalMu.Unlock()

	// Try to steal from other workers. stealAttempts counts every steal
	// attempt (success or failure) so StealSuccessRate = successes/attempts
	// is meaningful; stealSuccess counts only successful steals.
	s.mu.Lock()
	s.stealAttempts++
	s.mu.Unlock()

	f, err := s.steal(workerID)
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

	return nil, fmt.Errorf("no fibers available")
}

// steal attempts to steal work from other workers
func (s *WorkStealingScheduler) steal(fromWorker int) (*fiber.Fiber, error) {
	// Try to steal from a random worker
	victims := make([]int, 0, s.numWorkers-1)
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
		victim.localQueue = s.filterFinished(victim.localQueue)
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

// Size returns the total number of fibers across all queues
func (s *WorkStealingScheduler) Size() int {
	s.globalMu.RLock()
	total := len(s.globalQueue)
	s.globalMu.RUnlock()

	for _, worker := range s.workers {
		worker.mu.Lock()
		total += len(worker.localQueue)
		worker.mu.Unlock()
	}

	return total
}

// GetRunQueue returns all fibers across all queues
func (s *WorkStealingScheduler) GetRunQueue() []*fiber.Fiber {
	fibers := make([]*fiber.Fiber, 0)

	s.globalMu.RLock()
	for _, f := range s.globalQueue {
		fibers = append(fibers, f.Clone())
	}
	s.globalMu.RUnlock()

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

// filterFinished removes finished fibers from the queue
func (s *WorkStealingScheduler) filterFinished(queue []*fiber.Fiber) []*fiber.Fiber {
	filtered := make([]*fiber.Fiber, 0, len(queue))
	for _, f := range queue {
		if !f.IsFinished() {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// GetNumWorkers returns the number of workers
func (s *WorkStealingScheduler) GetNumWorkers() int {
	return s.numWorkers
}

// Clear empties all worker queues, the global queue, and resets statistics.
func (s *WorkStealingScheduler) Clear() {
	s.globalMu.Lock()
	s.globalQueue = make([]*fiber.Fiber, 0)
	s.globalMu.Unlock()
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
