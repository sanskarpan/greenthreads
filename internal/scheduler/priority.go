package scheduler

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// PriorityScheduler implements a priority-based scheduler
// Fibers with higher priority are executed first
type PriorityScheduler struct {
	*BaseScheduler
	pqueue *PriorityQueue
}

// NewPriorityScheduler creates a new priority scheduler
func NewPriorityScheduler() *PriorityScheduler {
	pq := &PriorityQueue{}
	heap.Init(pq)

	return &PriorityScheduler{
		BaseScheduler: NewBaseScheduler("Priority", TypePriority),
		pqueue:        pq,
	}
}

// Schedule adds a fiber to the priority queue
func (s *PriorityScheduler) Schedule(f *fiber.Fiber) error {
	if f == nil {
		return fmt.Errorf("cannot schedule nil fiber")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(s.pqueue, f)
	s.totalScheduled++
	s.lastScheduleTime = time.Now()

	return nil
}

// Next returns the next fiber to run (highest priority)
func (s *PriorityScheduler) Next() (*fiber.Fiber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove finished fibers
	s.pqueue.RemoveFinished()

	if s.pqueue.Len() == 0 {
		return nil, fmt.Errorf("no fibers in run queue")
	}

	f := heap.Pop(s.pqueue).(*fiber.Fiber)

	s.recordSwitch()

	return f, nil
}

// MarkCompleted records a completion and removes the fiber from the
// priority queue. It is idempotent (a fiber already completed is not
// counted twice) and overrides the BaseScheduler default so the priority
// heap, not the base run queue, is consulted.
func (s *PriorityScheduler) MarkCompleted(fiberID fiber.FiberID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, seen := s.completed[fiberID]; seen {
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
	filtered := (*s.pqueue)[:0]
	for _, f := range *s.pqueue {
		if f.ID != fiberID {
			filtered = append(filtered, f)
		}
	}
	*s.pqueue = filtered
	heap.Init(s.pqueue)
}

// Size returns the number of fibers in the scheduler
func (s *PriorityScheduler) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.pqueue.Len()
}

// Clear empties the priority queue and resets base statistics.
func (s *PriorityScheduler) Clear() {
	s.mu.Lock()
	s.pqueue = &PriorityQueue{}
	heap.Init(s.pqueue)
	s.mu.Unlock()
	s.BaseScheduler.Clear()
}

// UpdatePriority changes a queued fiber's priority and re-heapifies under the
// scheduler lock. This is the only safe way to change a priority for a fiber
// that may already be in the heap; mutating Fiber.Priority directly races with
// heap operations and can leave the heap inconsistent. It returns an error if
// the fiber is not currently queued.
func (s *PriorityScheduler) UpdatePriority(id fiber.FiberID, priority int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range *s.pqueue {
		if f.ID == id {
			f.SetPriority(priority)
			heap.Init(s.pqueue)
			return nil
		}
	}
	return fmt.Errorf("fiber %d not in priority queue", id)
}

// Remove removes a fiber from the priority queue by ID. It overrides
// BaseScheduler.Remove which only scans the base run queue (unused by
// PriorityScheduler). Returns an error if the fiber is not found.
func (s *PriorityScheduler) Remove(fiberID fiber.FiberID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, f := range *s.pqueue {
		if f.ID == fiberID {
			filtered := make(PriorityQueue, 0, len(*s.pqueue)-1)
			filtered = append(filtered, (*s.pqueue)[:i]...)
			filtered = append(filtered, (*s.pqueue)[i+1:]...)
			*s.pqueue = filtered
			heap.Init(s.pqueue)
			return nil
		}
	}
	return fmt.Errorf("fiber %d not found in priority queue", fiberID)
}

// GetStats returns scheduler statistics with the correct queue depth for
// the priority heap. Overrides BaseScheduler.GetStats which reads the
// unused base runQueue.
func (s *PriorityScheduler) GetStats() SchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SchedulerStats{
		TotalScheduled:   s.totalScheduled,
		TotalCompleted:   s.totalCompleted,
		TotalBlocked:     s.totalBlocked,
		CurrentRunQueue:  s.pqueue.Len(),
		CurrentBlocked:   len(s.blockedQueue),
		ContextSwitches:  s.contextSwitches,
		LastScheduleTime: s.lastScheduleTime,
	}
}

// Reheapify rebuilds the heap after an external SetPriority on a fiber that
// is currently queued. Callers that have direct access to a queued fiber
// (which is rare: the runtime hides live fibers behind GetFiber) should
// invoke this after SetPriority to keep the heap consistent. It returns
// false if the fiber is not in the heap.
func (s *PriorityScheduler) Reheapify(id fiber.FiberID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range *s.pqueue {
		if f.ID == id {
			heap.Init(s.pqueue)
			return true
		}
	}
	return false
}

// GetRunQueue returns all fibers in priority order
func (s *PriorityScheduler) GetRunQueue() []*fiber.Fiber {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.pqueue.GetAll()
}

// PriorityQueue implements heap.Interface for fibers
type PriorityQueue []*fiber.Fiber

// Len returns the number of fibers in the heap.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less orders higher priorities first and preserves creation order on ties.
func (pq PriorityQueue) Less(i, j int) bool {
	// Higher priority comes first
	// If priorities are equal, use FIFO (older fibers first)
	if pq[i].PriorityValue() == pq[j].PriorityValue() {
		return pq[i].CreatedAtValue().Before(pq[j].CreatedAtValue())
	}
	return pq[i].PriorityValue() > pq[j].PriorityValue()
}

// Swap exchanges two heap entries.
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds one fiber to the heap.
func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*fiber.Fiber))
}

// Pop removes the heap's last entry as required by heap.Interface.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// RemoveFinished removes all finished fibers from the queue
func (pq *PriorityQueue) RemoveFinished() {
	filtered := make([]*fiber.Fiber, 0, len(*pq))
	for _, f := range *pq {
		if !f.IsFinished() {
			filtered = append(filtered, f)
		}
	}
	*pq = filtered
	heap.Init(pq)
}

// AgeAll increases the priority of all fibers to prevent starvation
func (pq *PriorityQueue) AgeAll() {
	for _, f := range *pq {
		priority := f.PriorityValue()
		if priority < 1<<30 {
			f.SetPriority(priority + 1)
		}
	}
	heap.Init(pq) // Re-heapify
}

// GetAll returns all fibers in priority order
func (pq *PriorityQueue) GetAll() []*fiber.Fiber {
	fibers := make([]*fiber.Fiber, len(*pq))
	for i, f := range *pq {
		fibers[i] = f.Clone()
	}
	sort.SliceStable(fibers, func(i, j int) bool {
		if fibers[i].Priority == fibers[j].Priority {
			return fibers[i].CreatedAt.Before(fibers[j].CreatedAt)
		}
		return fibers[i].Priority > fibers[j].Priority
	})
	return fibers
}
