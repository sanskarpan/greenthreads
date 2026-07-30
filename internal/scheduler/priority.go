package scheduler

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// PriorityScheduler implements a priority-based scheduler
// Fibers with higher priority are executed first
type PriorityScheduler struct {
	*BaseScheduler
	pqueue   *PriorityQueue
	popCount int64
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

	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return fmt.Errorf("scheduler stopped")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(s.pqueue, PriorityItem{
		Fiber:        f,
		PriorityVal:  f.PriorityValue(),
		CreatedAtVal: f.CreatedAtValue(),
	})
	s.totalScheduled++
	s.lastScheduleTime = time.Now()

	return nil
}

// Next returns the next fiber to run (highest priority)
func (s *PriorityScheduler) Next() (*fiber.Fiber, error) {
	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return nil, fmt.Errorf("scheduler stopped")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove finished fibers
	s.pqueue.RemoveFinished()

	if s.pqueue.Len() == 0 {
		return nil, ErrNoFibers
	}

	s.popCount++
	const ageEvery = 100
	if s.popCount%ageEvery == 0 {
		s.ageAllLocked()
	}

	item := heap.Pop(s.pqueue).(PriorityItem)
	f := item.Fiber

	s.recordSwitch()

	return f, nil
}

// ageAllLocked boosts all waiting fibers' priority by 1 to prevent
// indefinite starvation of low-priority fibers (SC-4). Must be called
// with s.mu held.
func (s *PriorityScheduler) ageAllLocked() {
	for i := range *s.pqueue {
		if (*s.pqueue)[i].PriorityVal < 1<<30 {
			(*s.pqueue)[i].PriorityVal++
			(*s.pqueue)[i].SetPriority((*s.pqueue)[i].PriorityVal)
		}
	}
	heap.Init(s.pqueue)
}

// BlockFiber removes the fiber from the priority queue and moves it to the
// blocked queue. This overrides BaseScheduler.BlockFiber which only scans the
// base runQueue (unused by PriorityScheduler).
func (s *PriorityScheduler) BlockFiber(f *fiber.Fiber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Remove from priority queue so Next() cannot dispatch a blocked fiber.
	for i, item := range *s.pqueue {
		if item.ID == f.ID {
			heap.Remove(s.pqueue, item.index)
			_ = i
			break
		}
	}
	s.blockedQueue = append(s.blockedQueue, f)
}

// UnblockFiber removes the fiber from the blocked queue and re-inserts it into
// the priority queue. This overrides BaseScheduler.UnblockFiber.
func (s *PriorityScheduler) UnblockFiber(fiberID fiber.FiberID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, bf := range s.blockedQueue {
		if bf.ID == fiberID {
			s.blockedQueue = append(s.blockedQueue[:i], s.blockedQueue[i+1:]...)
			bf.Unblock()
			// Re-insert into the priority queue with cached priority fields.
			heap.Push(s.pqueue, PriorityItem{
				Fiber:        bf,
				PriorityVal:  bf.PriorityValue(),
				CreatedAtVal: bf.CreatedAtValue(),
			})
			return nil
		}
	}
	return fmt.Errorf("fiber %d not found in blocked queue", fiberID)
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
	// Remove from priority queue using O(log n) heap.Remove.
	for i, item := range *s.pqueue {
		if item.ID == fiberID {
			heap.Remove(s.pqueue, i)
			return
		}
	}
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
	for i, item := range *s.pqueue {
		if item.ID == id {
			item.SetPriority(priority)
			(*s.pqueue)[i].PriorityVal = priority
			heap.Fix(s.pqueue, item.index)
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
	for i, item := range *s.pqueue {
		if item.ID == fiberID {
			heap.Remove(s.pqueue, i)
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
// is currently queued. It refreshes the cached PriorityVal from the fiber's
// current value before fixing the heap position so the new priority is
// honoured. Callers that have direct access to a queued fiber (which is rare:
// the runtime hides live fibers behind GetFiber) should invoke this after
// SetPriority to keep the heap consistent. It returns false if the fiber is
// not in the heap.
func (s *PriorityScheduler) Reheapify(id fiber.FiberID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range *s.pqueue {
		if (*s.pqueue)[i].ID == id {
			// Refresh the cached priority so Less() sees the new value.
			(*s.pqueue)[i].PriorityVal = (*s.pqueue)[i].PriorityValue()
			heap.Fix(s.pqueue, (*s.pqueue)[i].index)
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

// PriorityItem wraps a fiber with cached priority metadata for O(1) heap
// comparisons and an index field to support O(log n) heap.Remove / heap.Fix.
type PriorityItem struct {
	*fiber.Fiber
	PriorityVal  int
	CreatedAtVal time.Time
	index        int // position in the heap; maintained by Push/Pop/Swap
}

// PriorityQueue implements heap.Interface for PriorityItems.
type PriorityQueue []PriorityItem

// Len returns the number of items in the heap.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less orders higher priorities first and preserves creation order on ties.
// Uses cached PriorityVal and CreatedAtVal so no fiber mutex is acquired.
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].PriorityVal == pq[j].PriorityVal {
		return pq[i].CreatedAtVal.Before(pq[j].CreatedAtVal)
	}
	return pq[i].PriorityVal > pq[j].PriorityVal
}

// Swap exchanges two heap entries and updates their index fields.
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push adds one PriorityItem to the heap.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(PriorityItem)
	item.index = n
	*pq = append(*pq, item)
}

// Pop removes the heap's last entry as required by heap.Interface.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = PriorityItem{} // clear to GC-safe state
	item.index = -1
	*pq = old[:n-1]
	return item
}

// RemoveFinished removes all finished fibers from the queue using in-place
// compaction. heap.Init is called once to restore heap invariants after the
// compaction.
func (pq *PriorityQueue) RemoveFinished() {
	n := 0
	for _, item := range *pq {
		if !item.IsFinished() {
			(*pq)[n] = item
			n++
		}
	}
	// Nil out tails for GC.
	for i := n; i < len(*pq); i++ {
		(*pq)[i] = PriorityItem{}
	}
	*pq = (*pq)[:n]
	if n > 0 {
		heap.Init(pq)
	}
}

// AgeAll increases the priority of all fibers to prevent starvation
func (pq *PriorityQueue) AgeAll() {
	for i := range *pq {
		priority := (*pq)[i].PriorityVal
		if priority < 1<<30 {
			(*pq)[i].SetPriority(priority + 1)
			(*pq)[i].PriorityVal = priority + 1
		}
	}
	heap.Init(pq) // Re-heapify
}

// GetAll returns all fibers in priority order
func (pq *PriorityQueue) GetAll() []*fiber.Fiber {
	fibers := make([]*fiber.Fiber, len(*pq))
	for i, item := range *pq {
		fibers[i] = item.Clone()
	}
	sort.SliceStable(fibers, func(i, j int) bool {
		if fibers[i].Priority == fibers[j].Priority {
			return fibers[i].CreatedAt.Before(fibers[j].CreatedAt)
		}
		return fibers[i].Priority > fibers[j].Priority
	})
	return fibers
}
