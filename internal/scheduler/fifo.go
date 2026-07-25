package scheduler

import (
	"fmt"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// FIFOScheduler implements a First-In-First-Out scheduler
// Fibers are executed in the order they are scheduled
type FIFOScheduler struct {
	*BaseScheduler
}

// NewFIFOScheduler creates a new FIFO scheduler
func NewFIFOScheduler() *FIFOScheduler {
	return &FIFOScheduler{
		BaseScheduler: NewBaseScheduler("FIFO", TypeFIFO),
	}
}

// Next returns the next fiber to run (first in queue)
func (s *FIFOScheduler) Next() (*fiber.Fiber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove finished fibers. filterFinished only drops the entries; the
	// authoritative completion accounting happens in MarkCompleted, which
	// the runtime calls in complete().
	s.runQueue = s.filterFinished(s.runQueue)

	if len(s.runQueue) == 0 {
		return nil, fmt.Errorf("no fibers in run queue")
	}

	// Get first fiber
	f := s.runQueue[0]
	s.runQueue = s.runQueue[1:]

	s.recordSwitch()

	return f, nil
}

// filterFinished removes finished fibers from the queue. It does NOT update
// completion statistics; that is the runtime's responsibility.
func (s *FIFOScheduler) filterFinished(queue []*fiber.Fiber) []*fiber.Fiber {
	filtered := make([]*fiber.Fiber, 0, len(queue))
	for _, f := range queue {
		if !f.IsFinished() {
			filtered = append(filtered, f)
		}
	}
	return filtered
}
