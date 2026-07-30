package scheduler

import (
	"fmt"

	"github.com/sanskarpan/greenthreads/internal/fiber"
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
	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return nil, fmt.Errorf("scheduler stopped")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove finished fibers. filterFinishedInPlace only drops the entries; the
	// authoritative completion accounting happens in MarkCompleted, which
	// the runtime calls in complete().
	s.runQueue = filterFinishedInPlace(s.runQueue)

	if len(s.runQueue) == 0 {
		return nil, ErrNoFibers
	}

	// Get first fiber
	f := s.runQueue[0]
	s.runQueue = s.runQueue[1:]

	s.recordSwitch()

	return f, nil
}
