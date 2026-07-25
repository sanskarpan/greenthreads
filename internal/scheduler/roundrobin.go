package scheduler

import (
	"fmt"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// RoundRobinScheduler implements a round-robin scheduler
// Each fiber gets a time quantum and is rotated to the back of the queue
type RoundRobinScheduler struct {
	*BaseScheduler
	quantum     time.Duration
	lastQuantum time.Time
}

// NewRoundRobinScheduler creates a new round-robin scheduler
func NewRoundRobinScheduler(quantum time.Duration) *RoundRobinScheduler {
	if quantum <= 0 {
		quantum = 10 * time.Millisecond // Default quantum
	}

	return &RoundRobinScheduler{
		BaseScheduler: NewBaseScheduler("RoundRobin", TypeRoundRobin),
		quantum:       quantum,
	}
}

// Next returns the next fiber to run
func (s *RoundRobinScheduler) Next() (*fiber.Fiber, error) {
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

	// A scheduler selection is a one-shot admission decision. The runtime
	// re-schedules work only through an explicit future Schedule call; this
	// avoids running one function concurrently with itself.
	s.recordSwitch()
	s.lastQuantum = time.Now()

	return f, nil
}

// SetQuantum sets the time quantum for the scheduler
func (s *RoundRobinScheduler) SetQuantum(quantum time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if quantum <= 0 {
		quantum = 10 * time.Millisecond
	}
	s.quantum = quantum
}

// GetQuantum returns the current time quantum
func (s *RoundRobinScheduler) GetQuantum() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.quantum
}

// ShouldPreempt returns true if the current fiber should be preempted
func (s *RoundRobinScheduler) ShouldPreempt() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return time.Since(s.lastQuantum) >= s.quantum
}

// filterFinished removes finished fibers from the queue. It does NOT update
// completion statistics; that is the runtime's responsibility.
func (s *RoundRobinScheduler) filterFinished(queue []*fiber.Fiber) []*fiber.Fiber {
	filtered := make([]*fiber.Fiber, 0, len(queue))
	for _, f := range queue {
		if !f.IsFinished() {
			filtered = append(filtered, f)
		}
	}
	return filtered
}
