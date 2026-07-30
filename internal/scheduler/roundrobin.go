package scheduler

import (
	"fmt"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// RoundRobinScheduler is a FIFO scheduler with a configurable time quantum.
// The runtime does not automatically preempt fibers after their quantum; callers
// can check ShouldPreempt() and cooperatively yield. Under the current execution
// model, RoundRobinScheduler behaves identically to FIFOScheduler unless a caller
// actively re-enqueues fibers on preemption.
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
