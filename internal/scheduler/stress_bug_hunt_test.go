package scheduler

import (
	"testing"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func TestStressMarkCompletedIsIdempotentAndRemovesQueuedFiber(t *testing.T) {
	t.Parallel()
	constructors := map[string]func() Scheduler{
		"fifo":          func() Scheduler { return NewFIFOScheduler() },
		"round-robin":   func() Scheduler { return NewRoundRobinScheduler(0) },
		"priority":      func() Scheduler { return NewPriorityScheduler() },
		"work-stealing": func() Scheduler { return NewWorkStealingScheduler(4) },
	}
	for name, newScheduler := range constructors {
		t.Run(name, func(t *testing.T) {
			s := newScheduler()
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Stop() }()
			f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, name)
			if err := s.Schedule(f); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 10; i++ {
				s.MarkCompleted(f.ID)
			}
			if got := s.GetStats().TotalCompleted; got != 1 {
				t.Fatalf("TotalCompleted=%d, want 1", got)
			}
			if got := s.Size(); got != 0 {
				t.Fatalf("MarkCompleted left %d queued entries", got)
			}
		})
	}
}

func TestStressPriorityUpdateAfterDispatchFails(t *testing.T) {
	t.Parallel()
	s := NewPriorityScheduler()
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Stop() }()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "dispatched")
	f.SetPriority(3)
	if err := s.Schedule(f); err != nil {
		t.Fatal(err)
	}
	got, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != f.ID {
		t.Fatalf("dispatched ID=%d, want %d", got.ID, f.ID)
	}
	if err := s.UpdatePriority(f.ID, 99); err == nil {
		t.Fatal("priority update succeeded after dispatch")
	}
	if got.PriorityValue() != 3 {
		t.Fatalf("failed update changed dispatched priority to %d", got.PriorityValue())
	}
}

func TestStressDirectSetPriorityRequiresReheapify(t *testing.T) {
	t.Parallel()
	s := NewPriorityScheduler()
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Stop() }()
	first := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "first")
	second := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "second")
	first.SetPriority(1)
	second.SetPriority(5)
	if err := s.Schedule(first); err != nil {
		t.Fatal(err)
	}
	if err := s.Schedule(second); err != nil {
		t.Fatal(err)
	}
	first.SetPriority(10)
	if !s.Reheapify(first.ID) {
		t.Fatal("Reheapify did not find queued fiber")
	}
	next, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != first.ID {
		t.Fatalf("reheapified priority update dispatched ID=%d, want %d", next.ID, first.ID)
	}
	if s.Reheapify(first.ID) {
		t.Fatal("Reheapify found a fiber after dispatch")
	}
}
