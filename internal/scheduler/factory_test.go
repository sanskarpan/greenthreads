package scheduler

import (
	"testing"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func newTestFiber(name string) *fiber.Fiber {
	return fiber.NewFiber(func() {}, fiber.DefaultStackSize, name)
}

// ---------- NewScheduler factory ----------

func TestNewSchedulerAllTypes(t *testing.T) {
	t.Parallel()
	for _, st := range []SchedulerType{TypeFIFO, TypeRoundRobin, TypePriority, TypeWorkStealing} {
		st := st
		t.Run(st.String(), func(t *testing.T) {
			t.Parallel()
			s := NewScheduler(st)
			if s == nil {
				t.Fatalf("NewScheduler(%v) returned nil", st)
			}
			if s.Size() != 0 {
				t.Fatalf("fresh scheduler should be empty, got size %d", s.Size())
			}
		})
	}
}

func TestNewSchedulerFIFOScheduleAndNext(t *testing.T) {
	t.Parallel()
	s := NewScheduler(TypeFIFO)
	f := newTestFiber("fifo")
	if err := s.Schedule(f); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if s.Size() != 1 {
		t.Fatalf("expected size 1, got %d", s.Size())
	}
	got, err := s.Next()
	if err != nil {
		t.Fatalf("Next error: %v", err)
	}
	if got == nil {
		t.Fatal("Next returned nil")
	}
	if got.ID != f.ID {
		t.Fatalf("expected fiber %d, got %d", f.ID, got.ID)
	}
}

func TestNewSchedulerRoundRobin(t *testing.T) {
	t.Parallel()
	s := NewScheduler(TypeRoundRobin)
	f1 := newTestFiber("rr1")
	f2 := newTestFiber("rr2")
	_ = s.Schedule(f1)
	_ = s.Schedule(f2)
	if s.Size() != 2 {
		t.Fatalf("expected 2, got %d", s.Size())
	}
	// Both fibers should be returned
	seen := map[fiber.FiberID]bool{}
	n1, _ := s.Next()
	n2, _ := s.Next()
	if n1 != nil {
		seen[n1.ID] = true
	}
	if n2 != nil {
		seen[n2.ID] = true
	}
	if !seen[f1.ID] || !seen[f2.ID] {
		t.Fatalf("RoundRobin did not return both fibers: %v", seen)
	}
}

func TestNewSchedulerPriority(t *testing.T) {
	t.Parallel()
	s := NewScheduler(TypePriority)
	lo := newTestFiber("lo")
	lo.Priority = 1
	hi := newTestFiber("hi")
	hi.Priority = 10
	_ = s.Schedule(lo)
	_ = s.Schedule(hi)
	first, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if first == nil || first.Priority != 10 {
		t.Fatalf("priority scheduler should return highest-priority fiber first, got priority %d", first.Priority)
	}
}

func TestNewSchedulerWorkStealing(t *testing.T) {
	t.Parallel()
	s := NewScheduler(TypeWorkStealing)
	for i := 0; i < 4; i++ {
		f := newTestFiber("ws")
		if err := s.Schedule(f); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}
	count := 0
	for {
		f, _ := s.Next()
		if f == nil {
			break
		}
		count++
		if count > 10 {
			break
		}
	}
	if count != 4 {
		t.Fatalf("expected 4 fibers, got %d", count)
	}
}

// ---------- Priority BlockFiber / UnblockFiber ----------

func TestPrioritySchedulerBlockUnblock(t *testing.T) {
	t.Parallel()
	s := NewPriorityScheduler()

	f := newTestFiber("block-priority")
	_ = s.Schedule(f)
	if s.Size() != 1 {
		t.Fatalf("expected 1 queued fiber, got %d", s.Size())
	}

	// BlockFiber should remove from run queue
	s.BlockFiber(f)
	if s.Size() != 0 {
		t.Fatalf("expected 0 after block, got %d", s.Size())
	}

	// UnblockFiber should re-add to run queue
	if err := s.UnblockFiber(f.ID); err != nil {
		t.Fatalf("UnblockFiber: %v", err)
	}
	if s.Size() != 1 {
		t.Fatalf("expected 1 after unblock, got %d", s.Size())
	}
	got, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got == nil || got.ID != f.ID {
		t.Fatalf("expected fiber %d after unblock, got %v", f.ID, got)
	}
}

func TestPrioritySchedulerUnblockUnknown(t *testing.T) {
	t.Parallel()
	s := NewPriorityScheduler()
	err := s.UnblockFiber(fiber.FiberID(99999))
	if err == nil {
		t.Fatal("expected error unblocking unknown fiber ID")
	}
}

func TestPrioritySchedulerBlockAlreadyDequeued(t *testing.T) {
	t.Parallel()
	s := NewPriorityScheduler()
	f := newTestFiber("already-gone")
	// Block a fiber that was never scheduled — must not panic
	s.BlockFiber(f)
	// Size should still be 0
	if s.Size() != 0 {
		t.Fatalf("expected 0, got %d", s.Size())
	}
}

func TestPrioritySchedulerBlockMultiple(t *testing.T) {
	t.Parallel()
	s := NewPriorityScheduler()
	fibers := make([]*fiber.Fiber, 5)
	for i := range fibers {
		fibers[i] = newTestFiber("prio-multi")
		fibers[i].Priority = i + 1
		_ = s.Schedule(fibers[i])
	}
	// Block middle one
	s.BlockFiber(fibers[2])
	if s.Size() != 4 {
		t.Fatalf("expected 4 after blocking one, got %d", s.Size())
	}
	// Unblock it — should come back
	_ = s.UnblockFiber(fibers[2].ID)
	if s.Size() != 5 {
		t.Fatalf("expected 5 after unblock, got %d", s.Size())
	}
}

// ---------- WorkStealing BlockFiber / UnblockFiber ----------

func TestWorkStealingBlockUnblock(t *testing.T) {
	t.Parallel()
	s := NewWorkStealingScheduler(2)

	f := newTestFiber("ws-block")
	_ = s.Schedule(f)

	// BlockFiber should track blocked state
	s.BlockFiber(f)

	// UnblockFiber should make the fiber schedulable again
	if err := s.UnblockFiber(f.ID); err != nil {
		t.Fatalf("UnblockFiber: %v", err)
	}
	// After unblock the fiber should be back in the run queue
	if s.Size() != 1 {
		t.Fatalf("expected 1 after unblock, got %d", s.Size())
	}
}

func TestWorkStealingUnblockUnknown(t *testing.T) {
	t.Parallel()
	s := NewWorkStealingScheduler(2)
	err := s.UnblockFiber(fiber.FiberID(88888))
	if err == nil {
		t.Fatal("expected error unblocking unknown fiber")
	}
}

func TestWorkStealingBlockNeverScheduled(t *testing.T) {
	t.Parallel()
	s := NewWorkStealingScheduler(2)
	f := newTestFiber("never-scheduled")
	// Must not panic
	s.BlockFiber(f)
}

func TestWorkStealingBlockThenSchedule(t *testing.T) {
	t.Parallel()
	s := NewWorkStealingScheduler(2)
	f1 := newTestFiber("ws-a")
	f2 := newTestFiber("ws-b")
	_ = s.Schedule(f1)
	_ = s.Schedule(f2)

	s.BlockFiber(f1)

	if err := s.UnblockFiber(f1.ID); err != nil {
		t.Fatalf("UnblockFiber: %v", err)
	}
	count := 0
	for {
		f, _ := s.Next()
		if f == nil {
			break
		}
		count++
		if count > 10 {
			break
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 total fibers, got %d", count)
	}
}

// ---------- NewWorkStealingScheduler via factory ----------

func TestNewSchedulerWorkStealingStats(t *testing.T) {
	t.Parallel()
	s := NewScheduler(TypeWorkStealing)
	stats := s.GetStats()
	if stats.TotalScheduled != 0 {
		t.Fatalf("fresh scheduler has non-zero TotalScheduled=%d", stats.TotalScheduled)
	}
}

// ---------- BaseScheduler BlockFiber / UnblockFiber (via FIFO) ----------

func TestFIFOSchedulerBlockUnblock(t *testing.T) {
	t.Parallel()
	s := NewFIFOScheduler()
	f := newTestFiber("fifo-block")
	_ = s.Schedule(f)

	s.BlockFiber(f)
	if s.Size() != 0 {
		t.Fatalf("expected 0 after block, got %d", s.Size())
	}

	if err := s.UnblockFiber(f.ID); err != nil {
		t.Fatalf("UnblockFiber: %v", err)
	}
	if s.Size() != 1 {
		t.Fatalf("expected 1 after unblock, got %d", s.Size())
	}
}

func TestRoundRobinSchedulerBlockUnblock(t *testing.T) {
	t.Parallel()
	s := NewRoundRobinScheduler(0)
	f := newTestFiber("rr-block")
	_ = s.Schedule(f)

	s.BlockFiber(f)
	if s.Size() != 0 {
		t.Fatalf("expected 0 after block, got %d", s.Size())
	}

	if err := s.UnblockFiber(f.ID); err != nil {
		t.Fatalf("UnblockFiber: %v", err)
	}
	if s.Size() != 1 {
		t.Fatalf("expected 1 after unblock, got %d", s.Size())
	}
}
