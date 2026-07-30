package scheduler

import (
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func TestFIFOScheduler(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()

	if sched == nil {
		t.Fatal("NewFIFOScheduler returned nil")
	}

	if sched.Type() != TypeFIFO {
		t.Errorf("Expected type FIFO, got %s", sched.Type())
	}

	if sched.Name() != "FIFO" {
		t.Errorf("Expected name 'FIFO', got '%s'", sched.Name())
	}
}

func TestFIFOScheduling(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	// Create fibers
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-1")
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-2")
	f3 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-3")

	// Schedule in order
	for _, f := range []*fiber.Fiber{f1, f2, f3} {
		if err := sched.Schedule(f); err != nil {
			t.Fatal(err)
		}
	}

	// Should return in FIFO order
	next, err := sched.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if next.ID != f1.ID {
		t.Errorf("Expected fiber-1, got %s", next.Name)
	}

	next, err = sched.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if next.ID != f2.ID {
		t.Errorf("Expected fiber-2, got %s", next.Name)
	}

	next, err = sched.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if next.ID != f3.ID {
		t.Errorf("Expected fiber-3, got %s", next.Name)
	}
}

func TestRoundRobinScheduler(t *testing.T) {
	t.Parallel()
	quantum := 10 * time.Millisecond
	sched := NewRoundRobinScheduler(quantum)

	if sched == nil {
		t.Fatal("NewRoundRobinScheduler returned nil")
	}

	if sched.Type() != TypeRoundRobin {
		t.Errorf("Expected type RoundRobin, got %s", sched.Type())
	}

	if sched.GetQuantum() != quantum {
		t.Errorf("Expected quantum %v, got %v", quantum, sched.GetQuantum())
	}
}

func TestRoundRobinScheduling(t *testing.T) {
	t.Parallel()
	sched := NewRoundRobinScheduler(10 * time.Millisecond)
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-1")
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-2")

	for _, f := range []*fiber.Fiber{f1, f2} {
		if err := sched.Schedule(f); err != nil {
			t.Fatal(err)
		}
	}

	// First fiber
	next, err := sched.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if next.ID != f1.ID {
		t.Errorf("Expected fiber-1, got %s", next.Name)
	}

	// Should rotate to fiber-2
	next, err = sched.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if next.ID != f2.ID {
		t.Errorf("Expected fiber-2, got %s", next.Name)
	}
}

func TestPriorityScheduler(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()

	if sched == nil {
		t.Fatal("NewPriorityScheduler returned nil")
	}

	if sched.Type() != TypePriority {
		t.Errorf("Expected type Priority, got %s", sched.Type())
	}
}

func TestPriorityScheduling(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	// Create fibers with different priorities
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "low-priority")
	f1.Priority = 1

	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "high-priority")
	f2.Priority = 10

	f3 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "mid-priority")
	f3.Priority = 5

	// Schedule in random order
	for _, f := range []*fiber.Fiber{f1, f2, f3} {
		if err := sched.Schedule(f); err != nil {
			t.Fatal(err)
		}
	}

	// Should return highest priority first
	next, err := sched.Next()
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if next.Priority != 10 {
		t.Errorf("Expected priority 10, got %d", next.Priority)
	}
}

func TestWorkStealingScheduler(t *testing.T) {
	t.Parallel()
	numWorkers := 4
	sched := NewWorkStealingScheduler(numWorkers)

	if sched == nil {
		t.Fatal("NewWorkStealingScheduler returned nil")
	}

	if sched.Type() != TypeWorkStealing {
		t.Errorf("Expected type WorkStealing, got %s", sched.Type())
	}

	if sched.GetNumWorkers() != numWorkers {
		t.Errorf("Expected %d workers, got %d", numWorkers, sched.GetNumWorkers())
	}
}

func TestWorkStealingScheduling(t *testing.T) {
	t.Parallel()
	sched := NewWorkStealingScheduler(4)
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	// Schedule multiple fibers
	for i := 0; i < 10; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
		if err := sched.Schedule(f); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve them
	count := 0
	for i := 0; i < 10; i++ {
		_, err := sched.Next()
		if err == nil {
			count++
		}
	}

	if count != 10 {
		t.Errorf("Expected to retrieve 10 fibers, got %d", count)
	}
}

func TestSchedulerSize(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()

	if sched.Size() != 0 {
		t.Errorf("Expected size 0, got %d", sched.Size())
	}

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	if err := sched.Schedule(f); err != nil {
		t.Fatal(err)
	}

	if sched.Size() != 1 {
		t.Errorf("Expected size 1, got %d", sched.Size())
	}
}

func TestSchedulerRemove(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	if err := sched.Schedule(f); err != nil {
		t.Fatal(err)
	}

	err := sched.Remove(f.ID)
	if err != nil {
		t.Errorf("Remove failed: %v", err)
	}

	if sched.Size() != 0 {
		t.Errorf("Expected size 0 after remove, got %d", sched.Size())
	}
}

func TestSchedulerStartStop(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()

	if sched.IsRunning() {
		t.Error("Scheduler should not be running initially")
	}

	err := sched.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}

	if !sched.IsRunning() {
		t.Error("Scheduler should be running after start")
	}

	err = sched.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}

	if sched.IsRunning() {
		t.Error("Scheduler should not be running after stop")
	}
}

func TestSchedulerStats(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	if err := sched.Schedule(f); err != nil {
		t.Fatal(err)
	}

	stats := sched.GetStats()

	if stats.TotalScheduled != 1 {
		t.Errorf("Expected TotalScheduled 1, got %d", stats.TotalScheduled)
	}

	if stats.CurrentRunQueue != 1 {
		t.Errorf("Expected CurrentRunQueue 1, got %d", stats.CurrentRunQueue)
	}
}

func TestSchedulerGetRunQueue(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()

	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-1")
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber-2")

	if err := sched.Schedule(f1); err != nil {
		t.Fatal(err)
	}
	if err := sched.Schedule(f2); err != nil {
		t.Fatal(err)
	}

	queue := sched.GetRunQueue()

	if len(queue) != 2 {
		t.Errorf("Expected run queue size 2, got %d", len(queue))
	}
}

func TestSchedulerEmptyQueue(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	_, err := sched.Next()

	if err == nil {
		t.Error("Expected error when queue is empty")
	}
}

func TestMarkCompletedDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	if err := sched.Schedule(f); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		sched.MarkCompleted(f.ID)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MarkCompleted deadlocked")
	}
	if got := sched.Size(); got != 0 {
		t.Fatalf("scheduler size = %d after completion", got)
	}
}

func TestWorkStealingConcurrentAccess(t *testing.T) {
	t.Parallel()
	sched := NewWorkStealingScheduler(4)
	const count = 100
	for i := 0; i < count; i++ {
		if err := sched.Schedule(fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < count; i++ {
			_, _ = sched.Next()
		}
		close(done)
	}()
	for i := 0; i < 100; i++ {
		_ = sched.Size()
		_ = sched.GetRunQueue()
		_ = sched.GetWorkerQueues()
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("work-stealing scheduler did not drain")
	}
}

func TestSchedulerTypeString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		st   SchedulerType
		want string
	}{
		{TypeFIFO, "FIFO"},
		{TypeRoundRobin, "RoundRobin"},
		{TypePriority, "Priority"},
		{TypeWorkStealing, "WorkStealing"},
		{SchedulerType(999), "Unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.st.String(); got != tc.want {
				t.Errorf("SchedulerType(%d).String() = %q, want %q", tc.st, got, tc.want)
			}
		})
	}
}

func TestBaseSchedulerClear(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	_ = sched.Schedule(f)
	_ = sched.Start()
	_, _ = sched.Next()
	sched.MarkCompleted(f.ID)
	sched.Clear()

	stats := sched.GetStats()
	if stats.TotalScheduled != 0 {
		t.Errorf("TotalScheduled = %d, want 0", stats.TotalScheduled)
	}
	if stats.TotalCompleted != 0 {
		t.Errorf("TotalCompleted = %d, want 0", stats.TotalCompleted)
	}
	if stats.CurrentRunQueue != 0 {
		t.Errorf("CurrentRunQueue = %d, want 0", stats.CurrentRunQueue)
	}
	if stats.CurrentBlocked != 0 {
		t.Errorf("CurrentBlocked = %d, want 0", stats.CurrentBlocked)
	}
	if sched.Size() != 0 {
		t.Errorf("Size = %d, want 0", sched.Size())
	}
}

func TestBaseSchedulerBlockFiber(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var bs *BaseScheduler
		bs.BlockFiber(nil) // should not panic
	})
	t.Run("normal blocking", func(t *testing.T) {
		sched := NewFIFOScheduler()
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "block-me")
		_ = sched.Schedule(f)
		if sched.Size() != 1 {
			t.Fatalf("expected size 1 before block, got %d", sched.Size())
		}
		sched.BlockFiber(f)
		if sched.Size() != 0 {
			t.Errorf("expected size 0 after block, got %d", sched.Size())
		}
		blocked := sched.GetBlockedQueue()
		if len(blocked) != 1 {
			t.Errorf("expected 1 blocked fiber, got %d", len(blocked))
		}
	})
}

func TestBaseSchedulerUnblockFiber(t *testing.T) {
	t.Parallel()
	t.Run("normal unblocking", func(t *testing.T) {
		sched := NewFIFOScheduler()
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "unblock-me")
		_ = sched.Schedule(f)
		sched.BlockFiber(f)

		err := sched.UnblockFiber(f.ID)
		if err != nil {
			t.Errorf("UnblockFiber failed: %v", err)
		}
		if sched.Size() != 1 {
			t.Errorf("expected size 1 after unblock, got %d", sched.Size())
		}
		blocked := sched.GetBlockedQueue()
		if len(blocked) != 0 {
			t.Errorf("expected 0 blocked fibers after unblock, got %d", len(blocked))
		}
	})
	t.Run("not-found case", func(t *testing.T) {
		sched := NewFIFOScheduler()
		err := sched.UnblockFiber(99999)
		if err == nil {
			t.Error("expected error for non-existent fiber")
		}
	})
}

func TestBaseSchedulerGetBlockedQueue(t *testing.T) {
	t.Parallel()
	sched := NewFIFOScheduler()
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "b1")
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "b2")
	_ = sched.Schedule(f1)
	_ = sched.Schedule(f2)
	sched.BlockFiber(f1)
	sched.BlockFiber(f2)

	blocked := sched.GetBlockedQueue()
	if len(blocked) != 2 {
		t.Fatalf("expected 2 blocked fibers, got %d", len(blocked))
	}
	// Verify clones (different pointers)
	if blocked[0] == f1 || blocked[1] == f2 {
		t.Error("GetBlockedQueue should return clones, not originals")
	}
}

func TestBaseSchedulerRemove(t *testing.T) {
	t.Parallel()
	t.Run("remove from run queue", func(t *testing.T) {
		sched := NewFIFOScheduler()
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "remove-me")
		_ = sched.Schedule(f)
		if err := sched.Remove(f.ID); err != nil {
			t.Errorf("Remove from run queue failed: %v", err)
		}
		if sched.Size() != 0 {
			t.Errorf("size after remove = %d, want 0", sched.Size())
		}
	})
	t.Run("remove from blocked queue", func(t *testing.T) {
		sched := NewFIFOScheduler()
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "blocked-remove")
		_ = sched.Schedule(f)
		sched.BlockFiber(f)
		if err := sched.Remove(f.ID); err != nil {
			t.Errorf("Remove from blocked queue failed: %v", err)
		}
		if sched.Size() != 0 {
			t.Errorf("size after remove = %d, want 0", sched.Size())
		}
	})
	t.Run("remove non-existent fiber", func(t *testing.T) {
		sched := NewFIFOScheduler()
		err := sched.Remove(99999)
		if err == nil {
			t.Error("expected error for non-existent fiber")
		}
	})
}

func TestPrioritySchedulerClear(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "p1")
	f1.SetPriority(5)
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "p2")
	f2.SetPriority(3)
	_ = sched.Schedule(f1)
	_ = sched.Schedule(f2)

	sched.Clear()
	if sched.Size() != 0 {
		t.Errorf("size after Clear = %d, want 0", sched.Size())
	}
	stats := sched.GetStats()
	if stats.TotalScheduled != 0 {
		t.Errorf("TotalScheduled after Clear = %d, want 0", stats.TotalScheduled)
	}
}

func TestPrioritySchedulerGetRunQueue(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "low")
	f1.SetPriority(1)
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "high")
	f2.SetPriority(10)
	_ = sched.Schedule(f1)
	_ = sched.Schedule(f2)

	queue := sched.GetRunQueue()
	if len(queue) != 2 {
		t.Fatalf("expected 2 fibers, got %d", len(queue))
	}
	if queue[0].Priority < queue[1].Priority {
		t.Errorf("GetRunQueue should return highest priority first; got %d then %d", queue[0].Priority, queue[1].Priority)
	}
}

func TestPrioritySchedulerAgeAll(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "ager")
	f.SetPriority(5)
	_ = sched.Schedule(f)

	pq := sched.pqueue
	pq.AgeAll()

	if got := f.PriorityValue(); got <= 5 {
		t.Errorf("priority after AgeAll = %d, want > 5", got)
	}
}

func TestPrioritySchedulerGetAll(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "older-low")
	f1.SetPriority(1)
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "newer-high")
	f2.SetPriority(10)
	_ = sched.Schedule(f1)
	_ = sched.Schedule(f2)

	all := sched.pqueue.GetAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 fibers, got %d", len(all))
	}
	if all[0].Priority == all[1].Priority {
		t.Error("priorities should differ")
	}
	// Verify clones
	for _, f := range all {
		if f == f1 || f == f2 {
			t.Error("GetAll should return clones")
		}
	}
}

func TestRoundRobinSchedulerSetQuantum(t *testing.T) {
	t.Parallel()
	t.Run("valid quantum", func(t *testing.T) {
		sched := NewRoundRobinScheduler(50 * time.Millisecond)
		sched.SetQuantum(100 * time.Millisecond)
		if got := sched.GetQuantum(); got != 100*time.Millisecond {
			t.Errorf("quantum = %v, want 100ms", got)
		}
	})
	t.Run("quantum <= 0 defaults to 10ms", func(t *testing.T) {
		sched := NewRoundRobinScheduler(50 * time.Millisecond)
		sched.SetQuantum(0)
		if got := sched.GetQuantum(); got != 10*time.Millisecond {
			t.Errorf("quantum after SetQuantum(0) = %v, want 10ms", got)
		}
		sched.SetQuantum(-1)
		if got := sched.GetQuantum(); got != 10*time.Millisecond {
			t.Errorf("quantum after SetQuantum(-1) = %v, want 10ms", got)
		}
	})
}

func TestRoundRobinSchedulerShouldPreempt(t *testing.T) {
	t.Parallel()
	t.Run("preemption timing", func(t *testing.T) {
		sched := NewRoundRobinScheduler(10 * time.Millisecond)
		if err := sched.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sched.Stop() }()
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "f")
		_ = sched.Schedule(f)
		_, _ = sched.Next() // sets lastQuantum

		preempt := sched.ShouldPreempt()
		// ShouldPreempt right after Next should be false
		if preempt {
			t.Log("ShouldPreempt returned true immediately after Next (race with time)")
		}
	})
}

func TestWorkStealingSchedulerClear(t *testing.T) {
	t.Parallel()
	sched := NewWorkStealingScheduler(2)
	f1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "ws1")
	f2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "ws2")
	_ = sched.Schedule(f1)
	_ = sched.Schedule(f2)

	sched.Clear()
	if sched.Size() != 0 {
		t.Errorf("size after Clear = %d, want 0", sched.Size())
	}
	queue := sched.GetRunQueue()
	if len(queue) != 0 {
		t.Errorf("run queue len after Clear = %d, want 0", len(queue))
	}
}

func BenchmarkFIFOScheduler(b *testing.B) {
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	fibers := make([]*fiber.Fiber, 100)
	for i := 0; i < 100; i++ {
		fibers[i] = fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, f := range fibers {
			if err := sched.Schedule(f); err != nil {
				b.Fatal(err)
			}
		}
		for range fibers {
			if _, err := sched.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkPriorityScheduler(b *testing.B) {
	sched := NewPriorityScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	fibers := make([]*fiber.Fiber, 100)
	for i := 0; i < 100; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
		f.Priority = i % 10
		fibers[i] = f
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, f := range fibers {
			if err := sched.Schedule(f); err != nil {
				b.Fatal(err)
			}
		}
		for range fibers {
			if _, err := sched.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkRoundRobinScheduler measures round-robin schedule/drain throughput.
func BenchmarkRoundRobinScheduler(b *testing.B) {
	sched := NewRoundRobinScheduler(10 * time.Millisecond)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()
	fibers := make([]*fiber.Fiber, 100)
	for i := 0; i < 100; i++ {
		fibers[i] = fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, f := range fibers {
			if err := sched.Schedule(f); err != nil {
				b.Fatal(err)
			}
		}
		for range fibers {
			if _, err := sched.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkWorkStealingScheduler measures work-stealing schedule/drain throughput.
func BenchmarkWorkStealingScheduler(b *testing.B) {
	sched := NewWorkStealingScheduler(4)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()
	fibers := make([]*fiber.Fiber, 100)
	for i := 0; i < 100; i++ {
		fibers[i] = fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, f := range fibers {
			if err := sched.Schedule(f); err != nil {
				b.Fatal(err)
			}
		}
		for range fibers {
			if _, err := sched.Next(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// TestPrioritySchedulerUpdatePriorityReheaps is a regression guard for AUDIT
// ID 13: changing a queued fiber's priority through UpdatePriority must change
// the order in which fibers are selected, and the update must be safe under
// the scheduler lock (no torn heap).
func TestPrioritySchedulerUpdatePriorityReheaps(t *testing.T) {
	t.Parallel()
	sched := NewPriorityScheduler()
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	low := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "low")
	low.SetPriority(1)
	high := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "high")
	high.SetPriority(5)
	if err := sched.Schedule(low); err != nil {
		t.Fatal(err)
	}
	if err := sched.Schedule(high); err != nil {
		t.Fatal(err)
	}

	if err := sched.UpdatePriority(low.ID, 9); err != nil {
		t.Fatal(err)
	}
	next, err := sched.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != low.ID {
		t.Fatalf("after promotion expected low fiber first, got %s (id=%d)", next.Name, next.ID)
	}
	next, err = sched.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != high.ID {
		t.Fatalf("expected high fiber second, got %s", next.Name)
	}
	if err := sched.UpdatePriority(low.ID, 100); err == nil {
		t.Fatal("UpdatePriority for a fiber no longer queued should error")
	}
}

// TestWorkStealingRecordsStealStats is a regression guard for AUDIT ID 11: the
// work-stealing scheduler must record steal attempts and successes so the
// runtime can surface them in metrics. It builds an imbalanced queue and drains
// it so one Next() call lands on an empty worker and steals from another.
func TestWorkStealingRecordsStealStats(t *testing.T) {
	t.Parallel()
	sched := NewWorkStealingScheduler(2)
	if err := sched.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	newF := func() *fiber.Fiber { return fiber.NewFiber(func() {}, fiber.DefaultStackSize, "fiber") }
	// Schedule two fibers (one per worker), drain worker 0, then schedule two
	// more so they land on worker 0 (min-queue). worker 1 is then empty while
	// worker 0 has work; the next Next() on worker 1 must steal.
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(sched.Schedule(newF())) // w0=1
	must(sched.Schedule(newF())) // w1=1
	if _, err := sched.Next(); err != nil { // worker 0: w0=0, w1=1
		t.Fatal(err)
	}
	must(sched.Schedule(newF())) // w0=1 (min)
	must(sched.Schedule(newF())) // w0=2 (tie -> minWorker 0)
	if _, err := sched.Next(); err != nil { // worker 1: w1=0
		t.Fatal(err)
	}
	if _, err := sched.Next(); err != nil { // worker 0: w0=1
		t.Fatal(err)
	}
	if _, err := sched.Next(); err != nil { // worker 1 empty -> steal from w0
		t.Fatal(err)
	}

	attempts, successes := sched.GetStealStats()
	if attempts == 0 {
		t.Fatal("expected steal attempts to be recorded, got 0")
	}
	if successes == 0 {
		t.Fatal("expected at least one successful steal, got 0")
	}
	if successes > attempts {
		t.Fatalf("successes (%d) cannot exceed attempts (%d)", successes, attempts)
	}
}
