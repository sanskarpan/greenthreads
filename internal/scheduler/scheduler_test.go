package scheduler

import (
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func TestFIFOScheduler(t *testing.T) {
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
	sched := NewPriorityScheduler()

	if sched == nil {
		t.Fatal("NewPriorityScheduler returned nil")
	}

	if sched.Type() != TypePriority {
		t.Errorf("Expected type Priority, got %s", sched.Type())
	}
}

func TestPriorityScheduling(t *testing.T) {
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

// TestPrioritySchedulerUpdatePriorityReheaps is a regression guard for AUDIT
// ID 13: changing a queued fiber's priority through UpdatePriority must change
// the order in which fibers are selected, and the update must be safe under
// the scheduler lock (no torn heap).
func TestPrioritySchedulerUpdatePriorityReheaps(t *testing.T) {
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
