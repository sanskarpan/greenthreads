package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

func TestNewRuntime(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)

	if rt == nil {
		t.Fatal("NewRuntime returned nil")
	}

	if rt.scheduler == nil {
		t.Error("Scheduler should not be nil")
	}

	if rt.metrics == nil {
		t.Error("Metrics should not be nil")
	}

	if rt.eventTracker == nil {
		t.Error("EventTracker should not be nil")
	}

	if rt.fibers == nil {
		t.Error("Fibers map should not be nil")
	}
}

func TestRuntimeStartStop(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)

	if rt.IsRunning() {
		t.Error("Runtime should not be running initially")
	}

	err := rt.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if !rt.IsRunning() {
		t.Error("Runtime should be running after start")
	}

	err = rt.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if rt.IsRunning() {
		t.Error("Runtime should not be running after stop")
	}
}

func TestRuntimeSpawn(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	done := make(chan struct{})
	fiberID, err := rt.Spawn(func() {
		close(done)
	}, "test-fiber")

	if err != nil {
		t.Fatalf("Spawn failed: %v", err)
	}

	if fiberID == 0 {
		t.Error("Fiber ID should not be 0")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fiber function was not executed")
	}
}

func TestRuntimeMultipleFibers(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	count := 0
	mutex := make(chan bool, 1)
	mutex <- true

	for i := 0; i < 5; i++ {
		if _, err := rt.Spawn(func() {
			<-mutex
			count++
			mutex <- true
		}, "fiber"); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	<-mutex
	if count != 5 {
		t.Errorf("Expected 5 executions, got %d", count)
	}
	mutex <- true
}

func TestRuntimeGetFiber(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	fiberID, err := rt.Spawn(func() {
		time.Sleep(100 * time.Millisecond)
	}, "test-fiber")
	if err != nil {
		t.Fatal(err)
	}

	fiber, err := rt.GetFiber(fiberID)
	if err != nil {
		t.Fatalf("GetFiber failed: %v", err)
	}

	if fiber == nil {
		t.Fatal("GetFiber returned nil")
	}

	if fiber.ID != fiberID {
		t.Errorf("Expected fiber ID %d, got %d", fiberID, fiber.ID)
	}

	if fiber.Name != "test-fiber" {
		t.Errorf("Expected name 'test-fiber', got '%s'", fiber.Name)
	}
}

func TestRuntimeGetAllFibers(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	for i := 0; i < 3; i++ {
		if _, err := rt.Spawn(func() {
			time.Sleep(100 * time.Millisecond)
		}, "fiber"); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	fibers := rt.GetAllFibers()

	// At least 3 fibers (plus main fiber)
	if len(fibers) < 3 {
		t.Errorf("Expected at least 3 fibers, got %d", len(fibers))
	}
}

func TestRuntimeMetrics(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	if _, err := rt.Spawn(func() {
		time.Sleep(50 * time.Millisecond)
	}, "fiber"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	metrics := rt.GetMetrics()

	if metrics.TotalFibersCreated < 1 {
		t.Errorf("Expected at least 1 fiber created, got %d", metrics.TotalFibersCreated)
	}

	if metrics.TotalContextSwitches < 1 {
		t.Errorf("Expected at least 1 context switch, got %d", metrics.TotalContextSwitches)
	}
}

func TestRuntimeEvents(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	if _, err := rt.Spawn(func() {}, "fiber"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	events := rt.GetEvents(100)

	if len(events) == 0 {
		t.Error("Expected some events to be recorded")
	}
}

func TestRuntimeReset(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)

	if _, err := rt.Spawn(func() {}, "fiber"); err == nil {
		t.Fatal("spawn should fail before runtime start")
	}

	rt.Reset()

	fibers := rt.GetAllFibers()

	if len(fibers) > 0 {
		t.Errorf("Expected 0 fibers after reset, got %d", len(fibers))
	}
}

func TestRuntimeDifferentSchedulers(t *testing.T) {
	schedulerTypes := []scheduler.SchedulerType{
		scheduler.TypeFIFO,
		scheduler.TypeRoundRobin,
		scheduler.TypePriority,
		scheduler.TypeWorkStealing,
	}

	for _, sType := range schedulerTypes {
		rt := NewRuntime(sType, 4)
		if err := rt.Start(); err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		if _, err := rt.Spawn(func() {
			close(done)
		}, "test"); err != nil {
			t.Fatal(err)
		}

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("fiber not executed with scheduler type %s", sType)
		}
		if err := rt.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}

	}
}

func TestRuntimeRunsLongFiberExactlyOnce(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()
	var runs atomic.Int32
	done := make(chan struct{})
	if _, err := rt.Spawn(func() {
		runs.Add(1)
		time.Sleep(100 * time.Millisecond)
		close(done)
	}, "long"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("long fiber did not complete")
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("fiber ran %d times, want once", got)
	}
}

func TestRuntimeCanRestartAfterStop(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	for cycle := 0; cycle < 2; cycle++ {
		if err := rt.Start(); err != nil {
			t.Fatalf("start cycle %d: %v", cycle, err)
		}
		done := make(chan struct{})
		if _, err := rt.Spawn(func() { close(done) }, "restart"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("cycle %d fiber did not complete", cycle)
		}
		if err := rt.Stop(context.Background()); err != nil {
			t.Fatalf("stop cycle %d: %v", cycle, err)
		}
	}
}

// TestSchedulerCompletionIsRecorded is a regression guard for AUDIT ID 12:
// rt.complete must call scheduler.MarkCompleted so the scheduler's
// TotalCompleted statistic increments, not just the metrics counter.
func TestSchedulerCompletionIsRecorded(t *testing.T) {
	for _, sType := range []scheduler.SchedulerType{
		scheduler.TypeFIFO, scheduler.TypeRoundRobin, scheduler.TypePriority, scheduler.TypeWorkStealing,
	} {
		t.Run(sType.String(), func(t *testing.T) {
			rt := NewRuntime(sType, 2)
			if err := rt.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = rt.Stop(ctx)
			}()
			const n = 5
			for i := 0; i < n; i++ {
				if _, err := rt.Spawn(func() {}, "fiber"); err != nil {
					t.Fatal(err)
				}
			}
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if rt.GetScheduler().GetStats().TotalCompleted == int64(n) {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("scheduler %s TotalCompleted = %d, want %d", sType, rt.GetScheduler().GetStats().TotalCompleted, n)
		})
	}
}

// TestRuntimeReportsBlockedFibersGauge is a regression guard for AUDIT ID 11:
// the BlockedFibers metric must reflect the actual count of fibers in the
// Blocked state. The deadlock detector updates the gauge on each check.
func TestRuntimeReportsBlockedFibersGauge(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	// Insert a fiber directly into the runtime's fiber map and mark it Blocked.
	// This is the state the deadlock detector / gauge wiring observes; it does
	// not depend on the sync-primitive path (tested separately).
	blocked := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "blocked")
	blocked.Block("test", nil)
	rt.fibersMu.Lock()
	rt.fibers[blocked.ID] = blocked
	rt.fibersMu.Unlock()

	rt.deadlockDetector.checkForDeadlock(rt)
	if got := rt.GetMetrics().BlockedFibers; got != 1 {
		t.Fatalf("BlockedFibers gauge = %d, want 1", got)
	}
	blocked.Unblock()
	rt.deadlockDetector.checkForDeadlock(rt)
	if got := rt.GetMetrics().BlockedFibers; got != 0 {
		t.Fatalf("BlockedFibers gauge = %d after unblock, want 0", got)
	}
}

// TestGetMetricsSurfacesWorkStealingStats is a regression guard for AUDIT ID
// 11: GetMetrics must source steal statistics from the work-stealing scheduler
// rather than always-zero metrics counters.
func TestGetMetricsSurfacesWorkStealingStats(t *testing.T) {
	rt := NewRuntime(scheduler.TypeWorkStealing, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	ws, ok := rt.GetScheduler().(*scheduler.WorkStealingScheduler)
	if !ok {
		t.Fatal("expected WorkStealingScheduler")
	}
	// Spawn a burst of short fibers across 4 workers so steal attempts occur.
	for i := 0; i < 40; i++ {
		if _, err := rt.Spawn(func() { time.Sleep(2 * time.Millisecond) }, "fiber"); err != nil {
			t.Fatal(err)
		}
	}
	// Allow work to flow.
	time.Sleep(200 * time.Millisecond)

	snap := rt.GetMetrics()
	attempts, successes := ws.GetStealStats()
	if snap.TotalStealAttempts != attempts {
		t.Fatalf("snapshot steal attempts = %d, scheduler = %d (not surfaced)", snap.TotalStealAttempts, attempts)
	}
	if snap.TotalStealSuccesses != successes {
		t.Fatalf("snapshot steal successes = %d, scheduler = %d (not surfaced)", snap.TotalStealSuccesses, successes)
	}
	// Sanity: with 4 workers and a burst, at least some steals were attempted.
	if attempts == 0 {
		t.Fatal("no steal attempts recorded; test setup did not induce contention")
	}
}

// TestFiberStateStaysRunningDuringExecution is a regression guard for AUDIT ID
// 9/22: the removed Yield API previously set State=Ready while the fiber's
// goroutine was still running. The fiber's state must remain Running until it
// actually finishes.
func TestFiberStateStaysRunningDuringExecution(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	running := make(chan struct{})
	stateChecked := make(chan struct{})
	id, err := rt.Spawn(func() {
		close(running)
		<-stateChecked
	}, "fiber")
	if err != nil {
		t.Fatal(err)
	}
	<-running
	f, err := rt.GetFiber(id)
	if err != nil {
		t.Fatal(err)
	}
	if f.State != fiber.StateRunning {
		t.Fatalf("fiber state = %s during execution, want Running", f.State)
	}
	close(stateChecked)
}

// TestResetClearsSchedulerQueues is a regression guard for the deep-scrutiny
// finding that Reset() cleared runtime maps/metrics but not scheduler queues,
// causing stale fibers to be re-dispatched after restart.
func TestResetClearsSchedulerQueues(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	// Spawn fibers that block so they stay in the scheduler queue.
	for i := 0; i < 5; i++ {
		if _, err := rt.Spawn(func() { time.Sleep(time.Hour) }, "stuck"); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond) // let them enter the scheduler
	if got := rt.GetScheduler().Size(); got == 0 {
		t.Fatal("expected fibers in scheduler before stop")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = rt.Stop(ctx) // Expected: timeout because fibers are stuck; that's fine for this test.
	rt.Reset()

	// After Reset, the scheduler should have no fibers queued.
	if got := rt.GetScheduler().Size(); got != 0 {
		t.Fatalf("scheduler size after Reset = %d, want 0 (stale fibers not cleared)", got)
	}
	if got := rt.GetScheduler().GetStats().TotalScheduled; got != 0 {
		t.Fatalf("scheduler TotalScheduled after Reset = %d, want 0", got)
	}

	// Restart and verify a clean start works.
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "fresh"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fresh fiber after Reset+Start did not execute (stale queue interference?)")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	_ = rt.Stop(ctx2)
}

func BenchmarkRuntimeSpawn(b *testing.B) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := rt.Spawn(func() {}, "fiber"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRuntimeExecution(b *testing.B) {
	rt := NewRuntime(scheduler.TypeWorkStealing, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		done := make(chan bool)
		if _, err := rt.Spawn(func() {
			done <- true
		}, "fiber"); err != nil {
			b.Fatal(err)
		}
		<-done
	}
}

// TestStopRespectsContextDeadline is a regression guard for AUDIT ID 1:
// rt.Stop must bound its wait for in-flight fibers by the supplied context. A
// fiber that blocks longer than the shutdown deadline must not hold Stop past
// that deadline. The fiber is released after the assertion so the test stays
// hermetic.
func TestStopRespectsContextDeadline(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	if _, err := rt.Spawn(func() {
		close(started)
		<-release
	}, "blocker"); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopStart := time.Now()
	err := rt.Stop(ctx)
	elapsed := time.Since(stopStart)
	if err == nil {
		t.Fatal("expected Stop to report a context error when in-flight fibers outlive the deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Stop blocked for %v despite a 50ms context; in-flight fiber held shutdown", elapsed)
	}
	close(release)
}

// TestRuntimeReleasesCompletedFibers is a regression guard for AUDIT ID 2:
// completed fibers must be reaped from rt.fibers so their bounded stacks do
// not accumulate for the lifetime of the runtime.
func TestRuntimeReleasesCompletedFibers(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	const n = 20
	for i := 0; i < n; i++ {
		if _, err := rt.Spawn(func() {}, "fiber"); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(rt.GetAllFibers()) <= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := len(rt.GetAllFibers())
	if got > 1 {
		t.Fatalf("completed fibers were not reaped; live fiber count = %d (want <= 1)", got)
	}
	snap := rt.GetMetrics()
	if snap.TotalFibersCompleted != int64(n) {
		t.Fatalf("TotalFibersCompleted = %d, want %d", snap.TotalFibersCompleted, n)
	}
	if snap.TotalFibersCreated != int64(n) {
		t.Fatalf("TotalFibersCreated = %d, want %d", snap.TotalFibersCreated, n)
	}
}
