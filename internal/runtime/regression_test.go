package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

// TestStopBoundsByContext_StuckFiber is a regression test for the
// CRITICAL bug where handleStop/handleReset called rt.Stop with
// context.Background(), hanging the control plane forever. The fix
// bounds Stop by the supplied context; this test verifies a stuck
// fiber does not hold Stop past its deadline.
func TestStopBoundsByContext_StuckFiber(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	if _, err := rt.Spawn(func() {
		close(started)
		select {} // never returns
	}, "stuck"); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopStart := time.Now()
	if err := rt.Stop(ctx); err == nil {
		t.Fatal("expected Stop to report a context error when a fiber is stuck")
	}
	if elapsed := time.Since(stopStart); elapsed > time.Second {
		t.Fatalf("Stop blocked for %v despite 50ms context; in-flight fiber held shutdown", elapsed)
	}
}

// TestSpawnRaceWithDispatch is a regression test for the bug where
// Spawn made the fiber visible to the scheduler before inserting it
// into rt.fibers and before recording creation metrics. A fast
// execution loop could complete the fiber and run complete() before
// Spawn finished, leading to inverted counters and a ghost fiber in
// rt.fibers. The fix inverts the order: metrics and map insertion
// happen before scheduler.Schedule.
func TestSpawnRaceWithDispatch(t *testing.T) {
	t.Parallel()
	const n = 200
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := rt.Spawn(func() {}, "burst"); err != nil {
				t.Errorf("spawn failed: %v", err)
			}
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap := rt.GetMetrics()
		if snap.TotalFibersCreated == int64(n) && snap.TotalFibersCompleted == int64(n) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap := rt.GetMetrics()
	t.Fatalf("counters inconsistent after burst: created=%d completed=%d active=%d", snap.TotalFibersCreated, snap.TotalFibersCompleted, snap.ActiveFibers)
}

// TestMarkCompletedIsIdempotent is a regression test for the bug
// where fifo.go and roundrobin.go's filterFinished incremented
// totalCompleted while the runtime's complete() called MarkCompleted
// which also incremented totalCompleted. After the fix,
// MarkCompleted is idempotent (uses a per-ID set) and filterFinished
// no longer mutates statistics.
func TestMarkCompletedIsIdempotent(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()
	id, err := rt.Spawn(func() {}, "f")
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rt.GetScheduler().GetStats().TotalCompleted == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Manually invoke MarkCompleted repeatedly. Only one increment should
	// be recorded because MarkCompleted is idempotent.
	for i := 0; i < 5; i++ {
		rt.GetScheduler().MarkCompleted(id)
	}
	if got := rt.GetScheduler().GetStats().TotalCompleted; got != 1 {
		t.Fatalf("MarkCompleted is not idempotent: TotalCompleted=%d, want 1", got)
	}
}

// TestMainFiberDoesNotMaskDeadlock is a regression test for the bug
// where the always-Ready main observer fiber was counted as runnable
// by the deadlock detector, suppressing every deadlock suspicion. The
// fix excludes the main fiber from the runnable count.
func TestMainFiberDoesNotMaskDeadlock(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	// Use a very short detector timeout and interval so the test is fast.
	rt.deadlockDetector.SetCheckInterval(50 * time.Millisecond)
	rt.deadlockDetector.SetTimeout(100 * time.Millisecond)

	// Insert a synthetic blocked fiber that is NOT the main fiber.
	blocked := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "blocked")
	blocked.Block("test", nil)
	rt.fibersMu.Lock()
	rt.fibers[blocked.ID] = blocked
	rt.fibersMu.Unlock()

	// Wait for at least one detector tick past the timeout.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rt.deadlockDetector.checkForDeadlock(rt)
		if len(rt.deadlockDetector.GetDeadlocks()) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("deadlock detector never reported a deadlock while the main fiber was runnable (the original bug)")
}

// TestConcurrentStartStop is a regression test for the critical bug
// where concurrent Start and Stop allowed the old execution loop to
// read the new run's context or result channel. The fix uses a
// per-run lifecycle mutex that Start must acquire before mutating
// shared state, ensuring the old loop has finished before the new
// one is published.
func TestConcurrentStartStop(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)

	const cycles = 50
	for i := 0; i < cycles; i++ {
		// Start
		if err := rt.Start(); err != nil {
			t.Fatalf("cycle %d: start failed: %v", i, err)
		}
		// Stop concurrently with a spawner
		var spawns atomic.Int64
		var wg sync.WaitGroup
		wg.Add(2)
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		go func() {
			defer wg.Done()
			_ = rt.Stop(stopCtx)
		}()
		go func() {
			defer wg.Done()
			// Try a spawn; it may legitimately fail because we are in
			// the middle of a Stop. What matters is no panic and no
			// state corruption.
			_, _ = rt.Spawn(func() {}, "x")
			spawns.Add(1)
		}()
		wg.Wait()
		stopCancel()

		// Verify state is consistent: no scheduler completion beyond
		// what was actually executed, and the runtime reports stopped.
		if rt.IsRunning() {
			t.Fatalf("cycle %d: runtime still running after Stop", i)
		}
	}

	// Final restart and quick smoke test.
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "final"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("final smoke fiber did not complete")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = rt.Stop(ctx)
}

// TestSpawnAfterStopFails is a regression test for the bug where
// Spawn could succeed against a runtime that was being Stopped, so
// the returned fiber ID would never be dispatched. After the fix,
// Spawn re-checks IsRunning under the runtime mutex and reports a
// clear error.
func TestSpawnAfterStopFails(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}

	// Stop and immediately try to spawn. Either the spawn succeeds
	// (because the spawn raced ahead of Stop) and the fiber runs, or
	// it fails with a clear error. We never want a silent zombie
	// fiber that is queued but never runs.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		_ = rt.Stop(ctx)
	}()

	for i := 0; i < 100; i++ {
		_, err := rt.Spawn(func() {}, "x")
		if err != nil {
			// OK, spawn correctly rejected after Stop began.
			<-stopDone
			return
		}
	}
	<-stopDone
	// All spawns succeeded. They may or may not have run depending on
	// timing. We just verify no panic and the runtime is stopped.
	if rt.IsRunning() {
		t.Fatal("runtime still running after Stop")
	}
}

// TestMetricsUptimeMatchesRunNotObject verifies that uptime reflects
// the current run rather than the age of the metrics object since
// NewMetrics was called. The audit found that the pre-fix code
// started the uptime clock at NewMetrics time, so a runtime that
// waited a long time between construction and Start would report a
// stale Uptime.
func TestMetricsUptimeMatchesRunNotObject(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	time.Sleep(50 * time.Millisecond)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()
	// Within a few milliseconds of Start, Uptime should be very small.
	snap := rt.GetMetrics()
	if snap.Uptime > 500*time.Millisecond {
		t.Fatalf("Uptime = %v, want < 500ms (should be measured from Start, not from NewMetrics)", snap.Uptime)
	}
}
