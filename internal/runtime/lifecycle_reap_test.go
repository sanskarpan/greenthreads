package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestStopDoesNotRunUndispatchedFibersInNextRun is the regression test for F1:
// fibers admitted but never dispatched before Stop must not survive in the
// scheduler queue and execute in a subsequent Start (without an intervening
// Reset).
func TestStopDoesNotRunUndispatchedFibersInNextRun(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	release := make(chan struct{})
	var ran int32

	// Occupy the single worker slot so the fibers spawned after it cannot be
	// dispatched and remain Ready in the scheduler queue.
	if _, err := rt.Spawn(func() { <-release }, "blocker"); err != nil {
		t.Fatalf("spawn blocker: %v", err)
	}
	time.Sleep(40 * time.Millisecond) // let the loop dispatch the blocker

	for i := 0; i < 5; i++ {
		if _, err := rt.Spawn(func() { atomic.AddInt32(&ran, 1) }, "queued"); err != nil {
			t.Fatalf("spawn queued %d: %v", i, err)
		}
	}

	// The blocker is stuck, so Stop takes the deadline path. reapPending must
	// still clear the un-dispatched fibers from the scheduler.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_ = rt.Stop(ctx)
	cancel()
	close(release)
	time.Sleep(40 * time.Millisecond) // let the blocker finish

	// Second run WITHOUT Reset. The queued run-1 fibers must not run here.
	if err := rt.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	_ = rt.Stop(ctx2)
	cancel2()

	if got := atomic.LoadInt32(&ran); got != 0 {
		t.Fatalf("un-dispatched fibers from run 1 executed in run 2: ran=%d, want 0", got)
	}
}

// TestUndispatchedFibersDoNotLeakFiberCap is the regression test for F2:
// un-dispatched fibers reaped at Stop must decrement the cap counter, so
// WithMaxFibers is not permanently exhausted by phantom count.
func TestUndispatchedFibersDoNotLeakFiberCap(t *testing.T) {
	rt := NewRuntimeWithOptions(
		WithSchedulerType(scheduler.TypeFIFO),
		WithNumWorkers(1),
		WithMaxFibers(50),
	)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	release := make(chan struct{})
	if _, err := rt.Spawn(func() { <-release }, "blocker"); err != nil {
		t.Fatalf("spawn blocker: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	for i := 0; i < 10; i++ {
		if _, err := rt.Spawn(func() {}, "queued"); err != nil {
			t.Fatalf("spawn queued %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_ = rt.Stop(ctx)
	cancel()
	close(release)

	// After the in-flight blocker finishes and the queued fibers are reaped,
	// the cap counter must settle back to zero.
	deadline := time.Now().Add(1 * time.Second)
	for atomic.LoadInt64(&rt.activeFiberCount) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt64(&rt.activeFiberCount); got != 0 {
		t.Fatalf("activeFiberCount leaked after Stop: got %d, want 0", got)
	}

	// The cap must still admit new work after a Reset.
	if err := rt.Reset(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := rt.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if _, err := rt.Spawn(func() {}, "post-reset"); err != nil {
		t.Fatalf("spawn after reset should succeed, got: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	_ = rt.Stop(ctx2)
	cancel2()
}

// TestFiberPanicIncrementsPanicMetric is the regression test for the phantom
// greenthreads_fiber_panics_total metric: a recovered fiber panic must be
// counted.
func TestFiberPanicIncrementsPanicMetric(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = rt.Stop(ctx)
		cancel()
	}()

	if _, err := rt.Spawn(func() { panic("boom") }, "panicker"); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)
	for rt.GetMetrics().TotalFiberPanics == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rt.GetMetrics().TotalFiberPanics; got != 1 {
		t.Fatalf("TotalFiberPanics = %d, want 1", got)
	}
}
