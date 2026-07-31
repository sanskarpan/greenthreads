package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestDetectorFlagsSlotExhaustionDeadlock is the regression test for F3: when
// every worker slot is held by a blocked fiber and a Ready fiber is waiting that
// can never be dispatched (because no slot will ever free), the detector must
// flag a deadlock. The previous logic counted the waiting Ready fiber as
// "progress" and never fired — blind to the exact deadlock it exists to catch.
func TestDetectorFlagsSlotExhaustionDeadlock(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 1) // one worker slot
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	// Model the hung state directly (the same technique the blocked-gauge test
	// uses): the single worker slot is held by a blocked fiber, and a Ready
	// fiber is queued that can never be dispatched.
	blocked := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "receiver")
	blocked.Block("waiting on channel with no runnable sender", nil)
	ready := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender") // StateReady

	rt.fibersMu.Lock()
	rt.fibers[blocked.ID] = blocked
	rt.fibers[ready.ID] = ready
	rt.fibersMu.Unlock()

	// Use a tiny timeout so a persisted no-progress state flags quickly. The
	// detector's lastProgress was set at Start; letting >1ms elapse guarantees
	// the no-progress interval exceeds the threshold on the manual scan.
	rt.deadlockDetector.SetTimeout(time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	rt.deadlockDetector.checkForDeadlock(rt)

	if got := len(rt.deadlockDetector.GetDeadlocks()); got == 0 {
		t.Fatal("detector did not flag the slot-exhaustion deadlock " +
			"(1 worker, 1 blocked fiber holding the slot, 1 ready fiber that can never run)")
	}
}

// TestDetectorNoFalsePositiveWithFreeSlot guards against over-firing: when a
// worker slot is free, a waiting Ready fiber can still be dispatched, so a lone
// blocked fiber must NOT be reported as a deadlock.
func TestDetectorNoFalsePositiveWithFreeSlot(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4) // four slots, only one used
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	blocked := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "blocked")
	blocked.Block("temporarily waiting", nil)
	ready := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "ready") // could be dispatched

	rt.fibersMu.Lock()
	rt.fibers[blocked.ID] = blocked
	rt.fibers[ready.ID] = ready
	rt.fibersMu.Unlock()

	rt.deadlockDetector.SetTimeout(0)
	rt.deadlockDetector.checkForDeadlock(rt)

	if got := len(rt.deadlockDetector.GetDeadlocks()); got != 0 {
		t.Fatalf("detector falsely flagged a deadlock with %d free worker slots: got %d deadlocks, want 0", 3, got)
	}
}
