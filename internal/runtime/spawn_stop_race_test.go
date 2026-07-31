package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestCompleteDecrementsFiberCountOnlyOnce is the deterministic regression test
// for the third double-decrement path: the counter decrement is owned by
// whichever path first removes the fiber from rt.fibers. If a fiber is dispatched
// and completed in the Schedule->re-check window, complete() removes it; a later
// stopping Spawn-rollback (or a second complete) must find it absent and NOT
// decrement again. Modeled here by removing the fiber once, then invoking a
// second remover — the counter must not go below the single expected decrement.
func TestCompleteDecrementsFiberCountOnlyOnce(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "victim")

	rt.fibersMu.Lock()
	rt.fibers[f.ID] = f
	rt.admitted[f.ID] = struct{}{}
	rt.fibersMu.Unlock()
	atomic.StoreInt64(&rt.activeFiberCount, 1)

	// First remover (the fiber's own completion) owns the single decrement.
	rt.complete(fiberResult{fiber: f})
	if got := atomic.LoadInt64(&rt.activeFiberCount); got != 0 {
		t.Fatalf("after first complete: activeFiberCount=%d, want 0", got)
	}

	// A second remover for the same fiber (e.g. a concurrent stopping
	// Spawn-rollback that lost the race) must not decrement again.
	rt.complete(fiberResult{fiber: f})
	if got := atomic.LoadInt64(&rt.activeFiberCount); got != 0 {
		t.Fatalf("second remover double-decremented: activeFiberCount=%d, want 0", got)
	}
}

// TestSpawnStopRaceDoesNotCorruptFiberCount is the regression test for BOTH
// directions of the Spawn/Stop accounting race found in review:
//   - double-decrement: reapPending and Spawn's rollback both decrement one
//     fiber, driving activeFiberCount NEGATIVE (observed -14); and
//   - zero-decrement leak: a fiber committed AFTER reapPending scanned is never
//     reaped and never completes, leaking the counter POSITIVE and permanently
//     (observed growing 0 -> 8 across cycles).
//
// After every spawner has returned and the runtime has fully drained, the
// counter must be EXACTLY 0 — not merely non-negative. Reusing one runtime
// across cycles (Stop -> Reset -> Start) also proves the leak does not
// accumulate. Run under `go test -race` to also catch data races.
func TestSpawnStopRaceDoesNotCorruptFiberCount(t *testing.T) {
	const iterations = 150
	const spawners = 8
	const perSpawner = 25

	rt := NewRuntimeWithOptions(
		WithSchedulerType(scheduler.TypeFIFO),
		WithNumWorkers(2),
		WithMaxFibers(10_000),
	)

	for iter := 0; iter < iterations; iter++ {
		if err := rt.Start(); err != nil {
			t.Fatalf("iter %d: start: %v", iter, err)
		}

		var wg sync.WaitGroup
		for g := 0; g < spawners; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perSpawner; i++ {
					_, _ = rt.Spawn(func() {}, "racer")
				}
			}()
		}

		// Stop concurrently with the in-flight spawners, generously so the
		// deadline path is rare and the counter can fully settle.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = rt.Stop(ctx)
		cancel()
		wg.Wait()

		// Wait for the counter to settle to exactly 0 after all completions.
		deadline := time.Now().Add(2 * time.Second)
		for atomic.LoadInt64(&rt.activeFiberCount) != 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if got := atomic.LoadInt64(&rt.activeFiberCount); got != 0 {
			t.Fatalf("iter %d: activeFiberCount = %d after full drain, want exactly 0 "+
				"(negative => double-decrement; positive => committed-after-reap leak)", iter, got)
		}

		if err := rt.Reset(); err != nil {
			t.Fatalf("iter %d: reset: %v", iter, err)
		}
	}
}

// TestSpawnStopRaceWithInFlightFibers targets the third decrement path: a fiber
// dispatched and completed (or in-flight) in the Schedule -> re-check window,
// so complete() and Spawn's stopping rollback could both decrement one fiber.
// Nonzero-duration bodies keep fibers in-flight while Stop runs, widening that
// window. Exercised across all schedulers; the counter must settle to exactly 0
// (a leftover double-decrement leaves it permanently negative).
func TestSpawnStopRaceWithInFlightFibers(t *testing.T) {
	scheds := []scheduler.SchedulerType{
		scheduler.TypeFIFO, scheduler.TypePriority, scheduler.TypeWorkStealing,
	}
	for _, st := range scheds {
		st := st
		t.Run(st.String(), func(t *testing.T) {
			for iter := 0; iter < 40; iter++ {
				rt := NewRuntimeWithOptions(
					WithSchedulerType(st),
					WithNumWorkers(4),
					WithMaxFibers(10_000),
				)
				if err := rt.Start(); err != nil {
					t.Fatalf("iter %d: start: %v", iter, err)
				}

				// Concurrently sample the counter to catch a transient negative
				// (the double-decrement shows up mid-flight, not only at rest).
				var minSeen int64
				sampleStop := make(chan struct{})
				var sampler sync.WaitGroup
				sampler.Add(1)
				go func() {
					defer sampler.Done()
					for {
						select {
						case <-sampleStop:
							return
						default:
							v := atomic.LoadInt64(&rt.activeFiberCount)
							for {
								m := atomic.LoadInt64(&minSeen)
								if v >= m || atomic.CompareAndSwapInt64(&minSeen, m, v) {
									break
								}
							}
						}
					}
				}()

				var wg sync.WaitGroup
				for g := 0; g < 8; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := 0; i < 15; i++ {
							// A short-but-nonzero body means the fiber is often
							// still in-flight when Spawn's rollback runs.
							_, _ = rt.Spawn(func() { time.Sleep(time.Millisecond) }, "racer")
						}
					}()
				}

				// Let some fibers get dispatched before Stop begins.
				time.Sleep(2 * time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = rt.Stop(ctx)
				cancel()
				wg.Wait()

				deadline := time.Now().Add(3 * time.Second)
				for atomic.LoadInt64(&rt.activeFiberCount) != 0 && time.Now().Before(deadline) {
					time.Sleep(2 * time.Millisecond)
				}
				close(sampleStop)
				sampler.Wait()

				if m := atomic.LoadInt64(&minSeen); m < 0 {
					t.Fatalf("scheduler=%v iter=%d: activeFiberCount went negative (%d) mid-flight (double-decrement)",
						st, iter, m)
				}
				if got := atomic.LoadInt64(&rt.activeFiberCount); got != 0 {
					t.Fatalf("scheduler=%v iter=%d: activeFiberCount=%d after drain, want exactly 0",
						st, iter, got)
				}
			}
		})
	}
}
