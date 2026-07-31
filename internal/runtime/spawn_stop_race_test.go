package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

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
