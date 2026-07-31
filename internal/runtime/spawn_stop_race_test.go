package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestSpawnStopRaceDoesNotCorruptFiberCount is the regression test for the
// double-decrement race found in review: when Spawn races Stop, a fiber still
// mid-Spawn (inserted as Ready but not yet committed) must be decremented by
// exactly one of {Spawn rollback, reapPending} — never both. Before the fix,
// both decremented and activeFiberCount went negative (observed -14).
//
// Run under `go test -race` to also catch any residual data race.
func TestSpawnStopRaceDoesNotCorruptFiberCount(t *testing.T) {
	const iterations = 150
	const spawners = 8
	const perSpawner = 25

	for iter := 0; iter < iterations; iter++ {
		rt := NewRuntimeWithOptions(
			WithSchedulerType(scheduler.TypeFIFO),
			WithNumWorkers(2),
			WithMaxFibers(10_000),
		)
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

		// Stop concurrently with the in-flight spawners.
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_ = rt.Stop(ctx)
		cancel()
		wg.Wait()

		// Allow any late in-flight completions to settle.
		deadline := time.Now().Add(500 * time.Millisecond)
		for atomic.LoadInt64(&rt.activeFiberCount) > 0 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}

		if got := atomic.LoadInt64(&rt.activeFiberCount); got < 0 {
			t.Fatalf("iter %d: activeFiberCount corrupted (negative): %d", iter, got)
		}
	}
}
