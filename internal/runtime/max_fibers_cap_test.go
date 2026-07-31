package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestMaxFibersCapHoldsUnderConcurrentSpawners is the regression test for SEC-5:
// the WithMaxFibers cap is enforced atomically inside Spawn, so a burst of
// concurrent spawners (as many WebSocket clients would produce) can never admit
// more than the cap. With the fibers held active, exactly `cap` spawns succeed.
func TestMaxFibersCapHoldsUnderConcurrentSpawners(t *testing.T) {
	const cap = 20
	rt := NewRuntimeWithOptions(
		WithSchedulerType(scheduler.TypeFIFO),
		WithNumWorkers(4),
		WithMaxFibers(cap),
	)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	release := make(chan struct{})
	var success, capErrors int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ { // 160 attempts >> cap
				_, err := rt.Spawn(func() { <-release }, "held")
				switch {
				case err == nil:
					atomic.AddInt64(&success, 1)
				case errors.Is(err, ErrMaxFibersReached):
					atomic.AddInt64(&capErrors, 1)
				default:
					t.Errorf("unexpected spawn error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// All admitted fibers block on release and never complete, so the count of
	// successful admissions must be exactly the cap — never more (SEC-5).
	if got := atomic.LoadInt64(&success); got != cap {
		t.Fatalf("successful spawns = %d, want exactly %d (cap breached if greater)", got, cap)
	}
	if got := atomic.LoadInt64(&capErrors); got == 0 {
		t.Fatal("expected some spawns to be rejected with ErrMaxFibersReached")
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = rt.Stop(ctx)
}
