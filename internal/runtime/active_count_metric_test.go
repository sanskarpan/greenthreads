package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestActiveFibersGaugeMatchesExactCount is the regression test for the bounded
// metrics-gauge drift: GetMetrics().ActiveFibers is now sourced from the exact
// activeFiberCount rather than the drift-prone metrics gauge, so the two always
// agree and the value returns to 0 after a full drain.
func TestActiveFibersGaugeMatchesExactCount(t *testing.T) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	}()

	release := make(chan struct{})
	for i := 0; i < 6; i++ {
		if _, err := rt.Spawn(func() { <-release }, "worker"); err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
	}
	// Let the workers get dispatched and park on the channel.
	time.Sleep(50 * time.Millisecond)

	// The gauge must equal the authoritative counter exactly.
	snap := rt.GetMetrics()
	exact := atomic.LoadInt64(&rt.activeFiberCount)
	if snap.ActiveFibers != exact {
		t.Fatalf("ActiveFibers gauge (%d) != activeFiberCount (%d)", snap.ActiveFibers, exact)
	}
	if snap.ActiveFibers != 6 {
		t.Fatalf("ActiveFibers = %d, want 6", snap.ActiveFibers)
	}

	close(release)

	// After the workers finish, the gauge returns to exactly 0.
	deadline := time.Now().Add(time.Second)
	for rt.GetMetrics().ActiveFibers != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rt.GetMetrics().ActiveFibers; got != 0 {
		t.Fatalf("ActiveFibers = %d after drain, want 0", got)
	}
}
