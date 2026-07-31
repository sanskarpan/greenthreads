package greenthreads_test

import (
	"context"
	"errors"
	"testing"
	"time"

	gt "github.com/sanskarpan/greenthreads/pkg/greenthreads"
)

func stopQuick(rt *gt.Runtime) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = rt.Stop(ctx)
}

// TestNewWithOptionsEnforcesMaxFibers exercises the functional-options
// constructor and the atomic fiber cap through the public API.
func TestNewWithOptionsEnforcesMaxFibers(t *testing.T) {
	rt := gt.NewWithOptions(
		gt.WithSchedulerType(gt.FIFO),
		gt.WithNumWorkers(1),
		gt.WithMaxFibers(3),
		gt.WithStackSize(64*1024),
	)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer stopQuick(rt)

	release := make(chan struct{})
	spawned := 0
	var capErr error
	for i := 0; i < 10; i++ {
		_, err := rt.Spawn(func() { <-release }, "held")
		if err != nil {
			capErr = err
			break
		}
		spawned++
	}
	close(release)

	if spawned != 3 {
		t.Fatalf("admitted %d fibers, want 3 (the cap)", spawned)
	}
	if !errors.Is(capErr, gt.ErrMaxFibersReached) {
		t.Fatalf("cap error = %v, want ErrMaxFibersReached", capErr)
	}
}

// TestSpawnBeforeStart verifies the ErrNotStarted sentinel is reachable via the
// public API.
func TestSpawnBeforeStart(t *testing.T) {
	rt := gt.New(gt.FIFO, 1)
	if _, err := rt.Spawn(func() {}, "x"); !errors.Is(err, gt.ErrNotStarted) {
		t.Fatalf("err = %v, want ErrNotStarted", err)
	}
}

// TestDeadlockDetectorAccessor covers the new public accessor.
func TestDeadlockDetectorAccessor(t *testing.T) {
	rt := gt.New(gt.FIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer stopQuick(rt)

	dd := rt.DeadlockDetector()
	if dd == nil {
		t.Fatal("DeadlockDetector() returned nil after Start")
	}
	dd.SetEnabled(true)
	if !dd.IsEnabled() {
		t.Fatal("detector reports disabled after SetEnabled(true)")
	}
}

// TestTypedChannelNonBlocking covers the generic typed channel constructor and
// its non-blocking API (no running fiber required).
func TestTypedChannelNonBlocking(t *testing.T) {
	ch := gt.NewFiberChannelOf[string](2)
	if ch == nil {
		t.Fatal("NewFiberChannelOf returned nil")
	}
	if ok, err := ch.TrySend("a"); err != nil || !ok {
		t.Fatalf("TrySend = (%v, %v), want (true, nil)", ok, err)
	}
	if v, ok := ch.TryReceive(); !ok || v != "a" {
		t.Fatalf("TryReceive = (%q, %v), want (\"a\", true)", v, ok)
	}
	if got := ch.Cap(); got != 2 {
		t.Fatalf("Cap = %d, want 2", got)
	}
	ch.Close()
}

// TestSyncConstructors ensures every re-exported primitive constructor returns
// a usable, non-nil value.
func TestSyncConstructors(t *testing.T) {
	if gt.NewFiberMutex() == nil {
		t.Error("NewFiberMutex returned nil")
	}
	if gt.NewFiberRWMutex() == nil {
		t.Error("NewFiberRWMutex returned nil")
	}
	if gt.NewFiberChannel(1) == nil {
		t.Error("NewFiberChannel returned nil")
	}
	if gt.NewFiberWaitGroup() == nil {
		t.Error("NewFiberWaitGroup returned nil")
	}
	if gt.NewFiberSemaphore(2) == nil {
		t.Error("NewFiberSemaphore returned nil")
	}
}

// TestSentinelErrorsDistinct guards against the re-exported sentinels collapsing
// to the same value.
func TestSentinelErrorsDistinct(t *testing.T) {
	errs := []error{
		gt.ErrNotStarted, gt.ErrAlreadyRunning, gt.ErrStoppedDuringSpawn,
		gt.ErrNilRuntime, gt.ErrMaxFibersReached,
	}
	for i := range errs {
		for j := i + 1; j < len(errs); j++ {
			if errors.Is(errs[i], errs[j]) {
				t.Fatalf("sentinel %d and %d are the same error", i, j)
			}
		}
	}
}

// TestFiberStateConstants sanity-checks the exported lifecycle states are
// distinct and stringify.
func TestFiberStateConstants(t *testing.T) {
	states := []gt.FiberState{
		gt.StateReady, gt.StateRunning, gt.StateBlocked, gt.StateFinished, gt.StateDead,
	}
	seen := map[gt.FiberState]bool{}
	for _, s := range states {
		if seen[s] {
			t.Fatalf("duplicate FiberState value %v", s)
		}
		seen[s] = true
	}
}
