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

// TestRuntimeSurface exercises the wrapper's forwarding methods end to end so
// the public API is covered, not just re-exported.
func TestRuntimeSurface(t *testing.T) {
	rt := gt.New(gt.WorkStealing, 2)

	// StartWithContext ties the run to a context.
	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.StartWithContext(ctx); err != nil {
		t.Fatalf("StartWithContext: %v", err)
	}
	if !rt.IsRunning() {
		t.Fatal("IsRunning() = false after start")
	}
	rt.SetStackSize(48 * 1024)

	// Spawn a batch through a SpawnGroup and wait.
	sg := rt.NewSpawnGroup()
	done := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		if _, err := sg.Spawn(func() { done <- struct{}{} }, "w"); err != nil {
			t.Fatalf("group spawn: %v", err)
		}
	}
	if ids := sg.IDs(); len(ids) != 4 {
		t.Fatalf("SpawnGroup.IDs len = %d, want 4", len(ids))
	}
	if errs := sg.Wait(); len(errs) != 0 {
		t.Fatalf("SpawnGroup.Wait errors: %v", errs)
	}
	for i := 0; i < 4; i++ {
		<-done
	}

	// SpawnWithTimeout returns promptly; SpawnWithResult delivers a value.
	if _, err := rt.SpawnWithTimeout(func() {}, "quick", 50*time.Millisecond); err != nil {
		t.Fatalf("SpawnWithTimeout: %v", err)
	}
	_, ch, err := rt.SpawnWithResult(func() interface{} { return 21 * 2 }, "calc")
	if err != nil {
		t.Fatalf("SpawnWithResult: %v", err)
	}
	if v := <-ch; v != 42 {
		t.Fatalf("SpawnWithResult value = %v, want 42", v)
	}

	// Inspection surface.
	if snap := rt.GetMetrics(); snap.TotalFibersCreated == 0 {
		t.Fatal("GetMetrics reports zero created fibers")
	}
	if lt := rt.GetLifetimeMetrics(); lt.TotalFibersCreated == 0 {
		t.Fatal("GetLifetimeMetrics reports zero created fibers")
	}
	if evs := rt.GetEvents(10); len(evs) == 0 {
		t.Fatal("GetEvents returned nothing")
	}
	_ = rt.GetAllFibers() // snapshot; may be empty after drain
	_ = rt.SchedulerStats()

	// Deadlock detector controls.
	dd := rt.DeadlockDetector()
	dd.SetCheckInterval(10 * time.Millisecond)
	dd.SetTimeout(50 * time.Millisecond)
	dd.SetEnabled(true)
	_ = dd.GetDeadlocks()
	dd.ClearDeadlocks()

	cancel()
	stopQuick(rt)

	// Reset after stop, then confirm a fresh run works.
	if err := rt.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := rt.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if _, err := rt.Spawn(func() {}, "post-reset"); err != nil {
		t.Fatalf("spawn after reset: %v", err)
	}
	stopQuick(rt)
}

// TestGetFiberHandle covers the read-only inspection handle path.
func TestGetFiberHandle(t *testing.T) {
	rt := gt.New(gt.FIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer stopQuick(rt)

	release := make(chan struct{})
	id, err := rt.Spawn(func() { <-release }, "held")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	h, ok := rt.GetFiber(id)
	if !ok {
		t.Fatal("GetFiber returned not-found for a live fiber")
	}
	if h.GetID() != id {
		t.Fatalf("handle ID = %d, want %d", h.GetID(), id)
	}
	if h.GetName() != "held" {
		t.Fatalf("handle name = %q, want %q", h.GetName(), "held")
	}
	close(release)
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
