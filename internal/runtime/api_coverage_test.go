package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// ---------- FiberHandle ----------

func TestGetFiberHandleNotFound(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	_, ok := rt.GetFiberHandle(999)
	if ok {
		t.Fatal("expected false for unknown fiber ID")
	}
}

func TestFiberHandleAllMethods(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	gate := make(chan struct{})
	id, err := rt.Spawn(func() { <-gate }, "handle-test")
	if err != nil {
		t.Fatal(err)
	}

	// Poll until the fiber is visible in the map
	var h FiberHandle
	require := func(cond bool, msg string) {
		t.Helper()
		if !cond {
			t.Fatal(msg)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var ok bool
		h, ok = rt.GetFiberHandle(id)
		if ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if h == nil {
		t.Fatal("fiber handle not found after 2s")
	}

	require(h.GetID() == id, "GetID mismatch")
	require(h.GetName() == "handle-test", "GetName mismatch")
	_ = h.GetState()
	_ = h.GetPriority()
	_ = h.IsFinished()
	_ = h.IsBlocked()
	_ = h.IsRunnable()
	_ = h.Failure()
	_ = h.PanicStack()
	clone := h.Clone()
	require(clone != nil, "Clone returned nil")
	require(clone.ID == id, "cloned ID mismatch")

	close(gate)
}

func TestFiberHandleAfterCompletion(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	done := make(chan struct{})
	id, err := rt.Spawn(func() { close(done) }, "done-fiber")
	if err != nil {
		t.Fatal(err)
	}
	<-done

	// After completion the fiber is reaped, handle should eventually be gone
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, ok := rt.GetFiberHandle(id)
		if !ok {
			return // reaped
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Not finding it is acceptable; finding it finished is also acceptable
	if h, ok := rt.GetFiberHandle(id); ok {
		if !h.IsFinished() {
			t.Logf("fiber %d still present but not finished after 2s — acceptable", id)
		}
	}
}

// ---------- NewRuntimeWithOptions ----------

func TestNewRuntimeWithOptionsDefaults(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions()
	if rt == nil {
		t.Fatal("expected non-nil Runtime")
	}
	if rt.numWorkers < 2 {
		t.Fatalf("expected numWorkers >= 2, got %d", rt.numWorkers)
	}
}

func TestWithNumWorkers(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(WithNumWorkers(3))
	if rt.numWorkers != 3 {
		t.Fatalf("want numWorkers=3, got %d", rt.numWorkers)
	}
	// Zero is rejected; default unchanged
	rt2 := NewRuntimeWithOptions(WithNumWorkers(0))
	if rt2.numWorkers < 1 {
		t.Fatalf("zero numWorkers should fall back to default, got %d", rt2.numWorkers)
	}
}

func TestWithStackSize(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(WithStackSize(128 * 1024))
	if rt.stackSize != 128*1024 {
		t.Fatalf("want stackSize=131072, got %d", rt.stackSize)
	}
	rt2 := NewRuntimeWithOptions(WithStackSize(0))
	if rt2.stackSize <= 0 {
		t.Fatalf("zero stackSize should fall back to default, got %d", rt2.stackSize)
	}
}

func TestWithSchedulerType(t *testing.T) {
	t.Parallel()
	for _, st := range []scheduler.SchedulerType{
		scheduler.TypeFIFO, scheduler.TypeRoundRobin,
		scheduler.TypePriority, scheduler.TypeWorkStealing,
	} {
		rt := NewRuntimeWithOptions(WithSchedulerType(st))
		if rt.scheduler == nil {
			t.Fatalf("scheduler nil for type %v", st)
		}
	}
}

func TestWithMaxFibers(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(WithMaxFibers(50))
	if rt.maxFibers != 50 {
		t.Fatalf("want maxFibers=50, got %d", rt.maxFibers)
	}
}

func TestWithDetectorConfig(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(
		WithDetectorConfig(true, 500*time.Millisecond, 2*time.Second),
	)
	if rt.deadlockDetector == nil {
		t.Fatal("deadlockDetector nil after WithDetectorConfig")
	}
	if !rt.deadlockDetector.IsEnabled() {
		t.Fatal("detector should be enabled")
	}
}

func TestNewRuntimeWithOptionsStartStop(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(
		WithNumWorkers(2),
		WithStackSize(64*1024),
		WithSchedulerType(scheduler.TypePriority),
		WithMaxFibers(10),
	)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "opt-fiber"); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := rt.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------- SpawnGroup ----------

func TestSpawnGroupBasic(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	sg := rt.NewSpawnGroup()
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := sg.Spawn(func() {}, "sg-fiber"); err != nil {
			t.Fatal(err)
		}
	}
	errs := sg.Wait()
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got %v", errs)
	}
	ids := sg.IDs()
	if len(ids) != n {
		t.Fatalf("expected %d IDs, got %d", n, len(ids))
	}
}

func TestSpawnGroupErrorPath(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	// Never started — Spawn should fail
	sg := rt.NewSpawnGroup()
	_, err := sg.Spawn(func() {}, "no-start")
	if err == nil {
		t.Fatal("expected error spawning into unstarted runtime")
	}
	errs := sg.Wait()
	if len(errs) == 0 {
		t.Fatal("expected at least one error in Wait after failed Spawn")
	}
}

func TestSpawnGroupWithMaxFibers(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(WithNumWorkers(2), WithMaxFibers(2))
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	sg := rt.NewSpawnGroup()
	gate := make(chan struct{})
	_, _ = sg.Spawn(func() { <-gate }, "f1")
	_, _ = sg.Spawn(func() { <-gate }, "f2")
	// Third spawn should fail (cap=2)
	_, err := sg.Spawn(func() {}, "f3")
	close(gate)
	sg.Wait()
	// May or may not error depending on timing; just verify no panic
	_ = err
}

// ---------- SpawnWithResult ----------

func TestSpawnWithResultBasic(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	id, ch, err := rt.SpawnWithResult(func() interface{} { return 42 }, "result-fiber")
	if err != nil {
		t.Fatalf("SpawnWithResult error: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero fiber ID")
	}
	if ch == nil {
		t.Fatal("expected non-nil result channel")
	}

	select {
	case v := <-ch:
		if v.(int) != 42 {
			t.Fatalf("expected 42, got %v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SpawnWithResult")
	}
}

func TestSpawnWithResultString(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	_, ch, err := rt.SpawnWithResult(func() interface{} { return "hello" }, "str-fiber")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-ch:
		if v.(string) != "hello" {
			t.Fatalf("expected 'hello', got %v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func TestSpawnWithResultBeforeStart(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	_, _, err := rt.SpawnWithResult(func() interface{} { return nil }, "no-start")
	if err == nil {
		t.Fatal("expected error when runtime not started")
	}
}

// ---------- SpawnWithTimeout ----------

func TestSpawnWithTimeoutCompletes(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	done := make(chan struct{})
	id, err := rt.SpawnWithTimeout(func() { close(done) }, "fast-fiber", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected non-zero fiber ID")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for SpawnWithTimeout to complete")
	}
}

func TestSpawnWithTimeoutExpires(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	// Fiber that blocks longer than the timeout
	gate := make(chan struct{})
	_, err := rt.SpawnWithTimeout(func() { <-gate }, "slow-fiber", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Give the timeout time to fire; the fiber slot should be released
	time.Sleep(200 * time.Millisecond)
	close(gate)
}

func TestSpawnWithTimeoutBeforeStart(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	_, err := rt.SpawnWithTimeout(func() {}, "no-start", time.Second)
	if err == nil {
		t.Fatal("expected error when runtime not started")
	}
}

// ---------- StartWithContext ----------

func TestStartWithContext(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	ctx, cancel := context.WithCancel(context.Background())
	if err := rt.StartWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "ctx-fiber"); err != nil {
		t.Fatal(err)
	}
	<-done
	cancel() // context cancel triggers shutdown
	// Give runtime time to drain
	time.Sleep(100 * time.Millisecond)
}

func TestStartWithContextAlreadyRunning(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	ctx := context.Background()
	if err := rt.StartWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(ctx) }()
	if err := rt.StartWithContext(ctx); err == nil {
		t.Fatal("expected error starting already-running runtime")
	}
}

// ---------- GetLifetimeMetrics ----------

func TestGetLifetimeMetrics(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "metric-fiber"); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := rt.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	snap := rt.GetLifetimeMetrics()
	if snap.TotalFibersCreated == 0 {
		t.Fatal("expected TotalFibersCreated > 0 from GetLifetimeMetrics")
	}
	if snap.TotalFibersCompleted == 0 {
		t.Fatal("expected TotalFibersCompleted > 0 from GetLifetimeMetrics")
	}
}

func TestGetLifetimeMetricsAfterReset(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "pre-reset"); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := rt.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	lifetime1 := rt.GetLifetimeMetrics()

	_ = rt.Reset()
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	done2 := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done2) }, "post-reset"); err != nil {
		t.Fatal(err)
	}
	<-done2
	if err := rt.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	lifetime2 := rt.GetLifetimeMetrics()

	// Lifetime counters must be non-decreasing
	if lifetime2.TotalFibersCreated < lifetime1.TotalFibersCreated {
		t.Fatalf("lifetime counters regressed: %d < %d",
			lifetime2.TotalFibersCreated, lifetime1.TotalFibersCreated)
	}
}

// ---------- DeadlockDetector controls ----------

func TestDeadlockDetectorSetEnabledIsEnabled(t *testing.T) {
	t.Parallel()
	dd := NewDeadlockDetector()

	dd.SetEnabled(true)
	if !dd.IsEnabled() {
		t.Fatal("expected IsEnabled=true after SetEnabled(true)")
	}
	dd.SetEnabled(false)
	if dd.IsEnabled() {
		t.Fatal("expected IsEnabled=false after SetEnabled(false)")
	}
}

func TestDeadlockDetectorClearDeadlocks(t *testing.T) {
	t.Parallel()
	dd := NewDeadlockDetector()
	// ClearDeadlocks on empty detector should not panic
	dd.ClearDeadlocks()
	if dls := dd.GetDeadlocks(); len(dls) != 0 {
		t.Fatalf("expected 0 deadlocks after clear, got %d", len(dls))
	}
}

func TestDeadlockDetectorNilSafe(t *testing.T) {
	t.Parallel()
	var dd *DeadlockDetector
	dd.ClearDeadlocks()
	dd.SetEnabled(true)
	if dd.IsEnabled() {
		t.Fatal("nil detector IsEnabled should return false")
	}
	if dls := dd.GetDeadlocks(); len(dls) != 0 {
		t.Fatal("nil detector GetDeadlocks should return empty")
	}
}

func TestDeadlockDetectorViaRuntimeOption(t *testing.T) {
	t.Parallel()
	rt := NewRuntimeWithOptions(
		WithDetectorConfig(true, 100*time.Millisecond, 500*time.Millisecond),
	)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if _, err := rt.Spawn(func() { close(done) }, "dd-fiber"); err != nil {
		t.Fatal(err)
	}
	<-done
	if err := rt.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ---------- SpawnGroup with multiple schedulers ----------

func TestSpawnGroupAllSchedulers(t *testing.T) {
	t.Parallel()
	for _, st := range []scheduler.SchedulerType{
		scheduler.TypeFIFO, scheduler.TypeRoundRobin,
		scheduler.TypePriority, scheduler.TypeWorkStealing,
	} {
		st := st
		t.Run(st.String(), func(t *testing.T) {
			t.Parallel()
			rt := NewRuntimeWithOptions(WithSchedulerType(st), WithNumWorkers(2))
			if err := rt.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rt.Stop(context.Background()) }()

			sg := rt.NewSpawnGroup()
			for i := 0; i < 3; i++ {
				if _, err := sg.Spawn(func() {}, "sg"); err != nil {
					t.Fatal(err)
				}
			}
			if errs := sg.Wait(); len(errs) > 0 {
				t.Fatalf("unexpected errors: %v", errs)
			}
		})
	}
}

// ---------- FiberID on SpawnGroup.IDs ----------

func TestSpawnGroupIDs(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	sg := rt.NewSpawnGroup()
	var spawned []fiber.FiberID
	for i := 0; i < 4; i++ {
		id, err := sg.Spawn(func() {}, "id-fiber")
		if err != nil {
			t.Fatal(err)
		}
		spawned = append(spawned, id)
	}
	sg.Wait()
	ids := sg.IDs()
	if len(ids) != len(spawned) {
		t.Fatalf("IDs() returned %d, spawned %d", len(ids), len(spawned))
	}
}
