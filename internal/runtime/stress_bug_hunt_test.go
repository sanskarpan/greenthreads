package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
	fibersync "github.com/sanskarpan/greenthreads/internal/sync"
)

type gatedSchedule struct {
	scheduler.Scheduler
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedSchedule) Schedule(f *fiber.Fiber) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.Scheduler.Schedule(f)
}

func waitFor(t *testing.T, d time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before deadline")
}

func TestStressLifecycleEdges(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 2)
	var nilCtx context.Context
	if err := rt.Stop(nilCtx); err != nil {
		t.Fatalf("stop before start: %v", err)
	}
	for cycle := 0; cycle < 20; cycle++ {
		if err := rt.Start(); err != nil {
			t.Fatalf("cycle %d start: %v", cycle, err)
		}
		if err := rt.Start(); err == nil {
			t.Fatalf("cycle %d second Start succeeded", cycle)
		}
		done := make(chan struct{})
		if _, err := rt.Spawn(func() { close(done) }, "cycle"); err != nil {
			t.Fatalf("cycle %d spawn: %v", cycle, err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("cycle %d fiber did not run", cycle)
		}
		if err := rt.Stop(nilCtx); err != nil {
			t.Fatalf("cycle %d nil-context Stop: %v", cycle, err)
		}
		if err := rt.Stop(context.Background()); err != nil {
			t.Fatalf("cycle %d repeated Stop: %v", cycle, err)
		}
		if rt.IsRunning() {
			t.Fatalf("cycle %d remained running", cycle)
		}
	}
}

func TestStressSpawnDuringShutdownIsRejected(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	gate := &gatedSchedule{
		Scheduler: rt.scheduler,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	rt.scheduler = gate
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	spawnErr := make(chan error, 1)
	go func() {
		_, err := rt.Spawn(func() {}, "racing")
		spawnErr <- err
	}()
	<-gate.entered
	if err := rt.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(gate.release)
	if err := <-spawnErr; err == nil {
		t.Fatal("Spawn succeeded after shutdown completed, leaving accepted work with no dispatcher")
	}
	if got := gate.Size(); got != 0 {
		t.Fatalf("shutdown race left %d queued fibers", got)
	}
}

func TestStressConcurrentSpawnsAndSnapshots(t *testing.T) {
	t.Parallel()
	const producers = 16
	const perProducer = 20
	rt := NewRuntime(scheduler.TypeWorkStealing, 8)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()
	var ran atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				if _, err := rt.Spawn(func() { ran.Add(1) }, "concurrent"); err != nil {
					t.Errorf("spawn: %v", err)
					return
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		fibers := rt.GetAllFibers()
		for j := 1; j < len(fibers); j++ {
			if fibers[j-1].ID >= fibers[j].ID {
				t.Fatalf("GetAllFibers returned non-increasing IDs: %d then %d", fibers[j-1].ID, fibers[j].ID)
			}
		}
	}
	wg.Wait()
	want := int64(producers * perProducer)
	waitFor(t, 3*time.Second, func() bool { return ran.Load() == want })
	waitFor(t, 3*time.Second, func() bool { return rt.GetMetrics().TotalFibersCompleted == want })
	if got := rt.GetMetrics().TotalFibersCreated; got != want {
		t.Fatalf("created=%d, want %d", got, want)
	}
}

func TestStressBlockedSyncFiberShutdownIsBounded(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	fc := fibersync.NewFiberChannel(0)
	gate := make(chan struct{})
	done := make(chan struct{})
	var id fiber.FiberID
	id, err := rt.Spawn(func() {
		<-gate
		live, getErr := rt.GetFiberDirect(id)
		if getErr == nil {
			_, _ = fc.Receive(live)
		}
		close(done)
	}, "blocked-receiver")
	if err != nil {
		t.Fatal(err)
	}
	close(gate)
	waitFor(t, time.Second, func() bool { return fc.RecvQueueSize() == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = rt.Stop(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop error=%v, want deadline exceeded", err)
	}
	fc.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closing the sync primitive did not release the abandoned fiber")
	}
}

func TestStressResetDuringInflightDoesNotCorruptAccounting(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := rt.Spawn(func() {
		close(started)
		<-release
	}, "inflight"); err != nil {
		t.Fatal(err)
	}
	<-started
	_ = rt.Reset()
	close(release)
	waitFor(t, time.Second, func() bool { return rt.GetMetrics().TotalFibersCompleted > 0 })
	snap := rt.GetMetrics()
	if snap.TotalFibersCompleted > snap.TotalFibersCreated {
		t.Fatalf("Reset produced impossible counters: created=%d completed=%d", snap.TotalFibersCreated, snap.TotalFibersCompleted)
	}
	_ = rt.Stop(context.Background())
}

func TestStressSetStackSizeAfterStartAffectsFutureFibers(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()
	block := make(chan struct{})
	started := make(chan struct{})
	if _, err := rt.Spawn(func() { close(started); <-block }, "occupy-worker"); err != nil {
		t.Fatal(err)
	}
	<-started
	rt.SetStackSize(fiber.MinStackSize * 2)
	id, err := rt.Spawn(func() {}, "new-stack-size")
	if err != nil {
		t.Fatal(err)
	}
	f, err := rt.GetFiber(id)
	if err != nil {
		t.Fatal(err)
	}
	if f.StackSize != fiber.MinStackSize*2 {
		t.Fatalf("stack size=%d, want %d", f.StackSize, fiber.MinStackSize*2)
	}
	close(block)
}

func TestStressReapOrderAndMissingLookup(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 1)
	if _, err := rt.GetFiber(fiber.FiberID(^uint64(0))); err == nil {
		t.Fatal("GetFiber found a nonexistent ID")
	}
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reap")
	rt.fibersMu.Lock()
	rt.fibers[f.ID] = f
	rt.fibersMu.Unlock()
	f.Run()
	result := fiberResult{fiber: f, duration: time.Millisecond}
	rt.complete(result)
	rt.complete(result)
	if _, err := rt.GetFiber(f.ID); err == nil {
		t.Fatal("completed fiber remained in runtime map")
	}
	if got := rt.GetMetrics().TotalFibersCompleted; got != 1 {
		t.Fatalf("duplicate reap recorded %d completions", got)
	}
	if got := rt.GetScheduler().GetStats().TotalCompleted; got != 1 {
		t.Fatalf("duplicate reap recorded %d scheduler completions", got)
	}
}

func TestStressConcurrentStops(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- rt.Stop(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Stop: %v", err)
		}
	}
	if rt.IsRunning() {
		t.Fatal("runtime running after concurrent Stops")
	}
}
