package fiber

import (
	"testing"
	"time"
)

func TestNewFiber(t *testing.T) {
	t.Parallel()
	fn := func() {
		// Test function
	}

	f := NewFiber(fn, DefaultStackSize, "test-fiber")

	if f == nil {
		t.Fatal("NewFiber returned nil")
	}

	if f.ID == 0 {
		t.Error("Fiber ID should not be 0")
	}

	if f.Name != "test-fiber" {
		t.Errorf("Expected name 'test-fiber', got '%s'", f.Name)
	}

	if f.State != StateReady {
		t.Errorf("Expected state Ready, got %s", f.State)
	}

	if f.StackSize != DefaultStackSize {
		t.Errorf("Expected stack size %d, got %d", DefaultStackSize, f.StackSize)
	}
}

func TestFiberRun(t *testing.T) {
	t.Parallel()
	executed := false
	fn := func() {
		executed = true
		time.Sleep(10 * time.Millisecond)
	}

	f := NewFiber(fn, DefaultStackSize, "test-run")
	f.Run()

	if !executed {
		t.Error("Fiber function was not executed")
	}

	if f.State != StateFinished {
		t.Errorf("Expected state Finished, got %s", f.State)
	}

	if f.StartedAt.IsZero() {
		t.Error("StartedAt should be set")
	}

	if f.FinishedAt.IsZero() {
		t.Error("FinishedAt should be set")
	}

	if f.RunTime == 0 {
		t.Error("RunTime should be greater than 0")
	}
}

func TestFiberBlock(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "test-block")

	f.Block("test reason", "test-object")

	if f.State != StateBlocked {
		t.Errorf("Expected state Blocked, got %s", f.State)
	}

	if f.BlockReason != "test reason" {
		t.Errorf("Expected BlockReason 'test reason', got '%s'", f.BlockReason)
	}

	if f.BlockedOn == nil {
		t.Error("BlockedOn should not be nil")
	}
}

func TestFiberUnblock(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "test-unblock")

	f.Block("test", "obj")
	f.Unblock()

	if f.State != StateReady {
		t.Errorf("Expected state Ready after unblock, got %s", f.State)
	}

	if f.BlockReason != "" {
		t.Errorf("Expected empty BlockReason, got '%s'", f.BlockReason)
	}

	if f.BlockedOn != nil {
		t.Error("BlockedOn should be nil after unblock")
	}
}

func TestFiberIsFinished(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "test-finished")

	if f.IsFinished() {
		t.Error("New fiber should not be finished")
	}

	f.State = StateFinished

	if !f.IsFinished() {
		t.Error("Fiber with StateFinished should be finished")
	}

	f.State = StateDead

	if !f.IsFinished() {
		t.Error("Fiber with StateDead should be finished")
	}
}

func TestFiberIsRunnable(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "test-runnable")

	if !f.IsRunnable() {
		t.Error("Fiber in Ready state should be runnable")
	}

	f.State = StateRunning

	if f.IsRunnable() {
		t.Error("Running fiber should not be runnable")
	}

	f.State = StateBlocked

	if f.IsRunnable() {
		t.Error("Blocked fiber should not be runnable")
	}
}

func TestFiberIsBlocked(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "test-isblocked")

	if f.IsBlocked() {
		t.Error("New fiber should not be blocked")
	}

	f.State = StateBlocked

	if !f.IsBlocked() {
		t.Error("Fiber with StateBlocked should be blocked")
	}
}

func TestFiberClone(t *testing.T) {
	t.Parallel()
	original := NewFiber(func() {}, DefaultStackSize, "test-clone")
	original.Priority = 5
	original.ScheduleCount = 10
	original.YieldCount = 3

	clone := original.Clone()

	if clone == nil {
		t.Fatal("Clone returned nil")
	}

	if clone.ID != original.ID {
		t.Error("Clone should have same ID")
	}

	if clone.Name != original.Name {
		t.Error("Clone should have same Name")
	}

	if clone.Priority != original.Priority {
		t.Error("Clone should have same Priority")
	}

	if clone.ScheduleCount != original.ScheduleCount {
		t.Error("Clone should have same ScheduleCount")
	}

	// Verify it's a shallow copy (different addresses)
	if clone == original {
		t.Error("Clone should be a different object")
	}
}

func TestFiberAddChild(t *testing.T) {
	t.Parallel()
	parent := NewFiber(func() {}, DefaultStackSize, "parent")
	child := NewFiber(func() {}, DefaultStackSize, "child")

	parent.AddChild(child)

	if child.Parent != parent {
		t.Error("Child's parent should be set")
	}

	if len(parent.Children) != 1 {
		t.Errorf("Expected 1 child, got %d", len(parent.Children))
	}

	if parent.Children[0] != child {
		t.Error("Child not added to parent's children list")
	}
}

func TestFiberPanic(t *testing.T) {
	t.Parallel()
	fn := func() {
		panic("test panic")
	}

	f := NewFiber(fn, DefaultStackSize, "test-panic")

	// Should not crash the test
	f.Run()

	if f.State != StateFinished {
		t.Errorf("Expected state Finished after panic, got %s", f.State)
	}
}

func TestFiberStateString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state FiberState
		want  string
	}{
		{StateReady, "Ready"},
		{StateRunning, "Running"},
		{StateBlocked, "Blocked"},
		{StateFinished, "Finished"},
		{StateDead, "Dead"},
		{FiberState(999), "Unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.state.String(); got != tc.want {
				t.Errorf("FiberState(%d).String() = %q, want %q", tc.state, got, tc.want)
			}
		})
	}
}

func TestFiberFailure(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var f *Fiber
		if err := f.Failure(); err != nil {
			t.Errorf("nil fiber Failure() should return nil, got %v", err)
		}
	})
	t.Run("fiber with panic", func(t *testing.T) {
		f := NewFiber(func() { panic("boom") }, DefaultStackSize, "panic-fiber")
		f.Run()
		if err := f.Failure(); err == nil {
			t.Error("expected non-nil error from panicked fiber")
		} else if err.Error() == "" {
			t.Error("error message should not be empty")
		}
	})
	t.Run("normal fiber has no failure", func(t *testing.T) {
		f := NewFiber(func() {}, DefaultStackSize, "ok-fiber")
		f.Run()
		if err := f.Failure(); err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})
}

func TestFiberPanicStack(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var f *Fiber
		if stack := f.PanicStack(); stack != nil {
			t.Errorf("nil fiber PanicStack() should return nil, got %v", stack)
		}
	})
	t.Run("fiber with panic has stack", func(t *testing.T) {
		f := NewFiber(func() { panic("boom") }, DefaultStackSize, "panic-fiber")
		f.Run()
		if stack := f.PanicStack(); len(stack) == 0 {
			t.Error("expected non-empty panic stack from panicked fiber")
		}
	})
	t.Run("normal fiber has no panic stack", func(t *testing.T) {
		f := NewFiber(func() {}, DefaultStackSize, "ok-fiber")
		f.Run()
		if stack := f.PanicStack(); len(stack) != 0 {
			t.Errorf("expected empty panic stack, got %d bytes", len(stack))
		}
	})
}

func TestFiberSetPriority(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "priority-test")
	if got := f.PriorityValue(); got != 0 {
		t.Errorf("default priority = %d, want 0", got)
	}
	f.SetPriority(42)
	if got := f.PriorityValue(); got != 42 {
		t.Errorf("priority after SetPriority(42) = %d, want 42", got)
	}
	f.SetPriority(-1)
	if got := f.PriorityValue(); got != -1 {
		t.Errorf("priority after SetPriority(-1) = %d, want -1", got)
	}
}

func TestFiberPriorityValue(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "priority-value")
	f.Priority = 99
	if got := f.PriorityValue(); got != 99 {
		t.Errorf("PriorityValue() = %d, want 99", got)
	}
}

func TestFiberCreatedAtValue(t *testing.T) {
	t.Parallel()
	before := time.Now()
	f := NewFiber(func() {}, DefaultStackSize, "created-at")
	after := time.Now()
	created := f.CreatedAtValue()
	if created.Before(before) || created.After(after) {
		t.Errorf("CreatedAtValue() = %v, should be between %v and %v", created, before, after)
	}
}

func TestFiberMarkScheduled(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var f *Fiber
		f.MarkScheduled() // should not panic
	})
	t.Run("normal case", func(t *testing.T) {
		f := NewFiber(func() {}, DefaultStackSize, "mark-scheduled")
		before := time.Now()
		f.MarkScheduled()
		after := time.Now()
		if f.State != StateRunning {
			t.Errorf("state after MarkScheduled = %s, want %s", f.State, StateRunning)
		}
		if f.LastScheduled.Before(before) || f.LastScheduled.After(after) {
			t.Error("LastScheduled should be set to current time")
		}
		if f.SwitchCount != 1 {
			t.Errorf("SwitchCount = %d, want 1", f.SwitchCount)
		}
	})
	t.Run("already finished", func(t *testing.T) {
		f := NewFiber(func() {}, DefaultStackSize, "finished")
		f.Run()
		prevSwitch := f.SwitchCount
		f.MarkScheduled()
		if f.SwitchCount != prevSwitch {
			t.Errorf("SwitchCount should not change for finished fiber, got %d", f.SwitchCount)
		}
	})
}

func TestFiberAddCPUTime(t *testing.T) {
	t.Parallel()
	f := NewFiber(func() {}, DefaultStackSize, "cpu-time")
	if f.CPUTime != 0 {
		t.Errorf("initial CPUTime = %v, want 0", f.CPUTime)
	}
	f.AddCPUTime(5 * time.Millisecond)
	if f.CPUTime != 5*time.Millisecond {
		t.Errorf("CPUTime after AddCPUTime(5ms) = %v, want 5ms", f.CPUTime)
	}
	f.AddCPUTime(3 * time.Millisecond)
	if f.CPUTime != 8*time.Millisecond {
		t.Errorf("CPUTime after AddCPUTime(3ms) = %v, want 8ms", f.CPUTime)
	}
}

func TestFiberString(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var f *Fiber
		if got := f.String(); got != "Fiber[nil]" {
			t.Errorf("nil fiber String() = %q, want %q", got, "Fiber[nil]")
		}
	})
	t.Run("normal fiber", func(t *testing.T) {
		f := NewFiber(func() {}, DefaultStackSize, "display")
		f.SetPriority(7)
		got := f.String()
		want := "Fiber[ID="
		if len(got) < len(want) || got[:len(want)] != want {
			t.Errorf("Fiber.String() = %q, should start with %q", got, want)
		}
		if got == "Fiber[nil]" {
			t.Errorf("non-nil fiber should not return %q", got)
		}
	})
}

func TestFiberConcurrency(t *testing.T) {
	t.Parallel()
	const numFibers = 100
	done := make(chan bool, numFibers)

	for i := 0; i < numFibers; i++ {
		go func() {
			f := NewFiber(func() {
				time.Sleep(1 * time.Millisecond)
			}, DefaultStackSize, "concurrent-fiber")
			f.Run()
			done <- true
		}()
	}

	for i := 0; i < numFibers; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent fibers")
		}
	}
}
