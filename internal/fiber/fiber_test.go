package fiber

import (
	"testing"
	"time"
)

func TestNewFiber(t *testing.T) {
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

func TestFiberConcurrency(t *testing.T) {
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
