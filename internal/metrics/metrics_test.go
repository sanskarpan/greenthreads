package metrics

import (
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func TestRecordFiberCreated(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFiberCreated(64 * 1024)
	m.RecordFiberCreated(64 * 1024)

	snap := m.GetSnapshot()
	if snap.TotalFibersCreated != 2 {
		t.Errorf("TotalFibersCreated = %d, want 2", snap.TotalFibersCreated)
	}
	if snap.ActiveFibers != 2 {
		t.Errorf("ActiveFibers = %d, want 2", snap.ActiveFibers)
	}
	if snap.TotalStackMemory != 128*1024 {
		t.Errorf("TotalStackMemory = %d, want %d", snap.TotalStackMemory, 128*1024)
	}
	if snap.PeakFiberCount != 2 {
		t.Errorf("PeakFiberCount = %d, want 2", snap.PeakFiberCount)
	}
}

func TestRecordFiberCompleted(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFiberCreated(64 * 1024)
	f := &fiber.Fiber{ID: 1, StackSize: 64 * 1024, CPUTime: 10 * time.Millisecond}
	m.RecordFiberCompleted(f)

	snap := m.GetSnapshot()
	if snap.TotalFibersCompleted != 1 {
		t.Errorf("TotalFibersCompleted = %d, want 1", snap.TotalFibersCompleted)
	}
	if snap.ActiveFibers != 0 {
		t.Errorf("ActiveFibers = %d, want 0", snap.ActiveFibers)
	}
	if snap.TotalStackMemory != 0 {
		t.Errorf("TotalStackMemory = %d, want 0 (released on completion)", snap.TotalStackMemory)
	}
	if snap.TotalCPUTime != 10*time.Millisecond {
		t.Errorf("TotalCPUTime = %v, want 10ms", snap.TotalCPUTime)
	}
	if snap.AverageRunTime != 10*time.Millisecond {
		t.Errorf("AverageRunTime = %v, want 10ms", snap.AverageRunTime)
	}
}

func TestRecordFiberCompletedIsIdempotent(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	f := &fiber.Fiber{ID: 1, StackSize: 64 * 1024, CPUTime: 5 * time.Millisecond}
	m.RecordFiberCompleted(f)
	m.RecordFiberCompleted(f)
	m.RecordFiberCompleted(f)

	snap := m.GetSnapshot()
	if snap.TotalFibersCompleted != 1 {
		t.Errorf("TotalFibersCompleted = %d, want 1 (idempotent)", snap.TotalFibersCompleted)
	}
}

func TestSetBlockedFibers(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.SetBlockedFibers(5)
	if got := m.GetSnapshot().BlockedFibers; got != 5 {
		t.Errorf("BlockedFibers = %d, want 5", got)
	}
	m.SetBlockedFibers(2)
	if got := m.GetSnapshot().BlockedFibers; got != 2 {
		t.Errorf("BlockedFibers = %d, want 2", got)
	}
	m.SetBlockedFibers(0)
	if got := m.GetSnapshot().BlockedFibers; got != 0 {
		t.Errorf("BlockedFibers = %d, want 0", got)
	}
}

func TestRecordContextSwitch(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	for i := 0; i < 10; i++ {
		m.RecordContextSwitch()
	}
	if got := m.GetSnapshot().TotalContextSwitches; got != 10 {
		t.Errorf("TotalContextSwitches = %d, want 10", got)
	}
}

func TestRecordScheduleCall(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	for i := 0; i < 5; i++ {
		m.RecordScheduleCall()
	}
	if got := m.GetSnapshot().TotalScheduleCalls; got != 5 {
		t.Errorf("TotalScheduleCalls = %d, want 5", got)
	}
}

func TestRecordYield(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	for i := 0; i < 3; i++ {
		m.RecordYield()
	}
	if got := m.GetSnapshot().TotalYields; got != 3 {
		t.Errorf("TotalYields = %d, want 3", got)
	}
}

func TestRecordStealAttempt(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordStealAttempt(true)
	m.RecordStealAttempt(true)
	m.RecordStealAttempt(false)

	snap := m.GetSnapshot()
	if snap.TotalStealAttempts != 3 {
		t.Errorf("TotalStealAttempts = %d, want 3", snap.TotalStealAttempts)
	}
	if snap.TotalStealSuccesses != 2 {
		t.Errorf("TotalStealSuccesses = %d, want 2", snap.TotalStealSuccesses)
	}
	expected := 2.0 / 3.0
	if snap.StealSuccessRate < expected-0.01 || snap.StealSuccessRate > expected+0.01 {
		t.Errorf("StealSuccessRate = %f, want ~%f", snap.StealSuccessRate, expected)
	}
}

func TestMetricsReset(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFiberCreated(64 * 1024)
	m.RecordContextSwitch()
	m.RecordYield()
	m.SetBlockedFibers(3)
	m.RecordStealAttempt(true)

	m.Reset()

	snap := m.GetSnapshot()
	if snap.TotalFibersCreated != 0 || snap.ActiveFibers != 0 || snap.BlockedFibers != 0 ||
		snap.TotalContextSwitches != 0 || snap.TotalYields != 0 || snap.TotalStealAttempts != 0 ||
		snap.PeakFiberCount != 0 || snap.TotalStackMemory != 0 {
		t.Errorf("Reset did not clear all metrics: %+v", snap)
	}
}

func TestEventTracker(t *testing.T) {
	t.Parallel()
	et := NewEventTracker(100)
	for i := 0; i < 50; i++ {
		et.RecordEvent(FiberEvent{
			FiberID: fiber.FiberID(i), EventType: EventCreated, Timestamp: time.Now(),
		})
	}
	all := et.GetEvents()
	if len(all) != 50 {
		t.Fatalf("GetEvents len = %d, want 50", len(all))
	}

	recent := et.GetRecentEvents(10)
	if len(recent) != 10 {
		t.Fatalf("GetRecentEvents(10) len = %d, want 10", len(recent))
	}
	if recent[0].FiberID != fiber.FiberID(40) {
		t.Errorf("GetRecentEvents(10)[0].FiberID = %d, want 40 (most recent 10 of 0-49)", recent[0].FiberID)
	}

	et.Clear()
	if len(et.GetEvents()) != 0 {
		t.Error("Clear did not empty events")
	}
}

func TestEventTrackerBounds(t *testing.T) {
	t.Parallel()
	et := NewEventTracker(5)
	for i := 0; i < 10; i++ {
		et.RecordEvent(FiberEvent{FiberID: fiber.FiberID(i), EventType: EventCreated, Timestamp: time.Now()})
	}
	all := et.GetEvents()
	if len(all) != 5 {
		t.Fatalf("after overflow, len = %d, want 5 (capped at maxEvents)", len(all))
	}
	if all[0].FiberID != fiber.FiberID(5) {
		t.Errorf("oldest after trim = %d, want 5", all[0].FiberID)
	}

	if got := et.GetRecentEvents(-1); len(got) != 0 {
		t.Errorf("GetRecentEvents(-1) len = %d, want 0", len(got))
	}
	if got := et.GetRecentEvents(0); len(got) != 0 {
		t.Errorf("GetRecentEvents(0) len = %d, want 0", len(got))
	}
	if got := et.GetRecentEvents(100); len(got) != 5 {
		t.Errorf("GetRecentEvents(100) len = %d, want 5", len(got))
	}
}

func TestNewEventTrackerDefaultSize(t *testing.T) {
	t.Parallel()
	et := NewEventTracker(0)
	if et == nil {
		t.Fatal("NewEventTracker(0) returned nil")
	}
	et.RecordEvent(FiberEvent{FiberID: 1, EventType: EventCreated, Timestamp: time.Now()})
	if len(et.GetEvents()) != 1 {
		t.Errorf("NewEventTracker(0) should default to a positive max; got len=0")
	}
}

func TestTrimCompletedMap(t *testing.T) {
	t.Parallel()
	t.Run("under target size", func(t *testing.T) {
		m := map[fiber.FiberID]struct{}{
			1: {}, 2: {}, 3: {},
		}
		trimCompletedMap(m, 10)
		if len(m) != 3 {
			t.Errorf("map size = %d, want 3 (unchanged)", len(m))
		}
	})
	t.Run("over target size", func(t *testing.T) {
		m := make(map[fiber.FiberID]struct{})
		for i := 0; i < 100; i++ {
			m[fiber.FiberID(i)] = struct{}{}
		}
		trimCompletedMap(m, 10)
		if len(m) > 10 {
			t.Errorf("map size = %d, want <= 10 after trim", len(m))
		}
	})
}

func TestRecordFiberBlocked(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RecordFiberBlocked()
	if got := m.GetSnapshot().BlockedFibers; got != 1 {
		t.Errorf("BlockedFibers after RecordFiberBlocked = %d, want 1", got)
	}
	m.RecordFiberBlocked()
	if got := m.GetSnapshot().BlockedFibers; got != 2 {
		t.Errorf("BlockedFibers after 2x RecordFiberBlocked = %d, want 2", got)
	}
}

func TestRecordFiberUnblocked(t *testing.T) {
	t.Parallel()
	t.Run("decrements when positive", func(t *testing.T) {
		m := NewMetrics()
		m.RecordFiberBlocked()
		m.RecordFiberBlocked()
		m.RecordFiberUnblocked()
		if got := m.GetSnapshot().BlockedFibers; got != 1 {
			t.Errorf("BlockedFibers after unblock = %d, want 1", got)
		}
	})
	t.Run("no-op when at zero", func(t *testing.T) {
		m := NewMetrics()
		m.RecordFiberUnblocked()
		if got := m.GetSnapshot().BlockedFibers; got != 0 {
			t.Errorf("BlockedFibers when at 0 = %d, want 0", got)
		}
	})
}

func TestComputeBlockedFibersFrom(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var m *Metrics
		m.ComputeBlockedFibersFrom(nil) // should not panic
	})
	t.Run("normal case", func(t *testing.T) {
		m := NewMetrics()
		blocked := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "blocked")
		blocked.State = fiber.StateBlocked
		runnable := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "runnable")
		runnable.State = fiber.StateReady
		fibers := []*fiber.Fiber{blocked, runnable, nil}
		m.ComputeBlockedFibersFrom(fibers)
		if got := m.GetSnapshot().BlockedFibers; got != 1 {
			t.Errorf("BlockedFibers after compute = %d, want 1", got)
		}
	})
}

func TestStartRun(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var m *Metrics
		m.StartRun() // should not panic
	})
	t.Run("normal case", func(t *testing.T) {
		m := NewMetrics()
		before := time.Now()
m.StartRun()
	snap := m.GetSnapshot()
	if snap.Uptime < 0 {
		t.Error("Uptime should be >= 0 after StartRun")
	}
	if snap.LastUpdateTime.Before(before) {
			t.Error("StartTime should be updated to current time")
		}
	})
}

func TestEventTypeString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		et   EventType
		want string
	}{
		{EventCreated, "Created"},
		{EventScheduled, "Scheduled"},
		{EventRunning, "Running"},
		{EventYielded, "Yielded"},
		{EventBlocked, "Blocked"},
		{EventUnblocked, "Unblocked"},
		{EventCompleted, "Completed"},
		{EventContextSwitch, "ContextSwitch"},
		{EventType(999), "Unknown"},
	}
	for _, c := range cases {
		if got := c.et.String(); got != c.want {
			t.Errorf("EventType(%d).String() = %q, want %q", c.et, got, c.want)
		}
	}
}
