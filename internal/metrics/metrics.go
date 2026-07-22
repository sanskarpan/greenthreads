// Package metrics records bounded runtime counters and lifecycle events.
package metrics

import (
	"sync"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// Metrics tracks runtime performance metrics
type Metrics struct {
	// Fiber statistics
	TotalFibersCreated   int64
	TotalFibersCompleted int64
	ActiveFibers         int64
	BlockedFibers        int64

	// Scheduling statistics
	TotalContextSwitches int64
	TotalScheduleCalls   int64
	TotalYields          int64

	// Timing statistics
	AverageRunTime  time.Duration
	AverageWaitTime time.Duration
	TotalCPUTime    time.Duration

	// Work stealing statistics (for work-stealing scheduler)
	TotalStealAttempts  int64
	TotalStealSuccesses int64
	StealSuccessRate    float64

	// Resource usage
	PeakFiberCount   int64
	TotalStackMemory int64

	// Timestamp
	StartTime      time.Time
	LastUpdateTime time.Time

	mu        sync.RWMutex
	completed map[fiber.FiberID]struct{}
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime:      time.Now(),
		LastUpdateTime: time.Now(),
		completed:      make(map[fiber.FiberID]struct{}),
	}
}

// RecordFiberCreated records that a fiber was created
func (m *Metrics) RecordFiberCreated(stackSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalFibersCreated++
	m.ActiveFibers++
	m.TotalStackMemory += int64(stackSize)
	m.LastUpdateTime = time.Now()

	if m.ActiveFibers > m.PeakFiberCount {
		m.PeakFiberCount = m.ActiveFibers
	}
}

// RecordFiberCompleted records that a fiber completed
func (m *Metrics) RecordFiberCompleted(f *fiber.Fiber) {
	if m == nil || f == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.completed[f.ID]; exists {
		return
	}
	m.completed[f.ID] = struct{}{}

	m.TotalFibersCompleted++
	if m.ActiveFibers > 0 {
		m.ActiveFibers--
	}
	m.TotalCPUTime += f.CPUTime
	if m.TotalStackMemory >= int64(f.StackSize) {
		m.TotalStackMemory -= int64(f.StackSize)
	}
	m.LastUpdateTime = time.Now()

	// Update average run time
	if m.TotalFibersCompleted > 0 {
		m.AverageRunTime = m.TotalCPUTime / time.Duration(m.TotalFibersCompleted)
	}
}

// RecordFiberBlocked records that a fiber was blocked
func (m *Metrics) RecordFiberBlocked() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.BlockedFibers++
	m.LastUpdateTime = time.Now()
}

// RecordFiberUnblocked records that a fiber was unblocked
func (m *Metrics) RecordFiberUnblocked() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.BlockedFibers > 0 {
		m.BlockedFibers--
	}
	m.LastUpdateTime = time.Now()
}

// SetBlockedFibers sets the blocked-fiber gauge to the current count. The
// runtime's deadlock detector calls this on every check so BlockedFibers
// reflects the actual number of fibers in the Blocked state, rather than an
// inc/dec counter that the runtime never drives from sync-primitive paths.
func (m *Metrics) SetBlockedFibers(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.BlockedFibers = n
	m.LastUpdateTime = time.Now()
	m.mu.Unlock()
}

// RecordContextSwitch records a context switch
func (m *Metrics) RecordContextSwitch() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalContextSwitches++
	m.LastUpdateTime = time.Now()
}

// RecordScheduleCall records a scheduler call
func (m *Metrics) RecordScheduleCall() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalScheduleCalls++
	m.LastUpdateTime = time.Now()
}

// RecordYield records a fiber yield
func (m *Metrics) RecordYield() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalYields++
	m.LastUpdateTime = time.Now()
}

// RecordStealAttempt records a work-stealing attempt
func (m *Metrics) RecordStealAttempt(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalStealAttempts++
	if success {
		m.TotalStealSuccesses++
	}

	if m.TotalStealAttempts > 0 {
		m.StealSuccessRate = float64(m.TotalStealSuccesses) / float64(m.TotalStealAttempts)
	}

	m.LastUpdateTime = time.Now()
}

// GetSnapshot returns a snapshot of current metrics
func (m *Metrics) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return MetricsSnapshot{
		TotalFibersCreated:   m.TotalFibersCreated,
		TotalFibersCompleted: m.TotalFibersCompleted,
		ActiveFibers:         m.ActiveFibers,
		BlockedFibers:        m.BlockedFibers,
		TotalContextSwitches: m.TotalContextSwitches,
		TotalScheduleCalls:   m.TotalScheduleCalls,
		TotalYields:          m.TotalYields,
		AverageRunTime:       m.AverageRunTime,
		AverageWaitTime:      m.AverageWaitTime,
		TotalCPUTime:         m.TotalCPUTime,
		TotalStealAttempts:   m.TotalStealAttempts,
		TotalStealSuccesses:  m.TotalStealSuccesses,
		StealSuccessRate:     m.StealSuccessRate,
		PeakFiberCount:       m.PeakFiberCount,
		TotalStackMemory:     m.TotalStackMemory,
		Uptime:               time.Since(m.StartTime),
		LastUpdateTime:       m.LastUpdateTime,
	}
}

// Reset resets all metrics
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalFibersCreated = 0
	m.TotalFibersCompleted = 0
	m.ActiveFibers = 0
	m.BlockedFibers = 0
	m.TotalContextSwitches = 0
	m.TotalScheduleCalls = 0
	m.TotalYields = 0
	m.AverageRunTime = 0
	m.AverageWaitTime = 0
	m.TotalCPUTime = 0
	m.TotalStealAttempts = 0
	m.TotalStealSuccesses = 0
	m.StealSuccessRate = 0
	m.PeakFiberCount = 0
	m.TotalStackMemory = 0
	m.completed = make(map[fiber.FiberID]struct{})
	m.StartTime = time.Now()
	m.LastUpdateTime = time.Now()
}

// MetricsSnapshot is an immutable snapshot of metrics
type MetricsSnapshot struct {
	TotalFibersCreated   int64
	TotalFibersCompleted int64
	ActiveFibers         int64
	BlockedFibers        int64
	TotalContextSwitches int64
	TotalScheduleCalls   int64
	TotalYields          int64
	AverageRunTime       time.Duration
	AverageWaitTime      time.Duration
	TotalCPUTime         time.Duration
	TotalStealAttempts   int64
	TotalStealSuccesses  int64
	StealSuccessRate     float64
	PeakFiberCount       int64
	TotalStackMemory     int64
	Uptime               time.Duration
	LastUpdateTime       time.Time
}

// FiberEvent represents an event in a fiber's lifecycle
type FiberEvent struct {
	FiberID   fiber.FiberID
	EventType EventType
	Timestamp time.Time
	Details   string
}

// EventType represents the type of fiber event
type EventType int

const (
	EventCreated EventType = iota
	EventScheduled
	EventRunning
	EventYielded
	EventBlocked
	EventUnblocked
	EventCompleted
	EventContextSwitch
)

// String returns the stable wire name for an event type.
func (et EventType) String() string {
	switch et {
	case EventCreated:
		return "Created"
	case EventScheduled:
		return "Scheduled"
	case EventRunning:
		return "Running"
	case EventYielded:
		return "Yielded"
	case EventBlocked:
		return "Blocked"
	case EventUnblocked:
		return "Unblocked"
	case EventCompleted:
		return "Completed"
	case EventContextSwitch:
		return "ContextSwitch"
	default:
		return "Unknown"
	}
}

// EventTracker tracks fiber events for profiling and visualization
type EventTracker struct {
	events    []FiberEvent
	maxEvents int
	mu        sync.RWMutex
}

// NewEventTracker creates a new event tracker
func NewEventTracker(maxEvents int) *EventTracker {
	if maxEvents <= 0 {
		maxEvents = 10000 // Default
	}

	return &EventTracker{
		events:    make([]FiberEvent, 0),
		maxEvents: maxEvents,
	}
}

// RecordEvent records a fiber event
func (et *EventTracker) RecordEvent(event FiberEvent) {
	et.mu.Lock()
	defer et.mu.Unlock()

	et.events = append(et.events, event)

	// Keep only last maxEvents
	if len(et.events) > et.maxEvents {
		et.events = et.events[len(et.events)-et.maxEvents:]
	}
}

// GetEvents returns all recorded events
func (et *EventTracker) GetEvents() []FiberEvent {
	et.mu.RLock()
	defer et.mu.RUnlock()

	events := make([]FiberEvent, len(et.events))
	copy(events, et.events)
	return events
}

// GetRecentEvents returns the N most recent events
func (et *EventTracker) GetRecentEvents(n int) []FiberEvent {
	et.mu.RLock()
	defer et.mu.RUnlock()

	if n <= 0 {
		return []FiberEvent{}
	}
	if n > len(et.events) {
		n = len(et.events)
	}

	start := len(et.events) - n
	events := make([]FiberEvent, n)
	copy(events, et.events[start:])
	return events
}

// Clear clears all events
func (et *EventTracker) Clear() {
	et.mu.Lock()
	defer et.mu.Unlock()

	et.events = make([]FiberEvent, 0)
}
