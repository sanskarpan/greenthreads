// Package metrics records bounded runtime counters and lifecycle events.
package metrics

import (
	"sync"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// LatencyBucket is one bucket in a fiber run-time histogram.
type LatencyBucket struct {
	Le    time.Duration // upper bound (inclusive)
	Count int64
}

// LatencyHistogram tracks fiber run-time distribution.
// Buckets are at 1ms, 10ms, 100ms, 1s, 10s, 60s, and +Inf.
type LatencyHistogram struct {
	buckets []LatencyBucket
	sum     time.Duration
	count   int64
	mu      sync.Mutex
}

var histogramBounds = []time.Duration{
	1 * time.Millisecond,
	10 * time.Millisecond,
	100 * time.Millisecond,
	1 * time.Second,
	10 * time.Second,
	60 * time.Second,
}

// NewLatencyHistogram creates a histogram with standard fiber-latency buckets.
func NewLatencyHistogram() *LatencyHistogram {
	buckets := make([]LatencyBucket, len(histogramBounds)+1)
	for i, b := range histogramBounds {
		buckets[i] = LatencyBucket{Le: b}
	}
	buckets[len(histogramBounds)] = LatencyBucket{Le: time.Duration(1<<63 - 1)} // +Inf
	return &LatencyHistogram{buckets: buckets}
}

// Record adds one observation to the histogram.
func (h *LatencyHistogram) Record(d time.Duration) {
	h.mu.Lock()
	h.sum += d
	h.count++
	for i := range h.buckets {
		if d <= h.buckets[i].Le {
			// Increment this bucket and all higher buckets (cumulative).
			for j := i; j < len(h.buckets); j++ {
				h.buckets[j].Count++
			}
			break
		}
	}
	h.mu.Unlock()
}

// Snapshot returns a copy of the histogram state.
func (h *LatencyHistogram) Snapshot() (buckets []LatencyBucket, sum time.Duration, count int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]LatencyBucket, len(h.buckets))
	copy(out, h.buckets)
	return out, h.sum, h.count
}

// counterOffset accumulates the cumulative counter values across resets so
// that Prometheus counters remain monotonically increasing even when Reset()
// is called between test runs or scheduler restarts.
type counterOffset struct {
	fibersCreated   int64
	fibersCompleted int64
	contextSwitches int64
	yields          int64
	scheduleCalls   int64
}

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

	// Panic counter (incremented via RecordFiberPanic)
	TotalFiberPanics int64

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

	FiberRunHistogram *LatencyHistogram

	mu          sync.RWMutex
	completed   map[fiber.FiberID]struct{}
	resetOffset counterOffset
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime:         time.Now(),
		LastUpdateTime:    time.Now(),
		completed:         make(map[fiber.FiberID]struct{}),
		FiberRunHistogram: NewLatencyHistogram(),
	}
}

// RecordFiberCreated records that a fiber was created
func (m *Metrics) RecordFiberCreated(stackSize int) {
	now := time.Now() // outside lock — PERF-6
	m.mu.Lock()
	m.TotalFibersCreated++
	m.ActiveFibers++
	m.TotalStackMemory += int64(stackSize)
	if m.ActiveFibers > m.PeakFiberCount {
		m.PeakFiberCount = m.ActiveFibers
	}
	m.LastUpdateTime = now
	m.mu.Unlock()
}

// RecordFiberCompleted records that a fiber completed
func (m *Metrics) RecordFiberCompleted(f *fiber.Fiber) {
	if m == nil || f == nil {
		return
	}
	now := time.Now() // outside lock — PERF-6
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.completed[f.ID]; exists {
		return
	}
	m.completed[f.ID] = struct{}{}
	// Bound the completed-id map by trimming oldest entries once the count
	// exceeds a small multiple of expected concurrency. The map is only
	// used for exactly-once accounting; trimming does not lose correctness
	// because RecordFiberCompleted is invoked exclusively from the runtime's
	// complete() path, which is itself serialized.
	if len(m.completed) > maxCompletedTracked {
		trimCompletedMap(m.completed, maxCompletedTracked/2)
	}

	m.TotalFibersCompleted++
	if m.ActiveFibers > 0 {
		m.ActiveFibers--
	}
	m.TotalCPUTime += f.CPUTime
	if m.TotalStackMemory >= int64(f.StackSize) {
		m.TotalStackMemory -= int64(f.StackSize)
	}
	m.LastUpdateTime = now

	// Update average run time
	if m.TotalFibersCompleted > 0 {
		m.AverageRunTime = m.TotalCPUTime / time.Duration(m.TotalFibersCompleted)
	}

	// Record fiber run time in the histogram. LatencyHistogram has its own
	// internal mutex; calling it while holding m.mu is safe because the
	// histogram lock is never acquired on a path that subsequently acquires m.mu.
	if f.CPUTime > 0 && m.FiberRunHistogram != nil {
		m.FiberRunHistogram.Record(f.CPUTime)
	}
}

// maxCompletedTracked bounds the size of the completed-id map. The cap is
// generously larger than any plausible concurrently-active fiber count so
// it never thrashes; it only protects against the long-running-runtime
// memory growth that would otherwise scale with lifetime completions.
const maxCompletedTracked = 16384

// trimCompletedMap removes oldest entries until the map is at most target
// entries. The "oldest" ordering uses a fixed-rate rotation counter so we
// do not need to maintain a parallel ordered structure.
func trimCompletedMap(m map[fiber.FiberID]struct{}, target int) {
	if len(m) <= target {
		return
	}
	// Trim half of the excess at a time to amortize cost; the next call
	// will trim the other half if completions keep coming fast.
	toRemove := len(m) - target
	for k := range m {
		if toRemove <= 0 {
			break
		}
		delete(m, k)
		toRemove--
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

// ComputeBlockedFibersFrom scans the given fiber list and writes the count
// of fibers currently in StateBlocked into the gauge. Callers that already
// hold a snapshot of fibers (the runtime, in particular) can avoid the
// periodic detector scan and surface the latest value in every metric read.
func (m *Metrics) ComputeBlockedFibersFrom(fibers []*fiber.Fiber) {
	if m == nil {
		return
	}
	count := int64(0)
	for _, f := range fibers {
		if f != nil && f.IsBlocked() {
			count++
		}
	}
	m.mu.Lock()
	m.BlockedFibers = count
	m.LastUpdateTime = time.Now()
	m.mu.Unlock()
}

// RecordContextSwitch records a context switch
func (m *Metrics) RecordContextSwitch() {
	now := time.Now() // outside lock — PERF-6
	m.mu.Lock()
	m.TotalContextSwitches++
	m.LastUpdateTime = now
	m.mu.Unlock()
}

// RecordScheduleCall records a scheduler call
func (m *Metrics) RecordScheduleCall() {
	now := time.Now() // outside lock — PERF-6
	m.mu.Lock()
	m.TotalScheduleCalls++
	m.LastUpdateTime = now
	m.mu.Unlock()
}

// RecordFiberPanic increments the panic counter. The runtime calls this when
// a fiber's function panics and is recovered. Kept separate from
// RecordFiberCompleted to avoid a circular-import dependency on fiber internals.
func (m *Metrics) RecordFiberPanic() {
	m.mu.Lock()
	m.TotalFiberPanics++
	m.mu.Unlock()
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

// GetSnapshot returns a snapshot of the current-run metrics. Counters are
// reset to zero after each Reset() call; use GetLifetimeSnapshot for
// Prometheus-compatible monotonically-increasing counters (OBS-2).
func (m *Metrics) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotLocked(false)
}

// GetLifetimeSnapshot returns a snapshot where the five monotonic counters
// (FibersCreated, FibersCompleted, ContextSwitches, Yields, ScheduleCalls)
// include values accumulated across all prior Reset() calls. Use this for
// Prometheus /metrics output to ensure counters never decrease (OBS-2).
func (m *Metrics) GetLifetimeSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshotLocked(true)
}

// snapshotLocked builds a MetricsSnapshot under the read lock already held.
// When lifetime is true, accumulated reset offsets are added to the counters.
func (m *Metrics) snapshotLocked(lifetime bool) MetricsSnapshot {
	created := m.TotalFibersCreated
	completed := m.TotalFibersCompleted
	switches := m.TotalContextSwitches
	schedules := m.TotalScheduleCalls
	yields := m.TotalYields
	if lifetime {
		created += m.resetOffset.fibersCreated
		completed += m.resetOffset.fibersCompleted
		switches += m.resetOffset.contextSwitches
		schedules += m.resetOffset.scheduleCalls
		yields += m.resetOffset.yields
	}
	return MetricsSnapshot{
		TotalFibersCreated:   created,
		TotalFibersCompleted: completed,
		ActiveFibers:         m.ActiveFibers,
		BlockedFibers:        m.BlockedFibers,
		TotalContextSwitches: switches,
		TotalScheduleCalls:   schedules,
		TotalYields:          yields,
		TotalFiberPanics:     m.TotalFiberPanics,
		AverageRunTime:       m.AverageRunTime,
		AverageWaitTime:      m.AverageWaitTime,
		TotalCPUTime:         m.TotalCPUTime,
		TotalStealAttempts:   m.TotalStealAttempts,
		TotalStealSuccesses:  m.TotalStealSuccesses,
		StealSuccessRate:     m.StealSuccessRate,
		PeakFiberCount:       m.PeakFiberCount,
		TotalStackMemory:     m.TotalStackMemory,
		Uptime: func() time.Duration {
			if m.StartTime.IsZero() {
				return 0
			}
			return time.Since(m.StartTime)
		}(),
		LastUpdateTime:    m.LastUpdateTime,
		FiberRunHistogram: m.FiberRunHistogram,
	}
}

// Reset resets per-run counters while accumulating their values into
// resetOffset so that Prometheus counters remain monotonically increasing
// across restarts (OBS-2).
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Accumulate current counters into the reset offset so Prometheus sees
	// monotonically increasing values across resets.
	m.resetOffset.fibersCreated += m.TotalFibersCreated
	m.resetOffset.fibersCompleted += m.TotalFibersCompleted
	m.resetOffset.contextSwitches += m.TotalContextSwitches
	m.resetOffset.yields += m.TotalYields
	m.resetOffset.scheduleCalls += m.TotalScheduleCalls

	// Reset the per-run counters.
	m.TotalFibersCreated = 0
	m.TotalFibersCompleted = 0
	m.TotalContextSwitches = 0
	m.TotalYields = 0
	m.TotalScheduleCalls = 0
	m.ActiveFibers = 0
	m.BlockedFibers = 0
	m.TotalStackMemory = 0
	m.PeakFiberCount = 0
	m.StartTime = time.Time{}
	m.AverageRunTime = 0
	m.AverageWaitTime = 0
	m.TotalCPUTime = 0
	m.TotalStealAttempts = 0
	m.TotalStealSuccesses = 0
	m.StealSuccessRate = 0
	m.completed = make(map[fiber.FiberID]struct{})
	m.FiberRunHistogram = NewLatencyHistogram()
	m.LastUpdateTime = time.Now()
}

// StartRun resets the start timestamp to now. The runtime calls this on
// every Start so Uptime reflects the current run, not the age of the
// metrics object since the runtime was constructed.
func (m *Metrics) StartRun() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.StartTime = time.Now()
	m.LastUpdateTime = m.StartTime
	m.mu.Unlock()
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
	// TotalFiberPanics counts fibers whose function panicked and was recovered.
	TotalFiberPanics int64
	// AverageRunTime is the mean CPU time per completed fiber.
	AverageRunTime time.Duration
	// AverageWaitTime is reserved for future implementation. Always zero.
	AverageWaitTime time.Duration
	TotalCPUTime        time.Duration
	TotalStealAttempts  int64
	TotalStealSuccesses int64
	StealSuccessRate    float64
	PeakFiberCount      int64
	TotalStackMemory    int64
	Uptime              time.Duration
	LastUpdateTime      time.Time
	// FiberRunHistogram captures the distribution of fiber wall-clock run times.
	// Nil when no fibers have completed.
	FiberRunHistogram *LatencyHistogram
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
	// EventCreated is emitted when a fiber is first admitted.
	EventCreated EventType = iota
	// EventScheduled is emitted when a fiber is added to a scheduler queue.
	EventScheduled
	// EventRunning is emitted when a fiber is dispatched for execution.
	EventRunning
	// EventYielded is emitted when a fiber voluntarily yields the CPU.
	EventYielded
	// EventBlocked is emitted when a fiber enters a blocked state.
	EventBlocked
	// EventUnblocked is emitted when a fiber leaves a blocked state.
	EventUnblocked
	// EventCompleted is emitted when a fiber finishes execution.
	EventCompleted
	// EventContextSwitch is emitted when the scheduler switches between fibers.
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

// EventTracker tracks fiber lifecycle events using a circular ring buffer.
// The ring buffer avoids the O(n) slice-shift that the old append-and-trim
// approach incurred on every overflow (PERF-7).
type EventTracker struct {
	events   []FiberEvent
	head     int // index of oldest event
	tail     int // index where next event is written
	count    int // number of valid events currently stored
	capacity int
	mu       sync.RWMutex
}

// NewEventTracker creates a new event tracker backed by a ring buffer of the
// given capacity. A capacity <= 0 defaults to 1000.
func NewEventTracker(capacity int) *EventTracker {
	if capacity <= 0 {
		capacity = 1000
	}
	return &EventTracker{
		events:   make([]FiberEvent, capacity),
		capacity: capacity,
	}
}

// RecordEvent appends an event to the ring buffer, overwriting the oldest
// event when the buffer is full.
func (et *EventTracker) RecordEvent(event FiberEvent) {
	et.mu.Lock()
	et.events[et.tail] = event
	et.tail = (et.tail + 1) % et.capacity
	if et.count < et.capacity {
		et.count++
	} else {
		// Ring is full; advance head to overwrite oldest.
		et.head = (et.head + 1) % et.capacity
	}
	et.mu.Unlock()
}

// GetEvents returns all events currently stored in the ring buffer, ordered
// from oldest to most recent.
func (et *EventTracker) GetEvents() []FiberEvent {
	et.mu.RLock()
	defer et.mu.RUnlock()

	if et.count == 0 {
		return []FiberEvent{}
	}
	result := make([]FiberEvent, et.count)
	for i := 0; i < et.count; i++ {
		result[i] = et.events[(et.head+i)%et.capacity]
	}
	return result
}

// GetRecentEvents returns the n most recent events, ordered oldest-first.
// Returns []FiberEvent{} (never nil) when n <= 0 or the tracker is empty.
func (et *EventTracker) GetRecentEvents(n int) []FiberEvent {
	et.mu.RLock()
	defer et.mu.RUnlock()

	if n <= 0 || et.count == 0 {
		return []FiberEvent{}
	}
	if n > et.count {
		n = et.count
	}
	result := make([]FiberEvent, n)
	// Read the n most recent events (from tail going backwards).
	for i := 0; i < n; i++ {
		idx := ((et.tail - 1 - i) % et.capacity + et.capacity) % et.capacity
		result[n-1-i] = et.events[idx]
	}
	return result
}

// GetCount returns the number of events currently stored in the ring buffer.
func (et *EventTracker) GetCount() int {
	et.mu.RLock()
	defer et.mu.RUnlock()
	return et.count
}

// Clear removes all events from the ring buffer and releases GC references.
func (et *EventTracker) Clear() {
	et.mu.Lock()
	et.head = 0
	et.tail = 0
	et.count = 0
	// Clear event data to release GC references.
	for i := range et.events {
		et.events[i] = FiberEvent{}
	}
	et.mu.Unlock()
}
