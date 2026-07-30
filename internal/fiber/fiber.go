// Package fiber models bounded, goroutine-backed fiber work items and their
// observable lifecycle state.
package fiber

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// FiberID identifies a fiber for the lifetime of the process.
type FiberID uint64

// FiberState describes the state visible to the scheduler and observers.
type FiberState int32

const (
	// StateReady indicates the fiber is eligible for scheduler selection.
	StateReady FiberState = iota
	// StateRunning indicates the fiber function is currently executing.
	StateRunning
	// StateBlocked indicates the fiber is waiting on a synchronization object.
	StateBlocked
	// StateFinished indicates the fiber function has completed execution.
	StateFinished
	// StateDead indicates the fiber has been torn down and cannot be rescheduled.
	StateDead
)

// String returns the stable display name for a fiber state.
func (s FiberState) String() string {
	switch s {
	case StateReady:
		return "Ready"
	case StateRunning:
		return "Running"
	case StateBlocked:
		return "Blocked"
	case StateFinished:
		return "Finished"
	case StateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// FiberFunc is the function executed once by a fiber.
type FiberFunc func()

// Fiber is the observable state and work item for one scheduled function.
// Mutable lifecycle fields are guarded internally; callers should use the
// methods on Fiber rather than changing state while the runtime is active.
type Fiber struct {
	ID         FiberID
	Name       string
	State      FiberState
	Priority   int
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	RunTime    time.Duration
	WaitTime   time.Duration

	Stack     *Stack
	StackSize int

	fn FiberFunc

	LastScheduled time.Time
	ScheduleCount int64
	YieldCount    int64

	Parent   *Fiber
	Children []*Fiber

	CPUTime     time.Duration
	SwitchCount int64

	BlockedOn   interface{}
	BlockReason string

	Color string

	mu         sync.RWMutex
	failure    error
	panicStack []byte
	runStarted bool
	completion bool
}

var nextFiberID uint64

// NewFiber allocates a ready fiber and its bounded simulated stack.
func NewFiber(fn FiberFunc, stackSize int, name string) *Fiber {
	id := FiberID(atomic.AddUint64(&nextFiberID, 1))
	// Each fiber allocates stackSize bytes of simulated stack. At DefaultStackSize (64KB)
	// and 10,000 concurrent fibers, total stack memory is ~640MB. Scale numWorkers
	// and DefaultStackSize accordingly.
	return &Fiber{
		ID:        id,
		Name:      name,
		State:     StateReady,
		CreatedAt: time.Now(),
		Stack:     NewStack(stackSize),
		StackSize: ValidateStackSize(stackSize),
		fn:        fn,
		Children:  make([]*Fiber, 0),
		Color:     generateColor(id),
	}
}

// Run executes the fiber function exactly once. A panic is captured as a
// failure so a bad fiber cannot crash its owning runtime or masquerade as
// successful work.
func (f *Fiber) Run() {
	if f == nil {
		return
	}

	f.mu.Lock()
	if f.runStarted || f.completion {
		f.mu.Unlock()
		return
	}
	f.runStarted = true
	f.State = StateRunning
	f.StartedAt = time.Now()
	f.ScheduleCount++
	fn := f.fn
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		if recovered := recover(); recovered != nil {
			var panicErr error
			if e, ok := recovered.(error); ok {
				panicErr = fmt.Errorf("fiber %d (%s) panicked: %w", f.ID, f.Name, e)
			} else {
				panicErr = fmt.Errorf("fiber %d (%s) panicked with value: %v", f.ID, f.Name, recovered)
			}
			f.failure = panicErr
			f.panicStack = debug.Stack()
		}
		f.State = StateFinished
		f.FinishedAt = time.Now()
		f.RunTime = f.FinishedAt.Sub(f.StartedAt)
		f.completion = true
		f.mu.Unlock()
	}()

	if fn != nil {
		fn()
	}
}

// Failure returns the captured panic as an error, if the fiber failed.
func (f *Fiber) Failure() error {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.failure
}

// PanicStack returns a copy of the captured panic stack, if any.
func (f *Fiber) PanicStack() []byte {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return append([]byte(nil), f.panicStack...)
}

// Block marks a fiber as waiting on a synchronization object.
func (f *Fiber) Block(reason string, on interface{}) {
	f.mu.Lock()
	if f.completion {
		f.mu.Unlock()
		return
	}
	f.State = StateBlocked
	f.BlockReason = reason
	f.BlockedOn = on
	f.mu.Unlock()
}

// Unblock makes a blocked fiber observable as ready (or running if already started).
func (f *Fiber) Unblock() {
	f.mu.Lock()
	if !f.completion {
		if f.runStarted {
			f.State = StateRunning
		} else {
			f.State = StateReady
		}
	}
	f.BlockReason = ""
	f.BlockedOn = nil
	f.mu.Unlock()
}

// SetPriority changes a fiber priority before it is selected by a scheduler.
func (f *Fiber) SetPriority(priority int) {
	f.mu.Lock()
	f.Priority = priority
	f.mu.Unlock()
}

// PriorityValue returns the current priority atomically with respect to fiber
// lifecycle updates.
func (f *Fiber) PriorityValue() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.Priority
}

// CreatedAtValue returns the creation timestamp used for stable queue order.
func (f *Fiber) CreatedAtValue() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.CreatedAt
}

// MarkScheduled records a scheduler hand-off without exposing a compound
// state mutation to callers.
func (f *Fiber) MarkScheduled() {
	if f == nil {
		return
	}
	f.mu.Lock()
	if f.completion {
		f.mu.Unlock()
		return
	}
	f.State = StateRunning
	f.LastScheduled = time.Now()
	f.SwitchCount++
	f.mu.Unlock()
}

// AddCPUTime records execution time without exposing a racy read-modify-write.
func (f *Fiber) AddCPUTime(duration time.Duration) {
	f.mu.Lock()
	f.CPUTime += duration
	f.mu.Unlock()
}

// IsFinished reports whether the fiber reached a terminal state.
func (f *Fiber) IsFinished() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.State == StateFinished || f.State == StateDead
}

// IsRunnable reports whether the scheduler may select the fiber.
func (f *Fiber) IsRunnable() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.State == StateReady
}

// IsBlocked reports whether the fiber is waiting on a synchronization object.
func (f *Fiber) IsBlocked() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.State == StateBlocked
}

// AddChild records a parent-child relationship for observability.
func (f *Fiber) AddChild(child *Fiber) {
	if f == nil || child == nil {
		return
	}
	child.mu.Lock()
	child.Parent = f
	child.mu.Unlock()
	f.mu.Lock()
	f.Children = append(f.Children, child)
	f.mu.Unlock()
}

// Clone returns an immutable-by-convention snapshot without exposing the
// executable function, context, or synchronization internals.
func (f *Fiber) Clone() *Fiber {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &Fiber{
		ID:            f.ID,
		Name:          f.Name,
		State:         f.State,
		Priority:      f.Priority,
		CreatedAt:     f.CreatedAt,
		StartedAt:     f.StartedAt,
		FinishedAt:    f.FinishedAt,
		RunTime:       f.RunTime,
		WaitTime:      f.WaitTime,
		StackSize:     f.StackSize,
		LastScheduled: f.LastScheduled,
		ScheduleCount: f.ScheduleCount,
		YieldCount:    f.YieldCount,
		CPUTime:       f.CPUTime,
		SwitchCount:   f.SwitchCount,
		BlockedOn:     f.BlockedOn,
		BlockReason:   f.BlockReason,
		Color:         f.Color,
	}
}

func generateColor(id FiberID) string {
	colors := []string{
		"#FF6B6B", "#4ECDC4", "#45B7D1", "#FFA07A", "#98D8C8",
		"#F7DC6F", "#BB8FCE", "#85C1E2", "#F8B739", "#52B788",
		"#E56B6F", "#6C5CE7", "#00B894", "#FD79A8", "#FDCB6E",
	}
	// FiberID is uint64; the modulo is computed in uint64 space and the
	// result is always < len(colors) (15), so the narrowing to int is safe.
	idx := id % FiberID(len(colors))
	return colors[idx]
}

// String returns a compact diagnostic representation of the snapshot state.
func (f *Fiber) String() string {
	if f == nil {
		return "Fiber[nil]"
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return fmt.Sprintf("Fiber[ID=%d, Name=%s, State=%s, Priority=%d]",
		f.ID, f.Name, f.State, f.Priority)
}
