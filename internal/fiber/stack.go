package fiber

import (
	"fmt"
	"sync/atomic"
)

const (
	// DefaultStackSize is the bounded simulated stack allocated for a fiber.
	DefaultStackSize = 64 * 1024
	// MinStackSize prevents zero-sized and unusably small stacks.
	MinStackSize = 4 * 1024
	// MaxStackSize caps per-fiber memory retained by this simulation.
	MaxStackSize = 8 * 1024 * 1024
)

// Stack is a bounded LIFO byte store used to model fiber stack usage.
// sp is accessed via sync/atomic; Stack is single-owner so no concurrent
// writers are expected. The sync.RWMutex has been removed.
type Stack struct {
	data []byte
	sp   int64 // atomic stack pointer
	size int
}

// NewStack creates a stack with the given (clamped) capacity but defers the
// backing-array allocation until the first Push. This makes construction O(1)
// in memory: a 64 KB × 10,000 fiber program costs zero bytes until each fiber
// actually pushes something onto its stack.
func NewStack(size int) *Stack {
	size = ValidateStackSize(size)
	return &Stack{size: size} // data intentionally nil (lazy)
}

// Push appends data and rejects requests that exceed the remaining capacity.
// Lazy allocation is safe: Stack is single-owner (its fiber's goroutine is the
// only writer). No concurrent Push calls occur.
func (s *Stack) Push(data []byte) error {
	if s == nil {
		return fmt.Errorf("nil stack")
	}
	sp := atomic.LoadInt64(&s.sp)
	newSP := sp + int64(len(data))
	if newSP > int64(s.size) {
		return fmt.Errorf("stack overflow: need %d bytes, have %d available",
			len(data), int64(s.size)-sp)
	}
	// Lazily allocate the backing array on first Push.
	if s.data == nil {
		s.data = make([]byte, s.size)
	}
	copy(s.data[sp:], data)
	atomic.StoreInt64(&s.sp, newSP)
	return nil
}

// Pop removes and returns exactly size bytes from the top of the stack.
func (s *Stack) Pop(size int) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("nil stack")
	}
	if size < 0 {
		return nil, fmt.Errorf("stack pop size must not be negative: %d", size)
	}
	sp := atomic.LoadInt64(&s.sp)
	if int64(size) > sp {
		return nil, fmt.Errorf("stack underflow: want %d bytes, sp=%d", size, sp)
	}
	if s.data == nil {
		// No data was ever pushed; this is a logic error if sp > 0.
		return nil, fmt.Errorf("stack underflow: backing array not initialized")
	}
	newSP := sp - int64(size)
	data := make([]byte, size)
	copy(data, s.data[newSP:sp])
	atomic.StoreInt64(&s.sp, newSP)
	return data, nil
}

// Reset discards all stored stack data.
func (s *Stack) Reset() {
	if s == nil {
		return
	}
	atomic.StoreInt64(&s.sp, 0)
}

// Size returns total capacity in bytes.
func (s *Stack) Size() int {
	if s == nil {
		return 0
	}
	return s.size
}

// Used returns bytes currently stored.
func (s *Stack) Used() int {
	if s == nil {
		return 0
	}
	return int(atomic.LoadInt64(&s.sp))
}

// Available returns unused capacity in bytes.
func (s *Stack) Available() int {
	if s == nil {
		return 0
	}
	return s.size - int(atomic.LoadInt64(&s.sp))
}

// Usage returns the fraction of capacity currently used.
func (s *Stack) Usage() float64 {
	if s == nil || s.size == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&s.sp)) / float64(s.size)
}

// Clone returns an independent copy of the stack contents. If the source stack
// has never been written to (sp == 0), the clone is also lazy.
//
// Race safety: Push() writes s.data before the atomic store to s.sp, so an
// acquire load of s.sp > 0 here guarantees s.data is visible (release-acquire
// ordering through the atomic counter). We therefore use sp > 0 rather than
// s.data != nil to avoid a data race on the pointer field itself.
func (s *Stack) Clone() *Stack {
	if s == nil {
		return nil
	}
	sp := atomic.LoadInt64(&s.sp) // acquire load — synchronises with Push's release store
	clone := &Stack{size: s.size}
	if sp > 0 {
		// s.data was initialised by Push before the atomic store that made sp > 0.
		clone.data = make([]byte, s.size)
		copy(clone.data, s.data)
	}
	atomic.StoreInt64(&clone.sp, sp)
	return clone
}

// StackInfo describes stack capacity and usage at one instant.
type StackInfo struct {
	Size      int
	Used      int
	Available int
	Usage     float64
}

// GetInfo returns a consistent stack snapshot.
func (s *Stack) GetInfo() StackInfo {
	if s == nil {
		return StackInfo{}
	}
	used := int(atomic.LoadInt64(&s.sp))
	return StackInfo{
		Size:      s.size,
		Used:      used,
		Available: s.size - used,
		Usage:     float64(used) / float64(s.size),
	}
}

// ValidateStackSize clamps a stack size to the supported memory bounds.
func ValidateStackSize(size int) int {
	if size < MinStackSize {
		return MinStackSize
	}
	if size > MaxStackSize {
		return MaxStackSize
	}
	return size
}
