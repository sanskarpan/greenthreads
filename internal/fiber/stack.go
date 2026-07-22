package fiber

import (
	"fmt"
	"sync"
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
type Stack struct {
	data []byte
	size int
	sp   int
	mu   sync.RWMutex
}

// NewStack allocates a stack after clamping the requested size to the safety
// bounds. It does not represent the Go runtime stack.
func NewStack(size int) *Stack {
	size = ValidateStackSize(size)
	return &Stack{data: make([]byte, size), size: size}
}

// Push appends data and rejects requests that exceed the remaining capacity.
func (s *Stack) Push(data []byte) error {
	if s == nil {
		return fmt.Errorf("nil stack")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) > s.size-s.sp {
		return fmt.Errorf("stack overflow: attempting to push %d bytes, only %d available", len(data), s.size-s.sp)
	}
	copy(s.data[s.sp:], data)
	s.sp += len(data)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if size > s.sp {
		return nil, fmt.Errorf("stack underflow: attempting to pop %d bytes, only %d available", size, s.sp)
	}
	s.sp -= size
	data := make([]byte, size)
	copy(data, s.data[s.sp:s.sp+size])
	return data, nil
}

// Reset discards all stored stack data.
func (s *Stack) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.sp = 0
	s.mu.Unlock()
}

// Size returns total capacity in bytes.
func (s *Stack) Size() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Used returns bytes currently stored.
func (s *Stack) Used() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sp
}

// Available returns unused capacity in bytes.
func (s *Stack) Available() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size - s.sp
}

// Usage returns the fraction of capacity currently used.
func (s *Stack) Usage() float64 {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.size == 0 {
		return 0
	}
	return float64(s.sp) / float64(s.size)
}

// Clone returns an independent copy of the stack contents.
func (s *Stack) Clone() *Stack {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	clone := &Stack{data: make([]byte, s.size), size: s.size, sp: s.sp}
	copy(clone.data, s.data)
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return StackInfo{
		Size:      s.size,
		Used:      s.sp,
		Available: s.size - s.sp,
		Usage:     float64(s.sp) / float64(s.size),
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
