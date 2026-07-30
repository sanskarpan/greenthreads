package sync

import (
	"fmt"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// FiberChannelOf is a type-safe fiber-aware channel. Unlike FiberChannel,
// the value type is fixed at construction time so callers do not need
// type assertions on Receive.
//
// It is implemented as a thin wrapper around FiberChannel that hides the
// interface{} conversion. All synchronization semantics are identical.
type FiberChannelOf[T any] struct {
	inner *FiberChannel
}

// NewFiberChannelOf creates a typed channel with the given buffer capacity.
func NewFiberChannelOf[T any](capacity int) *FiberChannelOf[T] {
	return &FiberChannelOf[T]{inner: NewFiberChannel(capacity)}
}

// Send sends a value, blocking the calling fiber if the buffer is full.
func (c *FiberChannelOf[T]) Send(value T, currentFiber *fiber.Fiber) error {
	return c.inner.Send(value, currentFiber)
}

// Receive receives a value, blocking the calling fiber if the buffer is empty.
// Returns the zero value of T on error.
func (c *FiberChannelOf[T]) Receive(currentFiber *fiber.Fiber) (T, error) {
	v, err := c.inner.Receive(currentFiber)
	if err != nil {
		var zero T
		return zero, err
	}
	val, ok := v.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("FiberChannelOf[%T]: received unexpected type %T", zero, v)
	}
	return val, nil
}

// TrySend attempts to send without blocking. It returns (true, nil) on
// success, (false, nil) when the buffer is full, and (false, ErrChannelClosed)
// when the channel is closed.
func (c *FiberChannelOf[T]) TrySend(value T) (bool, error) {
	return c.inner.TrySend(value)
}

// TryReceive attempts to receive without blocking. Returns the zero value of T
// and false when no value is immediately available.
func (c *FiberChannelOf[T]) TryReceive() (T, bool) {
	v, ok := c.inner.TryReceive()
	if !ok {
		var zero T
		return zero, false
	}
	val, ok2 := v.(T)
	if !ok2 {
		var zero T
		return zero, false
	}
	return val, true
}

// Close closes the channel.
func (c *FiberChannelOf[T]) Close() { c.inner.Close() }

// Len returns the number of buffered values.
func (c *FiberChannelOf[T]) Len() int { return c.inner.Len() }

// Cap returns the channel capacity.
func (c *FiberChannelOf[T]) Cap() int { return c.inner.Cap() }
