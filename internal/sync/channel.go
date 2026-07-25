// Package sync provides fiber-aware synchronization with explicit waiters.
package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// ErrChannelClosed indicates that a channel cannot accept or produce another
// value. Buffered values remain readable after Close.
var ErrChannelClosed = errors.New("fiber channel is closed")

type channelSender struct {
	fiber *fiber.Fiber
	value interface{}
	ready chan struct{}
	err   error
}

type channelReceiver struct {
	fiber *fiber.Fiber
	value interface{}
	ready chan struct{}
	err   error
}

// FiberChannel is a bounded FIFO channel with explicit waiter handoff.
// Blocking calls wait in the calling goroutine and never report success before
// the value has been transferred.
type FiberChannel struct {
	buffer    []interface{}
	capacity  int
	closed    bool
	senders   []*channelSender
	receivers []*channelReceiver
	mu        sync.Mutex
}

// NewFiberChannel creates a buffered channel. A zero capacity channel is a
// rendezvous channel.
func NewFiberChannel(capacity int) *FiberChannel {
	if capacity < 0 {
		capacity = 0
	}
	return &FiberChannel{
		buffer:    make([]interface{}, 0, capacity),
		capacity:  capacity,
		senders:   make([]*channelSender, 0),
		receivers: make([]*channelReceiver, 0),
	}
}

// Send transfers value or waits until a receiver or buffer slot is available.
func (fc *FiberChannel) Send(value interface{}, currentFiber *fiber.Fiber) error {
	if fc == nil {
		return fmt.Errorf("nil fiber channel")
	}
	fc.mu.Lock()
	if fc.closed {
		fc.mu.Unlock()
		return ErrChannelClosed
	}
	if len(fc.receivers) > 0 {
		receiver := fc.receivers[0]
		fc.receivers = fc.receivers[1:]
		receiver.value = value
		receiver.err = nil
		receiver.fiber.Unblock()
		close(receiver.ready)
		fc.mu.Unlock()
		return nil
	}
	if len(fc.buffer) < fc.capacity {
		fc.buffer = append(fc.buffer, value)
		fc.mu.Unlock()
		return nil
	}
	if currentFiber == nil {
		fc.mu.Unlock()
		return fmt.Errorf("blocking send requires a current fiber")
	}
	waiter := &channelSender{fiber: currentFiber, value: value, ready: make(chan struct{})}
	currentFiber.Block("waiting to send on channel", fc)
	fc.senders = append(fc.senders, waiter)
	fc.mu.Unlock()
	<-waiter.ready
	return waiter.err
}

// SendCtx transfers value or waits until a receiver or buffer slot is
// available, returning ctx.Err() when ctx is cancelled while blocked. On
// cancellation the waiter is removed from the send queue under the mutex.
func (fc *FiberChannel) SendCtx(ctx context.Context, value interface{}, currentFiber *fiber.Fiber) error {
	if fc == nil {
		return fmt.Errorf("nil fiber channel")
	}
	fc.mu.Lock()
	if fc.closed {
		fc.mu.Unlock()
		return ErrChannelClosed
	}
	if len(fc.receivers) > 0 {
		receiver := fc.receivers[0]
		fc.receivers = fc.receivers[1:]
		receiver.value = value
		receiver.err = nil
		receiver.fiber.Unblock()
		close(receiver.ready)
		fc.mu.Unlock()
		return nil
	}
	if len(fc.buffer) < fc.capacity {
		fc.buffer = append(fc.buffer, value)
		fc.mu.Unlock()
		return nil
	}
	if currentFiber == nil {
		fc.mu.Unlock()
		return fmt.Errorf("blocking send requires a current fiber")
	}
	waiter := &channelSender{fiber: currentFiber, value: value, ready: make(chan struct{})}
	currentFiber.Block("waiting to send on channel", fc)
	fc.senders = append(fc.senders, waiter)
	fc.mu.Unlock()
	select {
	case <-waiter.ready:
		return waiter.err
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced
		// with cancellation. The handoff path sets waiter.err and closes
		// waiter.ready while holding fc.mu, so if ready is not yet closed
		// here the handoff cannot complete until we release fc.mu.
		fc.mu.Lock()
		select {
		case <-waiter.ready:
			fc.mu.Unlock()
			return waiter.err
		default:
			fc.removeSenderLocked(waiter)
			fc.mu.Unlock()
			currentFiber.Unblock()
			return ctx.Err()
		}
	}
}

// removeSenderLocked removes a sender waiter from the queue. The caller must
// hold fc.mu.
func (fc *FiberChannel) removeSenderLocked(waiter *channelSender) {
	for i, s := range fc.senders {
		if s == waiter {
			fc.senders = append(fc.senders[:i], fc.senders[i+1:]...)
			return
		}
	}
}

// Receive returns the next buffered or rendezvous value, waiting when the
// channel has no value. A closed, drained channel returns ErrChannelClosed.
func (fc *FiberChannel) Receive(currentFiber *fiber.Fiber) (interface{}, error) {
	if fc == nil {
		return nil, fmt.Errorf("nil fiber channel")
	}
	fc.mu.Lock()
	if len(fc.buffer) > 0 {
		value := fc.buffer[0]
		fc.buffer = fc.buffer[1:]
		fc.admitSenderLocked()
		fc.mu.Unlock()
		return value, nil
	}
	if len(fc.senders) > 0 {
		sender := fc.senders[0]
		fc.senders = fc.senders[1:]
		sender.fiber.Unblock()
		sender.err = nil
		close(sender.ready)
		fc.mu.Unlock()
		return sender.value, nil
	}
	if fc.closed {
		fc.mu.Unlock()
		return nil, ErrChannelClosed
	}
	if currentFiber == nil {
		fc.mu.Unlock()
		return nil, fmt.Errorf("blocking receive requires a current fiber")
	}
	waiter := &channelReceiver{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting to receive from channel", fc)
	fc.receivers = append(fc.receivers, waiter)
	fc.mu.Unlock()
	<-waiter.ready
	return waiter.value, waiter.err
}

// ReceiveCtx returns the next value or waits until one is available, returning
// ctx.Err() when ctx is cancelled while blocked. On cancellation the waiter is
// removed from the receive queue under the mutex.
func (fc *FiberChannel) ReceiveCtx(ctx context.Context, currentFiber *fiber.Fiber) (interface{}, error) {
	if fc == nil {
		return nil, fmt.Errorf("nil fiber channel")
	}
	fc.mu.Lock()
	if len(fc.buffer) > 0 {
		value := fc.buffer[0]
		fc.buffer = fc.buffer[1:]
		fc.admitSenderLocked()
		fc.mu.Unlock()
		return value, nil
	}
	if len(fc.senders) > 0 {
		sender := fc.senders[0]
		fc.senders = fc.senders[1:]
		sender.fiber.Unblock()
		sender.err = nil
		close(sender.ready)
		fc.mu.Unlock()
		return sender.value, nil
	}
	if fc.closed {
		fc.mu.Unlock()
		return nil, ErrChannelClosed
	}
	if currentFiber == nil {
		fc.mu.Unlock()
		return nil, fmt.Errorf("blocking receive requires a current fiber")
	}
	waiter := &channelReceiver{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting to receive from channel", fc)
	fc.receivers = append(fc.receivers, waiter)
	fc.mu.Unlock()
	select {
	case <-waiter.ready:
		return waiter.value, waiter.err
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced
		// with cancellation. The handoff path sets waiter.value/waiter.err and
		// closes waiter.ready while holding fc.mu, so if ready is not yet
		// closed here the handoff cannot complete until we release fc.mu.
		fc.mu.Lock()
		select {
		case <-waiter.ready:
			fc.mu.Unlock()
			return waiter.value, waiter.err
		default:
			fc.removeReceiverLocked(waiter)
			fc.mu.Unlock()
			currentFiber.Unblock()
			return nil, ctx.Err()
		}
	}
}

// removeReceiverLocked removes a receiver waiter from the queue. The caller
// must hold fc.mu.
func (fc *FiberChannel) removeReceiverLocked(waiter *channelReceiver) {
	for i, r := range fc.receivers {
		if r == waiter {
			fc.receivers = append(fc.receivers[:i], fc.receivers[i+1:]...)
			return
		}
	}
}

// TrySend attempts a transfer without waiting.
func (fc *FiberChannel) TrySend(value interface{}) bool {
	if fc == nil {
		return false
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.closed {
		return false
	}
	if len(fc.receivers) > 0 {
		receiver := fc.receivers[0]
		fc.receivers = fc.receivers[1:]
		receiver.value = value
		receiver.err = nil
		receiver.fiber.Unblock()
		close(receiver.ready)
		return true
	}
	if len(fc.buffer) >= fc.capacity {
		return false
	}
	fc.buffer = append(fc.buffer, value)
	return true
}

// TryReceive returns a value only when one is immediately available.
func (fc *FiberChannel) TryReceive() (interface{}, bool) {
	if fc == nil {
		return nil, false
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.buffer) > 0 {
		value := fc.buffer[0]
		fc.buffer = fc.buffer[1:]
		fc.admitSenderLocked()
		return value, true
	}
	if len(fc.senders) > 0 {
		sender := fc.senders[0]
		fc.senders = fc.senders[1:]
		sender.fiber.Unblock()
		sender.err = nil
		close(sender.ready)
		return sender.value, true
	}
	return nil, false
}

func (fc *FiberChannel) admitSenderLocked() {
	if len(fc.senders) == 0 || fc.closed || len(fc.buffer) >= fc.capacity {
		return
	}
	sender := fc.senders[0]
	fc.senders = fc.senders[1:]
	fc.buffer = append(fc.buffer, sender.value)
	sender.fiber.Unblock()
	sender.err = nil
	close(sender.ready)
}

// Close prevents new sends and wakes all pending operations with a stable
// error. Existing buffered values can still be received.
func (fc *FiberChannel) Close() {
	if fc == nil {
		return
	}
	fc.mu.Lock()
	if fc.closed {
		fc.mu.Unlock()
		return
	}
	fc.closed = true
	for _, receiver := range fc.receivers {
		receiver.err = ErrChannelClosed
		receiver.fiber.Unblock()
		close(receiver.ready)
	}
	fc.receivers = nil
	for _, sender := range fc.senders {
		sender.err = ErrChannelClosed
		sender.fiber.Unblock()
		close(sender.ready)
	}
	fc.senders = nil
	fc.mu.Unlock()
}

// IsClosed reports whether the channel has been closed.
func (fc *FiberChannel) IsClosed() bool {
	if fc == nil {
		return true
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.closed
}

// Len returns the number of buffered values.
func (fc *FiberChannel) Len() int {
	if fc == nil {
		return 0
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.buffer)
}

// Cap returns the configured buffer capacity.
func (fc *FiberChannel) Cap() int {
	if fc == nil {
		return 0
	}
	return fc.capacity
}

// SendQueueSize returns the number of blocked senders.
func (fc *FiberChannel) SendQueueSize() int {
	if fc == nil {
		return 0
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.senders)
}

// RecvQueueSize returns the number of blocked receivers.
func (fc *FiberChannel) RecvQueueSize() int {
	if fc == nil {
		return 0
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.receivers)
}
