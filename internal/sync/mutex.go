package sync

import (
	"context"
	"fmt"
	"sync"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

type mutexWaiter struct {
	fiber *fiber.Fiber
	ready chan struct{}
}

// FiberMutex is an ownership-tracking FIFO mutex. Waiters sleep on a private
// notification instead of polling an OS timer.
type FiberMutex struct {
	locked    bool
	owner     fiber.FiberID
	waitQueue []*mutexWaiter
	mu        sync.Mutex
}

// NewFiberMutex creates an unlocked fiber mutex.
func NewFiberMutex() *FiberMutex {
	return &FiberMutex{waitQueue: make([]*mutexWaiter, 0)}
}

// Lock acquires the mutex or waits for ownership to be handed off.
func (fm *FiberMutex) Lock(currentFiber *fiber.Fiber) {
	if fm == nil {
		panic("lock of nil fiber mutex")
	}
	if currentFiber == nil {
		panic("fiber mutex lock requires a non-nil fiber")
	}
	fm.mu.Lock()
	if !fm.locked {
		fm.locked = true
		fm.owner = currentFiber.ID
		fm.mu.Unlock()
		return
	}
	waiter := &mutexWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for mutex", fm)
	fm.waitQueue = append(fm.waitQueue, waiter)
	fm.mu.Unlock()
	<-waiter.ready
}

// LockCtx acquires the mutex or waits until ownership is handed off, returning
// ctx.Err() when ctx is cancelled while blocked. On cancellation the waiter is
// removed from the queue under the mutex.
func (fm *FiberMutex) LockCtx(ctx context.Context, currentFiber *fiber.Fiber) error {
	if fm == nil {
		return fmt.Errorf("nil fiber mutex")
	}
	if currentFiber == nil {
		return fmt.Errorf("lock requires a current fiber")
	}
	fm.mu.Lock()
	if !fm.locked {
		fm.locked = true
		fm.owner = currentFiber.ID
		fm.mu.Unlock()
		return nil
	}
	waiter := &mutexWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for mutex", fm)
	fm.waitQueue = append(fm.waitQueue, waiter)
	fm.mu.Unlock()
	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced with
		// cancellation. The handoff path (Unlock) sets fm.owner and closes
		// waiter.ready while holding fm.mu, so if ready is not yet closed here
		// the handoff cannot complete until we release fm.mu.
		fm.mu.Lock()
		select {
		case <-waiter.ready:
			fm.mu.Unlock()
			return nil
		default:
			fm.removeWaiterLocked(waiter)
			fm.mu.Unlock()
			currentFiber.Unblock()
			return ctx.Err()
		}
	}
}

// removeWaiterLocked removes a mutex waiter from the queue. The caller must
// hold fm.mu.
func (fm *FiberMutex) removeWaiterLocked(waiter *mutexWaiter) {
	for i, w := range fm.waitQueue {
		if w == waiter {
			copy(fm.waitQueue[i:], fm.waitQueue[i+1:])
			fm.waitQueue[len(fm.waitQueue)-1] = nil // clear now-unreachable tail slot
			fm.waitQueue = fm.waitQueue[:len(fm.waitQueue)-1]
			return
		}
	}
}

// TryLock attempts to acquire the mutex without blocking. Returns false if
// the mutex is held or if currentFiber is nil (nil fiber is NOT treated as
// a successful lock acquisition — callers must not proceed as if the lock
// was acquired when TryLock returns false).
func (fm *FiberMutex) TryLock(currentFiber *fiber.Fiber) bool {
	if fm == nil || currentFiber == nil {
		return false
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.locked {
		return false
	}
	fm.locked = true
	fm.owner = currentFiber.ID
	return true
}

// Unlock releases ownership and hands it directly to the oldest waiter.
func (fm *FiberMutex) Unlock(currentFiber *fiber.Fiber) {
	if fm == nil || currentFiber == nil {
		panic("unlock with nil fiber mutex owner")
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if !fm.locked {
		panic("unlock of unlocked mutex")
	}
	if fm.owner != currentFiber.ID {
		panic("unlock by non-owner")
	}
	if len(fm.waitQueue) == 0 {
		fm.locked = false
		fm.owner = 0
		return
	}
	next := fm.waitQueue[0]
	fm.waitQueue[0] = nil // clear GC reference before slicing
	fm.waitQueue = fm.waitQueue[1:]
	fm.owner = next.fiber.ID
	next.fiber.Unblock()
	close(next.ready)
}

// IsLocked reports whether the mutex has an owner.
func (fm *FiberMutex) IsLocked() bool {
	if fm == nil {
		return false
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.locked
}

// Owner returns the current owner, or zero when unlocked.
func (fm *FiberMutex) Owner() fiber.FiberID {
	if fm == nil {
		return 0
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.owner
}

// WaitQueueSize reports the number of pending lock requests.
func (fm *FiberMutex) WaitQueueSize() int {
	if fm == nil {
		return 0
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return len(fm.waitQueue)
}

type rwWaiter struct {
	fiber *fiber.Fiber
	ready chan struct{}
}

// FiberRWMutex is a writer-preferred reader/writer mutex.
type FiberRWMutex struct {
	readers     int
	writer      fiber.FiberID
	readerQueue []*rwWaiter
	writerQueue []*rwWaiter
	mu          sync.Mutex
}

// NewFiberRWMutex creates an unlocked reader/writer mutex.
func NewFiberRWMutex() *FiberRWMutex {
	return &FiberRWMutex{
		readerQueue: make([]*rwWaiter, 0),
		writerQueue: make([]*rwWaiter, 0),
	}
}

// RLock acquires a shared read lock or waits behind active/pending writers.
func (frw *FiberRWMutex) RLock(currentFiber *fiber.Fiber) {
	if frw == nil {
		panic("rlock of nil fiber rwmutex")
	}
	if currentFiber == nil {
		panic("fiber rwmutex rlock requires a non-nil fiber")
	}
	frw.mu.Lock()
	if frw.writer == 0 && len(frw.writerQueue) == 0 {
		frw.readers++
		frw.mu.Unlock()
		return
	}
	waiter := &rwWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for read lock", frw)
	frw.readerQueue = append(frw.readerQueue, waiter)
	frw.mu.Unlock()
	<-waiter.ready
}

// RLockCtx acquires a shared read lock or waits, returning ctx.Err() when ctx
// is cancelled while blocked. On cancellation the waiter is removed from the
// reader queue under the mutex.
func (frw *FiberRWMutex) RLockCtx(ctx context.Context, currentFiber *fiber.Fiber) error {
	if frw == nil {
		return fmt.Errorf("nil fiber rwmutex")
	}
	if currentFiber == nil {
		return fmt.Errorf("rlock requires a current fiber")
	}
	frw.mu.Lock()
	if frw.writer == 0 && len(frw.writerQueue) == 0 {
		frw.readers++
		frw.mu.Unlock()
		return nil
	}
	waiter := &rwWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for read lock", frw)
	frw.readerQueue = append(frw.readerQueue, waiter)
	frw.mu.Unlock()
	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced with
		// cancellation. The handoff path (RUnlock/Unlock) increments frw.readers
		// and closes waiter.ready while holding frw.mu, so if ready is not yet
		// closed here the handoff cannot complete until we release frw.mu.
		frw.mu.Lock()
		select {
		case <-waiter.ready:
			frw.mu.Unlock()
			return nil
		default:
			frw.removeReaderLocked(waiter)
			frw.mu.Unlock()
			currentFiber.Unblock()
			return ctx.Err()
		}
	}
}

// removeReaderLocked removes a reader waiter from the queue. The caller must
// hold frw.mu.
func (frw *FiberRWMutex) removeReaderLocked(waiter *rwWaiter) {
	for i, w := range frw.readerQueue {
		if w == waiter {
			copy(frw.readerQueue[i:], frw.readerQueue[i+1:])
			frw.readerQueue[len(frw.readerQueue)-1] = nil // clear now-unreachable tail slot
			frw.readerQueue = frw.readerQueue[:len(frw.readerQueue)-1]
			return
		}
	}
}

// RUnlock releases one shared read lock and admits the next writer if the
// final reader has left. When no writer is queued, all pending readers are
// admitted so a previously-stranded reader (e.g. one that queued behind a
// writer that was canceled) is released by the final active reader.
func (frw *FiberRWMutex) RUnlock() {
	if frw == nil {
		panic("runlock of nil rwmutex")
	}
	frw.mu.Lock()
	defer frw.mu.Unlock()
	if frw.readers == 0 {
		panic("runlock of unlocked rwmutex")
	}
	frw.readers--
	if frw.readers == 0 {
		if frw.grantWriterLocked() {
			return
		}
		// No writer is queued; admit any readers that piled up behind a
		// canceled writer. Without this, a canceled writer can leave
		// queued readers stranded even after the last active reader
		// exits.
		for _, waiter := range frw.readerQueue {
			frw.readers++
			waiter.fiber.Unblock()
			close(waiter.ready)
		}
		frw.readerQueue = nil
	}
}

// Lock acquires exclusive ownership or waits behind readers and writers.
func (frw *FiberRWMutex) Lock(currentFiber *fiber.Fiber) {
	if frw == nil {
		panic("lock of nil fiber rwmutex")
	}
	if currentFiber == nil {
		panic("fiber rwmutex lock requires a non-nil fiber")
	}
	frw.mu.Lock()
	if frw.readers == 0 && frw.writer == 0 && len(frw.writerQueue) == 0 {
		frw.writer = currentFiber.ID
		frw.mu.Unlock()
		return
	}
	waiter := &rwWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for write lock", frw)
	frw.writerQueue = append(frw.writerQueue, waiter)
	frw.mu.Unlock()
	<-waiter.ready
}

// LockCtx acquires exclusive ownership or waits, returning ctx.Err() when ctx
// is cancelled while blocked. On cancellation the waiter is removed from the
// writer queue under the mutex.
func (frw *FiberRWMutex) LockCtx(ctx context.Context, currentFiber *fiber.Fiber) error {
	if frw == nil {
		return fmt.Errorf("nil fiber rwmutex")
	}
	if currentFiber == nil {
		return fmt.Errorf("lock requires a current fiber")
	}
	frw.mu.Lock()
	if frw.readers == 0 && frw.writer == 0 && len(frw.writerQueue) == 0 {
		frw.writer = currentFiber.ID
		frw.mu.Unlock()
		return nil
	}
	waiter := &rwWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for write lock", frw)
	frw.writerQueue = append(frw.writerQueue, waiter)
	frw.mu.Unlock()
	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced with
		// cancellation. The handoff path (Unlock) sets frw.writer and closes
		// waiter.ready while holding frw.mu, so if ready is not yet closed here
		// the handoff cannot complete until we release frw.mu.
		frw.mu.Lock()
		select {
		case <-waiter.ready:
			frw.mu.Unlock()
			return nil
		default:
			frw.removeWriterLocked(waiter)
			frw.mu.Unlock()
			currentFiber.Unblock()
			return ctx.Err()
		}
	}
}

// removeWriterLocked removes a writer waiter from the queue. The caller must
// hold frw.mu.
func (frw *FiberRWMutex) removeWriterLocked(waiter *rwWaiter) {
	for i, w := range frw.writerQueue {
		if w == waiter {
			copy(frw.writerQueue[i:], frw.writerQueue[i+1:])
			frw.writerQueue[len(frw.writerQueue)-1] = nil // clear now-unreachable tail slot
			frw.writerQueue = frw.writerQueue[:len(frw.writerQueue)-1]
			return
		}
	}
}

// Unlock releases exclusive ownership and admits a writer before readers.
func (frw *FiberRWMutex) Unlock(currentFiber *fiber.Fiber) {
	if frw == nil || currentFiber == nil {
		panic("unlock with nil rwmutex owner")
	}
	frw.mu.Lock()
	defer frw.mu.Unlock()
	if frw.writer != currentFiber.ID {
		panic("unlock by non-owner")
	}
	frw.writer = 0
	if frw.grantWriterLocked() {
		return
	}
	for _, waiter := range frw.readerQueue {
		frw.readers++
		waiter.fiber.Unblock()
		close(waiter.ready)
	}
	frw.readerQueue = nil
}

func (frw *FiberRWMutex) grantWriterLocked() bool {
	if len(frw.writerQueue) == 0 || frw.readers != 0 || frw.writer != 0 {
		return false
	}
	waiter := frw.writerQueue[0]
	frw.writerQueue[0] = nil // clear GC reference before slicing
	frw.writerQueue = frw.writerQueue[1:]
	frw.writer = waiter.fiber.ID
	waiter.fiber.Unblock()
	close(waiter.ready)
	return true
}
