package sync

import (
	"context"
	"fmt"
	"sync"

	"github.com/sanskar/greenthreads/internal/fiber"
)

type waitGroupWaiter struct {
	fiber *fiber.Fiber
	ready chan struct{}
}

// FiberWaitGroup coordinates completion of a counted set of fibers.
type FiberWaitGroup struct {
	counter   int
	waitQueue []*waitGroupWaiter
	mu        sync.Mutex
}

// NewFiberWaitGroup creates an empty wait group.
func NewFiberWaitGroup() *FiberWaitGroup {
	return &FiberWaitGroup{waitQueue: make([]*waitGroupWaiter, 0)}
}

// Add changes the counter transactionally. Invalid negative results do not
// mutate the wait group.
func (fwg *FiberWaitGroup) Add(delta int) error {
	if fwg == nil {
		return fmt.Errorf("nil waitgroup")
	}
	fwg.mu.Lock()
	defer fwg.mu.Unlock()
	if delta > 0 && len(fwg.waitQueue) > 0 {
		return fmt.Errorf("cannot add to waitgroup while fibers are waiting")
	}
	if delta > 0 && fwg.counter > maxInt-delta {
		return fmt.Errorf("waitgroup counter overflow")
	}
	if delta < 0 && delta < -fwg.counter {
		return fmt.Errorf("negative waitgroup counter")
	}
	fwg.counter += delta
	if fwg.counter == 0 {
		for _, waiter := range fwg.waitQueue {
			waiter.fiber.Unblock()
			close(waiter.ready)
		}
		fwg.waitQueue = nil
	}
	return nil
}

// Done decrements the counter by one.
func (fwg *FiberWaitGroup) Done() error {
	if fwg == nil {
		return fmt.Errorf("nil waitgroup")
	}
	return fwg.Add(-1)
}

// Wait blocks until the counter reaches zero.
func (fwg *FiberWaitGroup) Wait(currentFiber *fiber.Fiber) {
	if fwg == nil || currentFiber == nil {
		return
	}
	fwg.mu.Lock()
	if fwg.counter == 0 {
		fwg.mu.Unlock()
		return
	}
	waiter := &waitGroupWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting on waitgroup", fwg)
	fwg.waitQueue = append(fwg.waitQueue, waiter)
	fwg.mu.Unlock()
	<-waiter.ready
}

// WaitCtx blocks until the counter reaches zero, returning ctx.Err() when ctx
// is cancelled while blocked. On cancellation the waiter is removed from the
// queue under the mutex.
func (fwg *FiberWaitGroup) WaitCtx(ctx context.Context, currentFiber *fiber.Fiber) error {
	if fwg == nil {
		return fmt.Errorf("nil fiber waitgroup")
	}
	if currentFiber == nil {
		return fmt.Errorf("wait requires a current fiber")
	}
	fwg.mu.Lock()
	if fwg.counter == 0 {
		fwg.mu.Unlock()
		return nil
	}
	waiter := &waitGroupWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting on waitgroup", fwg)
	fwg.waitQueue = append(fwg.waitQueue, waiter)
	fwg.mu.Unlock()
	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced with
		// cancellation. The handoff path (Add/Done) closes waiter.ready while
		// holding fwg.mu, so if ready is not yet closed here the handoff cannot
		// complete until we release fwg.mu.
		fwg.mu.Lock()
		select {
		case <-waiter.ready:
			fwg.mu.Unlock()
			return nil
		default:
			fwg.removeWaiterLocked(waiter)
			fwg.mu.Unlock()
			currentFiber.Unblock()
			return ctx.Err()
		}
	}
}

// removeWaiterLocked removes a waitgroup waiter from the queue. The caller
// must hold fwg.mu.
func (fwg *FiberWaitGroup) removeWaiterLocked(waiter *waitGroupWaiter) {
	for i, w := range fwg.waitQueue {
		if w == waiter {
			fwg.waitQueue = append(fwg.waitQueue[:i], fwg.waitQueue[i+1:]...)
			return
		}
	}
}

// Counter returns the current count.
func (fwg *FiberWaitGroup) Counter() int {
	if fwg == nil {
		return 0
	}
	fwg.mu.Lock()
	defer fwg.mu.Unlock()
	return fwg.counter
}

// WaitQueueSize returns the number of waiting fibers.
func (fwg *FiberWaitGroup) WaitQueueSize() int {
	if fwg == nil {
		return 0
	}
	fwg.mu.Lock()
	defer fwg.mu.Unlock()
	return len(fwg.waitQueue)
}

// FiberSemaphore is a counting semaphore. Releases hand permits directly to
// waiters before increasing the available count.
type FiberSemaphore struct {
	permits    int
	maxPermits int
	waitQueue  []*semaphoreWaiter
	mu         sync.Mutex
}

type semaphoreWaiter struct {
	fiber *fiber.Fiber
	ready chan struct{}
}

// NewFiberSemaphore creates a semaphore with permits available immediately.
// permits is recorded as the maximum; Release will not grow available permits
// above this value.
func NewFiberSemaphore(permits int) *FiberSemaphore {
	if permits < 0 {
		permits = 0
	}
	return &FiberSemaphore{
		permits:    permits,
		maxPermits: permits,
		waitQueue:  make([]*semaphoreWaiter, 0),
	}
}

// Acquire consumes a permit or waits for a release.
func (fs *FiberSemaphore) Acquire(currentFiber *fiber.Fiber) {
	if fs == nil || currentFiber == nil {
		return
	}
	fs.mu.Lock()
	if fs.permits > 0 {
		fs.permits--
		fs.mu.Unlock()
		return
	}
	waiter := &semaphoreWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for semaphore", fs)
	fs.waitQueue = append(fs.waitQueue, waiter)
	fs.mu.Unlock()
	<-waiter.ready
}

// AcquireCtx consumes a permit or waits for a release, returning ctx.Err()
// when ctx is cancelled while blocked. On cancellation the waiter is removed
// from the queue under the mutex.
func (fs *FiberSemaphore) AcquireCtx(ctx context.Context, currentFiber *fiber.Fiber) error {
	if fs == nil {
		return fmt.Errorf("nil fiber semaphore")
	}
	if currentFiber == nil {
		return fmt.Errorf("acquire requires a current fiber")
	}
	fs.mu.Lock()
	if fs.permits > 0 {
		fs.permits--
		fs.mu.Unlock()
		return nil
	}
	waiter := &semaphoreWaiter{fiber: currentFiber, ready: make(chan struct{})}
	currentFiber.Block("waiting for semaphore", fs)
	fs.waitQueue = append(fs.waitQueue, waiter)
	fs.mu.Unlock()
	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		// Re-check under the mutex so we observe a handoff that raced with
		// cancellation. The handoff path (Release) closes waiter.ready while
		// holding fs.mu, so if ready is not yet closed here the handoff cannot
		// complete until we release fs.mu.
		fs.mu.Lock()
		select {
		case <-waiter.ready:
			fs.mu.Unlock()
			return nil
		default:
			fs.removeWaiterLocked(waiter)
			fs.mu.Unlock()
			currentFiber.Unblock()
			return ctx.Err()
		}
	}
}

// removeWaiterLocked removes a semaphore waiter from the queue. The caller
// must hold fs.mu.
func (fs *FiberSemaphore) removeWaiterLocked(waiter *semaphoreWaiter) {
	for i, w := range fs.waitQueue {
		if w == waiter {
			fs.waitQueue = append(fs.waitQueue[:i], fs.waitQueue[i+1:]...)
			return
		}
	}
}

// TryAcquire consumes a permit only when one is immediately available.
func (fs *FiberSemaphore) TryAcquire() bool {
	if fs == nil {
		return false
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.permits == 0 {
		return false
	}
	fs.permits--
	return true
}

// Release wakes the oldest waiter or increments the available count. Over-
// releasing beyond the constructor maximum is a no-op.
func (fs *FiberSemaphore) Release() {
	if fs == nil {
		return
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.waitQueue) > 0 {
		waiter := fs.waitQueue[0]
		fs.waitQueue = fs.waitQueue[1:]
		waiter.fiber.Unblock()
		close(waiter.ready)
		return
	}
	if fs.permits < fs.maxPermits {
		fs.permits++
	}
}

var maxInt = int(^uint(0) >> 1)

// AvailablePermits returns the current available count.
func (fs *FiberSemaphore) AvailablePermits() int {
	if fs == nil {
		return 0
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.permits
}

// MaxPermits returns the maximum number of permits configured at construction.
func (fs *FiberSemaphore) MaxPermits() int {
	if fs == nil {
		return 0
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.maxPermits
}

// WaitQueueSize reports the number of blocked acquisitions.
func (fs *FiberSemaphore) WaitQueueSize() int {
	if fs == nil {
		return 0
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return len(fs.waitQueue)
}
