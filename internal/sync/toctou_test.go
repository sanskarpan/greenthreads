package sync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// TestCtxHandoffRace exercises the TOCTOU window between ctx cancellation and
// the handoff path for every *Ctx blocking variant. When the handoff fires
// concurrently with cancellation, the *Ctx method MUST report the handoff
// result (success), not ctx.Err(). Reporting failure after a successful
// handoff causes ghost ownership for locks/semaphores (permanent deadlock for
// subsequent waiters) and lost-value/phantom-failure for channels.
//
// Each subtest runs a tight loop so the intermittent race surfaces under -race.
func TestCtxHandoffRace(t *testing.T) {
	t.Run("FiberMutexLockCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			mutex := NewFiberMutex()
			holder := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "holder")
			contender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "contender")
			mutex.Lock(holder)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- mutex.LockCtx(ctx, contender) }()
			waitBlocked(contender)

			raceConcurrently(cancel, func() { mutex.Unlock(holder) })

			err := <-errCh
			owner := mutex.Owner()
			locked := mutex.IsLocked()

			// Ghost ownership: cancellation reported but lock was handed off
			// to the contender. This is the deadlock-inducing bug.
			if errors.Is(err, context.Canceled) && owner == contender.ID {
				t.Fatalf("iter %d: LockCtx reported cancellation but owns the mutex (ghost ownership)", i)
			}
			// Inverse: success reported but lock not owned.
			if err == nil && owner != contender.ID {
				t.Fatalf("iter %d: LockCtx succeeded but owner=%d want=%d", i, owner, contender.ID)
			}
			if locked && owner == contender.ID {
				mutex.Unlock(contender)
			}
		}
	})

	t.Run("FiberRWMutexLockCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			rw := NewFiberRWMutex()
			holder := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "holder")
			contender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "contender")
			rw.Lock(holder)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- rw.LockCtx(ctx, contender) }()
			waitBlocked(contender)

			raceConcurrently(cancel, func() { rw.Unlock(holder) })

			err := <-errCh

			// FiberRWMutex exposes no owner getter, so probe ownership by
			// attempting to unlock as the contender. If the handoff fired the
			// contender owns the lock and Unlock succeeds. If cancellation won
			// the lock is free and Unlock(contender) panics.
			owns := safeUnlock(rw, contender)

			// Ghost ownership: cancellation reported but the contender actually
			// owns the writer slot.
			if errors.Is(err, context.Canceled) && owns {
				t.Fatalf("iter %d: RWMutex LockCtx reported cancellation but owns the writer (ghost ownership)", i)
			}
			// Success reported must imply ownership.
			if err == nil && !owns {
				t.Fatalf("iter %d: RWMutex LockCtx succeeded but does not own the writer", i)
			}
		}
	})

	t.Run("FiberSemaphoreAcquireCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			// maxPermits must be >= 1 so that a Release with an empty queue
			// returns a permit to the pool, letting us distinguish the
			// cancel-won outcome (permits==1) from the handoff-won outcome
			// (permits==0).
			sem := NewFiberSemaphore(1)
			if !sem.TryAcquire() {
				t.Fatal("pre-acquire failed")
			}
			waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- sem.AcquireCtx(ctx, waiter) }()
			waitBlocked(waiter)

			raceConcurrently(cancel, func() { sem.Release() })

			err := <-errCh
			permits := sem.AvailablePermits()

			// Ghost ownership (permit leak): cancellation reported but the
			// release was handed directly to this waiter, so no permit was
			// returned to the pool. Subsequent acquirers would block forever.
			if errors.Is(err, context.Canceled) && permits == 0 {
				t.Fatalf("iter %d: AcquireCtx reported cancellation but the permit was handed off (ghost ownership)", i)
			}
			// Success reported must mean the permit was consumed (not pooled).
			if err == nil && permits != 0 {
				t.Fatalf("iter %d: AcquireCtx succeeded but permits=%d", i, permits)
			}
			if permits > 0 {
				// Drain leaked permit to keep the semaphore balanced.
				sem.TryAcquire()
			}
		}
	})

	t.Run("FiberChannelSendCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			ch := NewFiberChannel(0)
			sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
			receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "receiver")
			ctx, cancel := context.WithCancel(context.Background())

			sendErrCh := make(chan error, 1)
			go func() { sendErrCh <- ch.SendCtx(ctx, "payload", sender) }()
			waitBlocked(sender)

			// Launch the receive handoff in its own goroutine; it may block if
			// cancellation wins (sender removed before handoff).
			recvVal := make(chan interface{}, 1)
			recvErr := make(chan error, 1)
			go func() {
				v, e := ch.Receive(receiver)
				recvVal <- v
				recvErr <- e
			}()
			// Race cancellation with the receive handoff.
			cancel()

			sendErr := <-sendErrCh
			if errors.Is(sendErr, context.Canceled) {
				// Cancel won; the receiver may be parked. Close to unblock it.
				ch.Close()
			}
			rv := <-recvVal
			re := <-recvErr

			// Phantom failure: sender reports cancellation even though the
			// receiver actually received the value.
			if errors.Is(sendErr, context.Canceled) && rv == "payload" && re == nil {
				t.Fatalf("iter %d: SendCtx reported cancellation but the value was delivered (phantom failure)", i)
			}
			// Success reported must mean the value was delivered.
			if sendErr == nil && (rv != "payload" || re != nil) {
				t.Fatalf("iter %d: SendCtx succeeded but receive value=%v err=%v", i, rv, re)
			}
		}
	})

	t.Run("FiberChannelReceiveCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			ch := NewFiberChannel(0)
			sender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "sender")
			receiver := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "receiver")
			ctx, cancel := context.WithCancel(context.Background())

			recvValCh := make(chan interface{}, 1)
			recvErrCh := make(chan error, 1)
			go func() {
				v, e := ch.ReceiveCtx(ctx, receiver)
				recvValCh <- v
				recvErrCh <- e
			}()
			waitBlocked(receiver)

			// Launch the send handoff in its own goroutine; it may block if
			// cancellation wins (receiver removed before handoff).
			sendErrCh := make(chan error, 1)
			go func() { sendErrCh <- ch.Send("payload", sender) }()
			// Race cancellation with the send handoff.
			cancel()

			recvErr := <-recvErrCh
			rv := <-recvValCh
			if errors.Is(recvErr, context.Canceled) {
				// Cancel won; the sender may be parked. Close to unblock it.
				ch.Close()
			}
			sendErr := <-sendErrCh

			// Phantom failure: receiver reports cancellation even though the
			// sender's value was consumed by the handoff.
			if errors.Is(recvErr, context.Canceled) && sendErr == nil {
				t.Fatalf("iter %d: ReceiveCtx reported cancellation but the send succeeded (phantom failure)", i)
			}
			// Success reported must mean the value was received.
			if recvErr == nil && (rv != "payload" || sendErr != nil) {
				t.Fatalf("iter %d: ReceiveCtx succeeded but value=%v sendErr=%v", i, rv, sendErr)
			}
		}
	})

	t.Run("FiberWaitGroupWaitCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			wg := NewFiberWaitGroup()
			if err := wg.Add(1); err != nil {
				t.Fatal(err)
			}
			waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- wg.WaitCtx(ctx, waiter) }()
			waitBlocked(waiter)

			raceConcurrently(cancel, func() {
				if err := wg.Done(); err != nil {
					t.Errorf("Done failed: %v", err)
				}
			})

			<-errCh
			// WaitGroup has no ownership to leak; this subtest primarily guards
			// against data races (run under -race) and hangs. Drain to zero.
			for wg.Counter() != 0 {
				time.Sleep(time.Microsecond)
			}
		}
	})

	t.Run("FiberRWMutexRLockCtx", func(t *testing.T) {
		const iterations = 1000
		for i := 0; i < iterations; i++ {
			rw := NewFiberRWMutex()
			holder := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "holder")
			reader := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reader")
			rw.Lock(holder)

			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- rw.RLockCtx(ctx, reader) }()
			waitBlocked(reader)

			raceConcurrently(cancel, func() { rw.Unlock(holder) })

			err := <-errCh
			// If the read lock was acquired, RUnlock must not panic.
			if err == nil {
				rw.RUnlock()
			}
			// If cancelled, the writer slot must be free by now (Unlock ran).
			// This subtest primarily guards against data races and hangs under
			// -race; RWMutex exposes no reader count to assert against.
			_ = err
		}
	})
}

// waitBlocked polls the fiber's blocked state until it has parked inside a
// blocking *Ctx method, ensuring the race fires while the waiter is queued.
func waitBlocked(f *fiber.Fiber) {
	deadline := time.Now().Add(time.Second)
	for !f.IsBlocked() {
		if time.Now().After(deadline) {
			return
		}
	}
}

// raceConcurrently launches the given actions in parallel and waits for both to
// return. The two actions fire as close together as possible to maximise the
// chance of hitting the TOCTOU window.
func raceConcurrently(a, b func()) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a() }()
	go func() { defer wg.Done(); b() }()
	wg.Wait()
}

// safeUnlock attempts Unlock as owner and reports whether the lock was owned by
// it. A panic indicates the lock was not owned (cancellation won).
func safeUnlock(rw *FiberRWMutex, owner *fiber.Fiber) (owned bool) {
	defer func() {
		if r := recover(); r != nil {
			owned = false
		}
	}()
	rw.Unlock(owner)
	return true
}
