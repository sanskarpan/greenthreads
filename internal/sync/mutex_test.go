package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func TestFiberMutexHandsOffOwnership(t *testing.T) {
	t.Parallel()
	mutex := NewFiberMutex()
	first := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "first")
	second := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "second")
	mutex.Lock(first)
	done := make(chan struct{})
	go func() {
		mutex.Lock(second)
		close(done)
	}()
	for mutex.WaitQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	mutex.Unlock(first)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mutex waiter was not released")
	}
	if mutex.Owner() != second.ID {
		t.Fatalf("owner = %d, want %d", mutex.Owner(), second.ID)
	}
	mutex.Unlock(second)
}

func TestFiberRWMutexDoesNotDoubleCountReaders(t *testing.T) {
	t.Parallel()
	mutex := NewFiberRWMutex()
	reader := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reader")
	writer := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "writer")
	mutex.RLock(reader)
	done := make(chan struct{})
	go func() {
		mutex.Lock(writer)
		close(done)
	}()
	// Poll until the writer is blocked, then release the reader.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if writer.IsBlocked() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mutex.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer was not admitted")
	}
	mutex.Unlock(writer)
}

func TestFiberMutexLockCtxCancelRemovesWaiter(t *testing.T) {
	t.Parallel()
	mutex := NewFiberMutex()
	holder := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "holder")
	contender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "contender")
	mutex.Lock(holder)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- mutex.LockCtx(ctx, contender) }()
	deadline := time.After(time.Second)
	for mutex.WaitQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("contender did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("LockCtx did not return on cancel")
	}
	for mutex.WaitQueueSize() != 0 {
		time.Sleep(time.Millisecond)
	}
	mutex.Unlock(holder)
}

func TestFiberMutexTryLock(t *testing.T) {
	t.Parallel()

	// nil mutex
	var nilMu *FiberMutex
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "f")
	if nilMu.TryLock(f) {
		t.Fatal("nil mutex TryLock should return false")
	}

	// nil currentFiber
	mu := NewFiberMutex()
	if mu.TryLock(nil) {
		t.Fatal("TryLock with nil fiber should return false")
	}

	// success when unlocked
	if !mu.TryLock(f) {
		t.Fatal("TryLock on unlocked mutex should succeed")
	}
	if mu.Owner() != f.ID {
		t.Fatalf("owner = %d, want %d", mu.Owner(), f.ID)
	}

	// failure when locked
	other := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "other")
	if mu.TryLock(other) {
		t.Fatal("TryLock on locked mutex should return false")
	}
	mu.Unlock(f)
}

func TestFiberMutexIsLocked(t *testing.T) {
	t.Parallel()

	// nil
	var nilMu *FiberMutex
	if nilMu.IsLocked() {
		t.Fatal("nil mutex IsLocked should return false")
	}

	mu := NewFiberMutex()
	if mu.IsLocked() {
		t.Fatal("new mutex should not be locked")
	}

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "f")
	mu.Lock(f)
	if !mu.IsLocked() {
		t.Fatal("locked mutex should report locked")
	}
	mu.Unlock(f)
	if mu.IsLocked() {
		t.Fatal("unlocked mutex should report unlocked")
	}
}

func TestFiberMutexOwner(t *testing.T) {
	t.Parallel()

	// nil
	var nilMu *FiberMutex
	if nilMu.Owner() != 0 {
		t.Fatal("nil mutex Owner should be 0")
	}

	mu := NewFiberMutex()
	if mu.Owner() != 0 {
		t.Fatal("new mutex Owner should be 0")
	}

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "f")
	mu.Lock(f)
	if mu.Owner() != f.ID {
		t.Fatalf("Owner = %d, want %d", mu.Owner(), f.ID)
	}
	mu.Unlock(f)
	if mu.Owner() != 0 {
		t.Fatal("unlocked mutex Owner should be 0")
	}
}

func TestFiberMutexWaitQueueSize(t *testing.T) {
	t.Parallel()

	// nil
	var nilMu *FiberMutex
	if nilMu.WaitQueueSize() != 0 {
		t.Fatal("nil mutex WaitQueueSize should be 0")
	}

	mu := NewFiberMutex()
	if mu.WaitQueueSize() != 0 {
		t.Fatal("new mutex WaitQueueSize should be 0")
	}

	holder := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "holder")
	mu.Lock(holder)

	// Queue a waiter
	contender := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "contender")
	done := make(chan struct{})
	go func() {
		mu.Lock(contender)
		close(done)
	}()
	deadline := time.After(time.Second)
	for mu.WaitQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("contender did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if mu.WaitQueueSize() != 1 {
		t.Fatalf("WaitQueueSize = %d, want 1", mu.WaitQueueSize())
	}
	mu.Unlock(holder)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("contender was not released")
	}
}

func TestFiberRWMutexRLockBlocking(t *testing.T) {
	t.Parallel()
	rw := NewFiberRWMutex()
	writer := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "writer")
	reader := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reader")

	// Writer holds the lock
	rw.Lock(writer)

	// Reader should block
	done := make(chan struct{})
	go func() {
		rw.RLock(reader)
		close(done)
	}()

	// Wait for reader to try to acquire
	deadline := time.After(time.Second)
	for !reader.IsBlocked() {
		select {
		case <-deadline:
			t.Fatal("reader did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Writer releases; reader should unblock
	rw.Unlock(writer)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader was not unblocked after writer unlock")
	}
	rw.RUnlock()
}

func BenchmarkFiberMutexContended(b *testing.B) {
	for _, n := range []int{2, 4, 8} {
		n := n
		b.Run(fmt.Sprintf("goroutines=%d", n), func(b *testing.B) {
			mu := NewFiberMutex()
			counter := 0
			var wg sync.WaitGroup
			iters := b.N / n
			if iters < 1 {
				iters = 1
			}
			b.ResetTimer()
			for g := 0; g < n; g++ {
				g := g
				wg.Add(1)
				go func() {
					defer wg.Done()
					f := fiber.NewFiber(nil, fiber.DefaultStackSize, fmt.Sprintf("bench-%d", g))
					for i := 0; i < iters; i++ {
						mu.Lock(f)
						counter++
						mu.Unlock(f)
					}
				}()
			}
			wg.Wait()
			_ = counter
		})
	}
}
