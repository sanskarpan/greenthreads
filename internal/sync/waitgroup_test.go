package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func TestFiberWaitGroupRejectsNegativeWithoutMutation(t *testing.T) {
	t.Parallel()
	wg := NewFiberWaitGroup()
	if err := wg.Add(-1); err == nil {
		t.Fatal("negative Add should fail")
	}
	if got := wg.Counter(); got != 0 {
		t.Fatalf("counter = %d after rejected Add", got)
	}
}

func TestFiberWaitGroupWakesWaiter(t *testing.T) {
	t.Parallel()
	wg := NewFiberWaitGroup()
	if err := wg.Add(1); err != nil {
		t.Fatal(err)
	}
	waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
	done := make(chan struct{})
	go func() {
		wg.Wait(waiter)
		close(done)
	}()
	for wg.WaitQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	if err := wg.Done(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitgroup waiter was not released")
	}
}

func TestFiberSemaphoreTransfersPermit(t *testing.T) {
	t.Parallel()
	sem := NewFiberSemaphore(0)
	waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
	done := make(chan struct{})
	go func() {
		sem.Acquire(waiter)
		close(done)
	}()
	for sem.WaitQueueSize() != 1 {
		time.Sleep(time.Millisecond)
	}
	sem.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("semaphore waiter was not released")
	}
}

func TestFiberSemaphoreAcquireCtxCancelRemovesWaiter(t *testing.T) {
	t.Parallel()
	sem := NewFiberSemaphore(0)
	waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- sem.AcquireCtx(ctx, waiter) }()
	deadline := time.After(time.Second)
	for sem.WaitQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("acquire did not block")
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
		t.Fatal("AcquireCtx did not return on cancel")
	}
	for sem.WaitQueueSize() != 0 {
		time.Sleep(time.Millisecond)
	}
}

func TestFiberSemaphoreReleaseDoesNotExceedMaxPermits(t *testing.T) {
	t.Parallel()
	sem := NewFiberSemaphore(2)
	if got := sem.MaxPermits(); got != 2 {
		t.Fatalf("MaxPermits = %d, want 2", got)
	}
	sem.Release()
	sem.Release()
	sem.Release()
	if got := sem.AvailablePermits(); got != 2 {
		t.Fatalf("AvailablePermits = %d, want 2", got)
	}
}

func TestFiberWaitGroupWaitCtxCancelRemovesWaiter(t *testing.T) {
	t.Parallel()
	wg := NewFiberWaitGroup()
	if err := wg.Add(1); err != nil {
		t.Fatal(err)
	}
	waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- wg.WaitCtx(ctx, waiter) }()
	deadline := time.After(time.Second)
	for wg.WaitQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("waiter did not block")
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
		t.Fatal("WaitCtx did not return on cancel")
	}
	for wg.WaitQueueSize() != 0 {
		time.Sleep(time.Millisecond)
	}
	_ = wg.Done()
}

func TestFiberWaitGroupAddNegativeCounter(t *testing.T) {
	t.Parallel()
	wg := NewFiberWaitGroup()
	if err := wg.Add(1); err != nil {
		t.Fatal(err)
	}
	// Adding -2 when counter is 1 should error
	if err := wg.Add(-2); err == nil {
		t.Fatal("Add(-2) with counter=1 should error")
	}
	if got := wg.Counter(); got != 1 {
		t.Fatalf("counter = %d after rejected Add, want 1", got)
	}
	_ = wg.Done()
}

func TestFiberWaitGroupDoneReturnsErrorOnNegative(t *testing.T) {
	t.Parallel()
	wg := NewFiberWaitGroup()
	// Done on zero counter returns error via Add(-1)
	if err := wg.Done(); err == nil {
		t.Fatal("Done on zero counter should return error")
	}
}

func TestFiberWaitGroupWaitBlocking(t *testing.T) {
	t.Parallel()
	wg := NewFiberWaitGroup()
	if err := wg.Add(1); err != nil {
		t.Fatal(err)
	}

	waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
	done := make(chan struct{})
	go func() {
		wg.Wait(waiter)
		close(done)
	}()

	deadline := time.After(time.Second)
	for wg.WaitQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("waiter did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := wg.Done(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not unblock after Done")
	}
}

func TestFiberSemaphoreNewNegative(t *testing.T) {
	t.Parallel()
	sem := NewFiberSemaphore(-5)
	if got := sem.MaxPermits(); got != 0 {
		t.Fatalf("MaxPermits = %d, want 0", got)
	}
	if got := sem.AvailablePermits(); got != 0 {
		t.Fatalf("AvailablePermits = %d, want 0", got)
	}
}

func TestFiberSemaphoreTryAcquire(t *testing.T) {
	t.Parallel()

	// nil
	var nilSem *FiberSemaphore
	if nilSem.TryAcquire() {
		t.Fatal("nil semaphore TryAcquire should return false")
	}

	// success
	sem := NewFiberSemaphore(1)
	if !sem.TryAcquire() {
		t.Fatal("TryAcquire with permits should succeed")
	}

	// failure after exhaustion
	if sem.TryAcquire() {
		t.Fatal("TryAcquire without permits should fail")
	}
}

func TestFiberSemaphoreAcquireBlocking(t *testing.T) {
	t.Parallel()
	sem := NewFiberSemaphore(0)

	waiter := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "waiter")
	done := make(chan struct{})
	go func() {
		sem.Acquire(waiter)
		close(done)
	}()

	deadline := time.After(time.Second)
	for sem.WaitQueueSize() != 1 {
		select {
		case <-deadline:
			t.Fatal("acquire did not block")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	sem.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Acquire did not unblock after Release")
	}
}

func TestFiberSemaphoreAvailablePermits(t *testing.T) {
	t.Parallel()

	// nil
	var nilSem *FiberSemaphore
	if nilSem.AvailablePermits() != 0 {
		t.Fatal("nil AvailablePermits should be 0")
	}

	sem := NewFiberSemaphore(3)
	if got := sem.AvailablePermits(); got != 3 {
		t.Fatalf("AvailablePermits = %d, want 3", got)
	}
	sem.TryAcquire()
	if got := sem.AvailablePermits(); got != 2 {
		t.Fatalf("after acquire AvailablePermits = %d, want 2", got)
	}
}

func TestFiberSemaphoreMaxPermits(t *testing.T) {
	t.Parallel()

	// nil
	var nilSem *FiberSemaphore
	if nilSem.MaxPermits() != 0 {
		t.Fatal("nil MaxPermits should be 0")
	}

	sem := NewFiberSemaphore(7)
	if got := sem.MaxPermits(); got != 7 {
		t.Fatalf("MaxPermits = %d, want 7", got)
	}
}
