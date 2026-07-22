package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func TestFiberWaitGroupRejectsNegativeWithoutMutation(t *testing.T) {
	wg := NewFiberWaitGroup()
	if err := wg.Add(-1); err == nil {
		t.Fatal("negative Add should fail")
	}
	if got := wg.Counter(); got != 0 {
		t.Fatalf("counter = %d after rejected Add", got)
	}
}

func TestFiberWaitGroupWakesWaiter(t *testing.T) {
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
