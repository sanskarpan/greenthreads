package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func TestFiberMutexHandsOffOwnership(t *testing.T) {
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
	mutex := NewFiberRWMutex()
	reader := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reader")
	writer := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "writer")
	mutex.RLock(reader)
	done := make(chan struct{})
	go func() {
		mutex.Lock(writer)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	mutex.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer was not admitted")
	}
	mutex.Unlock(writer)
}

func TestFiberMutexLockCtxCancelRemovesWaiter(t *testing.T) {
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
