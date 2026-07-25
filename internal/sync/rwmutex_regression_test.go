package sync

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

// TestRWMutexStrandedReadersAfterWriterCancel is a regression test for the
// bug where a FiberRWMutex reader that was queued behind a pending writer
// remained stranded forever if the writer was canceled. The fix in RUnlock
// admits all queued readers when the last active reader exits and no writer
// is queued.
func TestRWMutexStrandedReadersAfterWriterCancel(t *testing.T) {
	t.Parallel()
	frw := NewFiberRWMutex()

	reader1 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reader1")
	reader2 := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "reader2")
	writer := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "writer")

	frw.RLock(reader1)

	// Writer starts waiting.
	writerCtx, writerCancel := context.WithCancel(context.Background())
	writerDone := make(chan error, 1)
	go func() { writerDone <- frw.LockCtx(writerCtx, writer) }()
	waitBlocked(writer)

	// Reader2 starts waiting. With writer preference it queues in readerQueue.
	readerCtx, readerCancel := context.WithCancel(context.Background())
	readerDone := make(chan error, 1)
	go func() { readerDone <- frw.RLockCtx(readerCtx, reader2) }()
	waitBlocked(reader2)

	// Cancel the writer; the writer is removed from writerQueue and Unblock
	// is called. Now there is no writer; the next RUnlock should release
	// reader2.
	writerCancel()
	select {
	case err := <-writerDone:
		if err == nil {
			t.Fatal("writer LockCtx returned nil after cancel; expected context error")
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not return after cancel")
	}

	// Cancel reader2's wait because the test only needs to demonstrate that
	// RUnlock admits the stranded reader. We do NOT rely on the
	// primitive's behavior on cancellation; we test the RUnlock path.
	_ = readerCancel

	// The final RUnlock from reader1 should now admit reader2. The reader
	// is currently blocked on RLockCtx's select. We wait briefly for the
	// reader2 goroutine to observe the grant and decrement its waiter
	// queue. We verify by reading the mutex's internal state.
	frw.RUnlock()

	// The stranded reader2 should have been admitted. We poll the
	// mutex's reader count to confirm.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		frw.mu.Lock()
		n := frw.readers
		frw.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("RUnlock did not admit the stranded reader2 after the writer was canceled")
}
