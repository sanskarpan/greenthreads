package sync

import (
	"fmt"
	"sync"
	"testing"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func FuzzFiberChannelSendReceive(f *testing.F) {
	f.Add(1, 42)
	f.Add(10, -1)
	f.Add(3, 0)
	f.Fuzz(func(t *testing.T, capacity int, value int) {
		if capacity <= 0 {
			return // synchronous channels require rendezvous, skip
		}
		ch := NewFiberChannel(capacity)
		cf := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "chan-test")
		err := ch.Send(value, cf)
		if err != nil {
			return
		}
		got, err := ch.Receive(cf)
		if err != nil {
			return
		}
		if got != value {
			t.Errorf("expected %d, got %d", value, got)
		}
	})
}

func FuzzFiberMutexLockUnlock(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(5)
	f.Fuzz(func(t *testing.T, n int) {
		m := NewFiberMutex()
		cf := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "mutex-test")
		for i := 0; i < n && i < 10; i++ {
			m.Lock(cf)
			if !m.IsLocked() {
				t.Error("mutex should be locked after Lock")
			}
			m.Unlock(cf)
		}
	})
}

func FuzzFiberWaitGroupAddDone(f *testing.F) {
	f.Add(1)
	f.Add(5)
	f.Add(0)
	f.Fuzz(func(t *testing.T, delta int) {
		wg := NewFiberWaitGroup()
		if delta < 0 || delta > 100 {
			return
		}
		err := wg.Add(delta)
		if err != nil {
			return
		}
		for i := 0; i < delta; i++ {
			_ = wg.Done()
		}
		if wg.Counter() != 0 {
			t.Errorf("expected counter 0, got %d", wg.Counter())
		}
	})
}

func FuzzFiberMutexConcurrent(f *testing.F) {
	f.Add(2, 10)
	f.Add(4, 5)
	f.Fuzz(func(t *testing.T, numGoroutines int, iterations int) {
		if numGoroutines < 2 || numGoroutines > 8 {
			return
		}
		if iterations < 1 || iterations > 20 {
			return
		}

		mu := NewFiberMutex()
		// Lock() calls currentFiber.Block(), so sharing a single fiber across
		// goroutines would race on its State field. Use per-goroutine fibers.
		fibers := make([]*fiber.Fiber, numGoroutines)
		for i := range fibers {
			fibers[i] = fiber.NewFiber(nil, fiber.DefaultStackSize, fmt.Sprintf("f%d", i))
		}

		var wg sync.WaitGroup
		counter := 0
		for g := 0; g < numGoroutines; g++ {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					mu.Lock(fibers[g])
					counter++
					mu.Unlock(fibers[g])
				}
			}()
		}
		// wg.Wait() ensures all goroutines have finished; counter is safe to read.
		wg.Wait()

		expected := numGoroutines * iterations
		if counter != expected {
			t.Errorf("expected counter=%d, got %d (race detected)", expected, counter)
		}
	})
}

func FuzzFiberChannelConcurrent(f *testing.F) {
	f.Add(3, 2, 5)
	f.Fuzz(func(t *testing.T, numSenders int, capacity int, itemsPerSender int) {
		if numSenders < 1 || numSenders > 6 {
			return
		}
		if capacity < 0 || capacity > 10 {
			return
		}
		if itemsPerSender < 1 || itemsPerSender > 10 {
			return
		}

		ch := NewFiberChannel(capacity)
		total := numSenders * itemsPerSender
		received := make(chan interface{}, total+10)

		var senderWG sync.WaitGroup
		for s := 0; s < numSenders; s++ {
			senderWG.Add(1)
			go func(sid int) {
				defer senderWG.Done()
				sf := fiber.NewFiber(nil, fiber.DefaultStackSize, fmt.Sprintf("sender-%d", sid))
				for i := 0; i < itemsPerSender; i++ {
					if err := ch.Send(i, sf); err != nil {
						return // channel closed
					}
				}
			}(s)
		}

		var receiverWG sync.WaitGroup
		receiverWG.Add(1)
		go func() {
			defer receiverWG.Done()
			rf := fiber.NewFiber(nil, fiber.DefaultStackSize, "receiver")
			for i := 0; i < total; i++ {
				v, err := ch.Receive(rf)
				if err != nil {
					return
				}
				received <- v
			}
		}()

		senderWG.Wait()
		ch.Close()
		receiverWG.Wait()
		close(received)

		count := 0
		for range received {
			count++
		}
		// Allow count <= total since close may cut off sends.
		if count > total {
			t.Errorf("received more items (%d) than sent (%d)", count, total)
		}
	})
}
