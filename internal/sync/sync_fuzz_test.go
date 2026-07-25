package sync

import (
	"testing"

	"github.com/sanskar/greenthreads/internal/fiber"
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
