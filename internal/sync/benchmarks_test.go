package sync

import (
	"testing"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

func BenchmarkMutexLockUnlock(b *testing.B) {
	m := NewFiberMutex()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Lock(f)
		m.Unlock(f)
	}
}

func BenchmarkRWMutexReadWrite(b *testing.B) {
	mu := NewFiberRWMutex()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock(f)
		mu.RUnlock()
		mu.Lock(f)
		mu.Unlock(f)
	}
}

func BenchmarkChannelSendReceive(b *testing.B) {
	ch := NewFiberChannel(1)
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ch.Send(i, f); err != nil {
			b.Fatal(err)
		}
		if _, err := ch.Receive(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSemaphoreAcquireRelease(b *testing.B) {
	s := NewFiberSemaphore(1)
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Acquire(f)
		s.Release()
	}
}

func BenchmarkWaitGroupAddDoneWait(b *testing.B) {
	wg := NewFiberWaitGroup()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wg.Add(1); err != nil {
			b.Fatal(err)
		}
		if err := wg.Done(); err != nil {
			b.Fatal(err)
		}
		wg.Wait(f)
	}
}
