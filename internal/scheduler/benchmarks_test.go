package scheduler

import (
	"testing"
	"time"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func BenchmarkFIFOSchedule(b *testing.B) {
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFIFONext(b *testing.B) {
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	fibers := make([]*fiber.Fiber, b.N)
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		fibers[i] = f
		_ = sched.Schedule(f)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFIFOScheduleAndNext(b *testing.B) {
	sched := NewFIFOScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoundRobinSchedule(b *testing.B) {
	sched := NewRoundRobinScheduler(10 * time.Millisecond)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoundRobinNext(b *testing.B) {
	sched := NewRoundRobinScheduler(10 * time.Millisecond)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	fibers := make([]*fiber.Fiber, b.N)
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		fibers[i] = f
		_ = sched.Schedule(f)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoundRobinScheduleAndNext(b *testing.B) {
	sched := NewRoundRobinScheduler(10 * time.Millisecond)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPrioritySchedule(b *testing.B) {
	sched := NewPriorityScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPriorityNext(b *testing.B) {
	sched := NewPriorityScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	fibers := make([]*fiber.Fiber, b.N)
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		fibers[i] = f
		_ = sched.Schedule(f)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPriorityScheduleAndNext(b *testing.B) {
	sched := NewPriorityScheduler()
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWorkStealingSchedule(b *testing.B) {
	sched := NewWorkStealingScheduler(4)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWorkStealingNext(b *testing.B) {
	sched := NewWorkStealingScheduler(4)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	fibers := make([]*fiber.Fiber, b.N)
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		fibers[i] = f
		_ = sched.Schedule(f)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWorkStealingScheduleAndNext(b *testing.B) {
	sched := NewWorkStealingScheduler(4)
	if err := sched.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
		if err := sched.Schedule(f); err != nil {
			b.Fatal(err)
		}
		if _, err := sched.Next(); err != nil {
			b.Fatal(err)
		}
	}
}
