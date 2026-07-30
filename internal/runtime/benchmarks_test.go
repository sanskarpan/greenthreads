package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

func BenchmarkSpawn(b *testing.B) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rt.Spawn(func() {}, "bench"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpawnAndComplete(b *testing.B) {
	rt := NewRuntime(scheduler.TypeWorkStealing, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		done := make(chan struct{})
		if _, err := rt.Spawn(func() {
			close(done)
		}, "bench"); err != nil {
			b.Fatal(err)
		}
		<-done
	}
}

func BenchmarkGetAllFibers(b *testing.B) {
	rt := NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	for i := 0; i < 100; i++ {
		if _, err := rt.Spawn(func() {
			time.Sleep(100 * time.Millisecond)
		}, "bench"); err != nil {
			b.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rt.GetAllFibers()
	}
}
