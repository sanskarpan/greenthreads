package runtime

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

func BenchmarkDeadlockDetectorScan(b *testing.B) {
	for _, nFibers := range []int{10, 100, 1000} {
		nFibers := nFibers
		b.Run(fmt.Sprintf("fibers=%d", nFibers), func(b *testing.B) {
			rt := NewRuntime(scheduler.TypeFIFO, nFibers+4)
			if err := rt.Start(); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = rt.Stop(context.Background()) }()

			// Spawn fibers that block forever (simulate load), capped at 50.
			cap := nFibers
			if cap > 50 {
				cap = 50
			}
			done := make(chan struct{})
			for i := 0; i < cap; i++ {
				_, err := rt.Spawn(func() {
					<-done
				}, fmt.Sprintf("fiber-%d", i))
				if err != nil {
					b.Logf("spawn error at %d: %v", i, err)
					break
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Force a deadlock scan cycle by reading all fibers.
				_ = rt.GetAllFibers()
			}
			b.StopTimer()
			close(done)
		})
	}
}
