package web

import (
	"context"
	"sync"
	"testing"
	"time"

	greenthruntime "github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

func TestStressMultipleConcurrentShutdowns(t *testing.T) {
	t.Parallel()
	rt := greenthruntime.NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(rt)
	const callers = 12
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			errs <- s.Shutdown(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Shutdown: %v", err)
		}
	}
	if rt.IsRunning() {
		t.Fatal("runtime remained running after Shutdown")
	}
}
