package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

func main() {
	fmt.Println("=== Simple Green Threads Example ===")

	// Create runtime with FIFO scheduler and 4 worker slots.
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)

	// Start runtime.
	err := rt.Start()
	if err != nil {
		panic(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	fmt.Println("Runtime started with FIFO scheduler")

	// Note: FiberWaitGroup is designed for fiber-to-fiber synchronization and
	// requires a *fiber.Fiber argument obtained via rt.GetFiberDirect(fiberID)
	// from inside a running fiber. For waiting from outside the runtime (e.g.,
	// the main goroutine), use a regular channel or sync.WaitGroup captured in
	// the fiber closure instead.
	var wg sync.WaitGroup
	results := make([]string, 5)

	// Spawn some fibers.
	for i := 0; i < 5; i++ {
		i := i
		name := fmt.Sprintf("Fiber-%d", i+1)

		wg.Add(1)
		fiberID, err := rt.Spawn(func() {
			defer wg.Done()

			fmt.Printf("[%s] Starting execution\n", name)
			time.Sleep(100 * time.Millisecond)

			for j := 1; j <= 3; j++ {
				fmt.Printf("[%s] Working... step %d\n", name, j)
				time.Sleep(50 * time.Millisecond)
			}

			results[i] = fmt.Sprintf("%s completed", name)
			fmt.Printf("[%s] Completed\n", name)
		}, name)

		if err != nil {
			wg.Done()
			fmt.Printf("Error spawning fiber: %v\n", err)
		} else {
			fmt.Printf("Spawned %s (ID: %d)\n", name, fiberID)
		}
	}

	// Wait for all fibers to finish via the shared WaitGroup.
	fmt.Println("\nWaiting for fibers to complete...")
	wg.Wait()

	fmt.Println("\n=== Results ===")
	for _, r := range results {
		fmt.Println(r)
	}

	// Print metrics.
	metrics := rt.GetMetrics()
	fmt.Println("\n=== Metrics ===")
	fmt.Printf("Total Fibers Created: %d\n", metrics.TotalFibersCreated)
	fmt.Printf("Total Fibers Completed: %d\n", metrics.TotalFibersCompleted)
	fmt.Printf("Active Fibers: %d\n", metrics.ActiveFibers)
	fmt.Printf("Context Switches: %d\n", metrics.TotalContextSwitches)
	fmt.Printf("Total Yields: %d\n", metrics.TotalYields)

	fmt.Println("\n=== Example Completed ===")
}
