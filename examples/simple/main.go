package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskar/greenthreads/internal/runtime"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

func main() {
	fmt.Println("=== Simple Green Threads Example ===")

	// Create runtime with FIFO scheduler
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)

	// Start runtime
	err := rt.Start()
	if err != nil {
		panic(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	fmt.Println("Runtime started with FIFO scheduler")

	// Spawn some fibers
	for i := 1; i <= 5; i++ {
		id := i
		name := fmt.Sprintf("Fiber-%d", id)

		fiberID, err := rt.Spawn(func() {
			fmt.Printf("[%s] Starting execution\n", name)
			time.Sleep(100 * time.Millisecond)

			for j := 1; j <= 3; j++ {
				fmt.Printf("[%s] Working... step %d\n", name, j)
				time.Sleep(50 * time.Millisecond)
			}

			fmt.Printf("[%s] Completed\n", name)
		}, name)

		if err != nil {
			fmt.Printf("Error spawning fiber: %v\n", err)
		} else {
			fmt.Printf("Spawned %s (ID: %d)\n", name, fiberID)
		}
	}

	// Wait for fibers to complete
	fmt.Println("\nWaiting for fibers to complete...")
	time.Sleep(2 * time.Second)

	// Print metrics
	metrics := rt.GetMetrics()
	fmt.Println("\n=== Metrics ===")
	fmt.Printf("Total Fibers Created: %d\n", metrics.TotalFibersCreated)
	fmt.Printf("Total Fibers Completed: %d\n", metrics.TotalFibersCompleted)
	fmt.Printf("Active Fibers: %d\n", metrics.ActiveFibers)
	fmt.Printf("Context Switches: %d\n", metrics.TotalContextSwitches)
	fmt.Printf("Total Yields: %d\n", metrics.TotalYields)

	fmt.Println("\n=== Example Completed ===")
}
