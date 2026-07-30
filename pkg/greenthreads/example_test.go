package greenthreads_test

import (
	"context"
	"fmt"
	"log"

	gt "github.com/sanskarpan/greenthreads/pkg/greenthreads"
)

// ExampleNew demonstrates the minimal lifecycle: create a runtime, spawn a
// fiber that sends a message over a plain Go channel, and stop cleanly.
func ExampleNew() {
	rt := gt.New(gt.FIFO, 1)
	if err := rt.Start(); err != nil {
		log.Fatal(err)
	}

	done := make(chan string, 1)
	if _, err := rt.Spawn(func() {
		done <- "hello from fiber"
	}, "greeter"); err != nil {
		log.Fatal(err)
	}

	msg := <-done
	fmt.Println(msg)

	if err := rt.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output:
	// hello from fiber
}

// ExampleNew_priority shows priority scheduling: three fibers are spawned and
// their completion order is forced to be deterministic via a buffered channel
// that is drained in the order messages arrive.
//
// Because goroutine scheduling is non-deterministic the output comment is
// intentionally omitted; the example still serves as runnable documentation.
func ExampleNew_priority() {
	rt := gt.New(gt.Priority, 1)
	if err := rt.Start(); err != nil {
		log.Fatal(err)
	}

	results := make(chan string, 3)

	for _, label := range []string{"low", "medium", "high"} {
		label := label
		if _, err := rt.Spawn(func() {
			results <- label
		}, "fiber-"+label); err != nil {
			log.Fatal(err)
		}
	}

	// Collect all three results in whatever order the scheduler chose.
	for i := 0; i < 3; i++ {
		fmt.Println("completed:", <-results)
	}

	if err := rt.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// ExampleRuntime_NewSpawnGroup shows fan-out: N independent fibers are
// launched through a SpawnGroup, and Wait() blocks until every fiber has
// finished, returning any spawn errors as a slice.
func ExampleRuntime_NewSpawnGroup() {
	rt := gt.New(gt.RoundRobin, 2)
	if err := rt.Start(); err != nil {
		log.Fatal(err)
	}

	results := make(chan int, 5)

	sg := rt.NewSpawnGroup()
	for i := 0; i < 5; i++ {
		i := i
		if _, err := sg.Spawn(func() {
			results <- i * i
		}, fmt.Sprintf("worker-%d", i)); err != nil {
			log.Fatal(err)
		}
	}

	// Wait blocks until all 5 fibers have finished.
	if errs := sg.Wait(); len(errs) != 0 {
		log.Fatal(errs)
	}
	close(results)

	sum := 0
	for v := range results {
		sum += v
	}
	fmt.Println("sum of squares:", sum) // 0+1+4+9+16 = 30

	if err := rt.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output:
	// sum of squares: 30
}

// ExampleRuntime_SpawnWithResult shows how to compute a value inside a fiber
// and receive it back on the returned channel without any manual
// synchronisation wiring.
func ExampleRuntime_SpawnWithResult() {
	rt := gt.New(gt.FIFO, 1)
	if err := rt.Start(); err != nil {
		log.Fatal(err)
	}

	_, ch, err := rt.SpawnWithResult(func() interface{} {
		// Simulate work and return a result.
		return 6 * 7
	}, "compute")
	if err != nil {
		log.Fatal(err)
	}

	result := <-ch
	fmt.Println("answer:", result)

	if err := rt.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output:
	// answer: 42
}

// ExampleRuntime_GetMetrics shows how to read runtime statistics after a
// batch of fibers has completed.
func ExampleRuntime_GetMetrics() {
	rt := gt.New(gt.FIFO, 1)
	if err := rt.Start(); err != nil {
		log.Fatal(err)
	}

	const n = 3
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		if _, err := rt.Spawn(func() {
			done <- struct{}{}
		}, "worker"); err != nil {
			log.Fatal(err)
		}
	}

	// Wait for all fibers to finish before sampling metrics.
	for i := 0; i < n; i++ {
		<-done
	}

	snap := rt.GetMetrics()
	fmt.Printf("fibers created:   %d\n", snap.TotalFibersCreated)
	fmt.Printf("fibers completed: %d\n", snap.TotalFibersCompleted)

	if err := rt.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output:
	// fibers created:   3
	// fibers completed: 3
}
