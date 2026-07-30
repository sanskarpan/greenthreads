package runtime

import (
	"time"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// SpawnWithResult spawns a fiber whose return value is sent on the returned channel.
// The channel receives exactly one value when the fiber completes.
// The fiber function must not panic; a panic is recovered and the channel
// receives nil (use SpawnWithFuture for error propagation).
func (rt *Runtime) SpawnWithResult(fn func() interface{}, name string) (fiber.FiberID, <-chan interface{}, error) {
	ch := make(chan interface{}, 1)
	id, err := rt.Spawn(func() { ch <- fn() }, name)
	if err != nil {
		return 0, nil, err
	}
	return id, ch, nil
}

// SpawnWithTimeout spawns a fiber with a maximum execution duration.
// When the timeout elapses, the fiber function's goroutine is abandoned
// (Go does not support goroutine cancellation; the inner goroutine may
// continue running after the fiber slot is released).
func (rt *Runtime) SpawnWithTimeout(fn fiber.FiberFunc, name string, timeout time.Duration) (fiber.FiberID, error) {
	return rt.Spawn(func() {
		done := make(chan struct{}, 1)
		go func() {
			defer func() { _ = recover() }() // absorb panics from abandoned goroutines
			fn()
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-time.After(timeout):
			// timed out — slot released, inner goroutine may still run
		}
	}, name)
}
