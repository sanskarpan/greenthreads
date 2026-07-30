package runtime

import (
	"sync"

	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// SpawnGroup fans out N fibers and collects spawn errors and completion.
// It is safe to call Spawn concurrently from multiple goroutines.
type SpawnGroup struct {
	rt   *Runtime
	mu   sync.Mutex
	ids  []fiber.FiberID
	errs []error
	wg   sync.WaitGroup
}

// NewSpawnGroup creates a SpawnGroup backed by rt.
func (rt *Runtime) NewSpawnGroup() *SpawnGroup {
	return &SpawnGroup{rt: rt}
}

// Spawn adds a fiber to the group. Returns the fiber ID or an error if
// the spawn failed (e.g., runtime stopped or cap exceeded).
func (sg *SpawnGroup) Spawn(fn fiber.FiberFunc, name string) (fiber.FiberID, error) {
	sg.wg.Add(1)
	id, err := sg.rt.Spawn(func() {
		defer sg.wg.Done()
		fn()
	}, name)
	if err != nil {
		// Append the error before Done() so Wait() cannot return before
		// the error is visible in sg.errs.
		sg.mu.Lock()
		sg.errs = append(sg.errs, err)
		sg.mu.Unlock()
		sg.wg.Done()
		return 0, err
	}
	sg.mu.Lock()
	sg.ids = append(sg.ids, id)
	sg.mu.Unlock()
	return id, nil
}

// Wait blocks until all spawned fibers have completed. Returns all spawn
// errors accumulated since the group was created. Fiber panics are not
// returned here; they are logged by the runtime and visible via GetFiber.
func (sg *SpawnGroup) Wait() []error {
	sg.wg.Wait()
	sg.mu.Lock()
	defer sg.mu.Unlock()
	if len(sg.errs) == 0 {
		return nil
	}
	return append([]error(nil), sg.errs...)
}

// IDs returns the fiber IDs that were successfully spawned.
func (sg *SpawnGroup) IDs() []fiber.FiberID {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	return append([]fiber.FiberID(nil), sg.ids...)
}
