package runtime

import (
	"github.com/sanskarpan/greenthreads/internal/fiber"
)

// FiberHandle is a read-only view of a fiber's observable state.
// All methods are safe to call concurrently. Exported fields on the
// underlying *fiber.Fiber should not be written directly; use FiberHandle
// instead to prevent data races.
type FiberHandle interface {
	GetID() fiber.FiberID
	GetName() string
	GetState() fiber.FiberState
	GetPriority() int
	IsFinished() bool
	IsBlocked() bool
	IsRunnable() bool
	Failure() error
	PanicStack() []byte
	Clone() *fiber.Fiber
}

// fiberHandleImpl wraps *fiber.Fiber to expose only FiberHandle methods.
type fiberHandleImpl struct{ f *fiber.Fiber }

func (h *fiberHandleImpl) GetID() fiber.FiberID { return h.f.ID }
func (h *fiberHandleImpl) GetName() string       { return h.f.Name }

// GetState returns the current state of the fiber. It uses the publicly
// accessible query methods on Fiber to avoid direct access to the unexported
// mu field from this package.
func (h *fiberHandleImpl) GetState() fiber.FiberState {
	if h.f.IsFinished() {
		return fiber.StateFinished
	}
	if h.f.IsBlocked() {
		return fiber.StateBlocked
	}
	if h.f.IsRunnable() {
		return fiber.StateReady
	}
	return fiber.StateRunning
}

func (h *fiberHandleImpl) GetPriority() int   { return h.f.PriorityValue() }
func (h *fiberHandleImpl) IsFinished() bool    { return h.f.IsFinished() }
func (h *fiberHandleImpl) IsBlocked() bool     { return h.f.IsBlocked() }
func (h *fiberHandleImpl) IsRunnable() bool    { return h.f.IsRunnable() }
func (h *fiberHandleImpl) Failure() error      { return h.f.Failure() }
func (h *fiberHandleImpl) PanicStack() []byte  { return h.f.PanicStack() }
func (h *fiberHandleImpl) Clone() *fiber.Fiber { return h.f.Clone() }

// GetFiberHandle returns a read-only handle to the live fiber. The handle
// remains valid until the fiber is reaped from rt.fibers (after complete()).
// For a safe immutable snapshot, call handle.Clone().
func (rt *Runtime) GetFiberHandle(id fiber.FiberID) (FiberHandle, bool) {
	rt.fibersMu.RLock()
	f, ok := rt.fibers[id]
	rt.fibersMu.RUnlock()
	if !ok {
		return nil, false
	}
	return &fiberHandleImpl{f: f}, true
}
