package fiber

import "testing"

func FuzzNewFiber(f *testing.F) {
	f.Add(DefaultStackSize, "worker")
	f.Add(0, "")
	f.Add(DefaultStackSize*2, "long-name-fiber")
	f.Fuzz(func(t *testing.T, stackSize int, name string) {
		fn := func() {}
		fib := NewFiber(fn, stackSize, name)
		if fib == nil {
			t.Error("NewFiber returned nil")
		}
	})
}

func FuzzFiberSetPriority(f *testing.F) {
	f.Add(0)
	f.Add(50)
	f.Add(-50)
	f.Fuzz(func(t *testing.T, priority int) {
		fib := NewFiber(func() {}, DefaultStackSize, "test")
		fib.SetPriority(priority)
		got := fib.PriorityValue()
		if got != priority {
			t.Errorf("expected priority %d, got %d", priority, got)
		}
	})
}
