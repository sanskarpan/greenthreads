package fiber

import "testing"

func FuzzStackPopNeverPanics(f *testing.F) {
	f.Add(0)
	f.Add(-1)
	f.Add(DefaultStackSize)
	f.Fuzz(func(t *testing.T, size int) {
		stack := NewStack(DefaultStackSize)
		_, _ = stack.Pop(size)
	})
}
