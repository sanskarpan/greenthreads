package fiber

import (
	"testing"
)

func BenchmarkFiberCreate(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewFiber(func() {}, DefaultStackSize, "bench")
	}
}

func BenchmarkFiberRun(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewFiber(func() {}, DefaultStackSize, "bench")
		f.Run()
	}
}

func BenchmarkFiberClone(b *testing.B) {
	f := NewFiber(func() {}, DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Clone()
	}
}

func BenchmarkFiberStateTransitions(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewFiber(func() {}, DefaultStackSize, "bench")
		f.State = StateReady
		f.State = StateRunning
		f.Block("reason", nil)
		f.Unblock()
	}
}
