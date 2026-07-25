package metrics

import (
	"testing"

	"github.com/sanskar/greenthreads/internal/fiber"
)

func BenchmarkMetricsRecord(b *testing.B) {
	m := NewMetrics()
	f := fiber.NewFiber(func() {}, fiber.DefaultStackSize, "bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.RecordFiberCreated(4096)
		m.RecordFiberCompleted(f)
		m.RecordContextSwitch()
		m.RecordScheduleCall()
	}
}

func BenchmarkMetricsSnapshot(b *testing.B) {
	m := NewMetrics()
	for i := 0; i < 1000; i++ {
		m.RecordFiberCreated(4096)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetSnapshot()
	}
}
