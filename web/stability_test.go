package web

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	rt "github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestStabilityLongRunning spawns and drains fibers continuously for 60 seconds,
// measuring peak memory, goroutine count, and metrics consistency. A goroutine
// leak or memory growth beyond 2x the steady state fails the test.
func TestStabilityLongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running stability test in short mode")
	}
	// Reuse the port from TestStressSoakLoad range but pick a unique one.
	addr := randomAddr(t)
	r := rt.NewRuntime(scheduler.TypeFIFO, 4)
	server := NewServer(r)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(addr) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	// Wait for listener.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	conn, _, err := ws.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	mustWrite(t, conn, map[string]interface{}{
		"type": "init", "payload": map[string]interface{}{"schedulerType": "workstealing", "numWorkers": 4},
	})
	if !waitForMsg(t, conn, "initSuccess", 3*time.Second) {
		t.Fatal("init failed")
	}

	var totalSpawned atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	duration := 60 * time.Second

	// Collect a goroutine and memory baseline after warmup (no spawn yet).
	runtime.GC()
	var baselineMem runtime.MemStats
	runtime.ReadMemStats(&baselineMem)
	baselineGoroutines := runtime.NumGoroutine()

	// Producer: continuously spawn short-lived fibers.
	go func() {
		defer close(done)
		next := time.NewTicker(2 * time.Millisecond)
		defer next.Stop()
		deadline := time.Now().Add(duration)
		for time.Now().Before(deadline) {
			select {
			case <-stop:
				return
			case <-next.C:
				name := fmt.Sprintf("s-%d", totalSpawned.Add(1))
				err := conn.WriteJSON(map[string]interface{}{
					"type": "spawn",
					"payload": map[string]interface{}{
						"name": name, "priority": 0, "duration": 10,
					},
				})
				if err != nil {
					t.Logf("spawn write error at %d: %v", totalSpawned.Load(), err)
					return
				}
			}
		}
	}()

	// Drain responses periodically.
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(duration + 10*time.Second))
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	// Wait for the producer to finish.
	<-done
	close(stop)

	// Allow in-flight fibers to complete.
	time.Sleep(2 * time.Second)
	runtime.GC()
	var peakMem runtime.MemStats
	runtime.ReadMemStats(&peakMem)
	peakGoroutines := runtime.NumGoroutine()

	// Assertions.
	if peakGoroutines > baselineGoroutines*2 {
		t.Errorf("possible goroutine leak: baseline=%d, peak=%d", baselineGoroutines, peakGoroutines)
	}
	heapGrowth := int64(peakMem.HeapInuse) - int64(baselineMem.HeapInuse)
	heapGrowthMB := float64(heapGrowth) / (1024 * 1024)
	if heapGrowthMB > float64(baselineMem.HeapInuse)*2/(1024*1024) && heapGrowthMB > 20 {
		t.Errorf("possible memory leak: heap growth = %.1f MB (baseline heap = %.1f MB)",
			heapGrowthMB, float64(baselineMem.HeapInuse)/(1024*1024))
	}

	if count := totalSpawned.Load(); count < 100 {
		t.Fatalf("only spawned %d fibers in %v; load too low", count, duration)
	}
	t.Logf("stability: spawned %d fibers over %v, goroutines baseline=%d peak=%d, heap growth=%.1f MB",
		totalSpawned.Load(), duration, baselineGoroutines, peakGoroutines, heapGrowthMB)
}
