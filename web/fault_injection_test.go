package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

func TestFaultWebSocketDisconnectDuringShutdown(t *testing.T) {
	t.Parallel()

	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfig(rt, ServerConfig{
		StopTimeout: 100 * time.Millisecond,
	})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[len("http"):]+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	writeJSONMsg(t, conn, "init", map[string]interface{}{
		"schedulerType": "fifo", "numWorkers": 1,
	})
	expectMsgType(t, conn, "initSuccess", time.Second)

	writeJSONMsg(t, conn, "spawn", map[string]interface{}{
		"name": "slow", "priority": 0, "duration": 60000,
	})
	expectMsgType(t, conn, "success", time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = server.Shutdown(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown timed out - possible deadlock")
	}
	t.Log("Shutdown completed without panic or deadlock during WS disconnect")
}

func TestFaultMultipleConcurrentShutdowns(t *testing.T) {
	t.Parallel()

	rt := runtime.NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfig(rt, ServerConfig{
		StopTimeout: 100 * time.Millisecond,
	})

	if _, err := rt.Spawn(func() { select {} }, "stuck"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	const callers = 10
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs <- server.Shutdown(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Log("Shutdown returned:", err)
	}
	if rt.IsRunning() {
		t.Fatal("runtime remained running after all Shutdown calls")
	}
	t.Log("All concurrent shutdowns completed cleanly")
}

func TestFaultShutdownDuringSpawn(t *testing.T) {
	t.Parallel()

	rt := runtime.NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfig(rt, ServerConfig{
		MaxFibersPerRuntime: 100000,
		StopTimeout:         100 * time.Millisecond,
	})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[len("http"):]+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	writeJSONMsg(t, conn, "init", map[string]interface{}{
		"schedulerType": "fifo", "numWorkers": 2,
	})
	expectMsgType(t, conn, "initSuccess", time.Second)

	doneSpawn := make(chan struct{})
	var spawnWg sync.WaitGroup
	spawnWg.Add(1)
	go func() {
		defer spawnWg.Done()
		for {
			select {
			case <-doneSpawn:
				return
			default:
			}
			if err := conn.WriteJSON(map[string]interface{}{
				"type": "spawn",
				"payload": map[string]interface{}{
					"name": "fast", "priority": 0, "duration": 10,
				},
			}); err != nil {
				return
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownDone := make(chan struct{})
	go func() {
		_ = server.Shutdown(ctx)
		close(shutdownDone)
	}()
	close(doneSpawn)
	spawnWg.Wait()

	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown timed out during concurrent spawns - possible deadlock")
	}
	t.Log("Shutdown completed without panic or deadlock during concurrent spawns")
}

func TestFaultServerStartStopCycle(t *testing.T) {
	t.Parallel()

	for i := 0; i < 10; i++ {
		rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
		if err := rt.Start(); err != nil {
			t.Fatalf("cycle %d: runtime start: %v", i, err)
		}
		server := NewServer(rt)
		addr := randomAddr(t)

		errCh := make(chan error, 1)
		go func() { errCh <- server.Start(addr) }()

		deadline := time.Now().Add(2 * time.Second)
		started := false
		for time.Now().Before(deadline) {
			resp, err := http.Get("http://" + addr + "/healthz")
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					started = true
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !started {
			t.Fatalf("cycle %d: server never became healthy", i)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := server.Shutdown(ctx); err != nil {
			t.Fatalf("cycle %d: Shutdown: %v", i, err)
		}
		cancel()

		if rt.IsRunning() {
			t.Fatalf("cycle %d: runtime still running after Shutdown", i)
		}
		t.Logf("cycle %d completed", i)
	}
}

func TestFaultHandlerPanicRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		msgType int
		payload []byte
	}{
		{
			name:    "binary data",
			msgType: websocket.BinaryMessage,
			payload: []byte{0x00, 0x01, 0x02, 0x03},
		},
		{
			name:    "malformed JSON",
			msgType: websocket.TextMessage,
			payload: []byte(`{not json`),
		},
		{
			name:    "huge payload within read limit",
			msgType: websocket.TextMessage,
			payload: func() []byte {
				data, _ := json.Marshal(map[string]interface{}{
					"type": "spawn",
					"payload": map[string]interface{}{
						"name":     strings.Repeat("A", 16000),
						"priority": 0,
						"duration": 10,
					},
				})
				return data
			}(),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := NewServer(nil)
			ts := httptest.NewServer(server.Handler())
			defer ts.Close()

			conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[len("http"):]+"/ws", nil)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer func() { _ = conn.Close() }()

			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := conn.WriteMessage(tc.msgType, tc.payload); err != nil {
				t.Fatalf("write: %v", err)
			}

			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			got := false
			for i := 0; i < 10; i++ {
				_, data, err := conn.ReadMessage()
				if err != nil {
					break
				}
				var msg map[string]interface{}
				if json.Unmarshal(data, &msg) != nil {
					continue
				}
				if msg["type"] == "error" {
					got = true
					break
				}
			}
			if !got {
				t.Fatal("did not receive error response; handler may have crashed")
			}
			t.Logf("received error response for %q - handler recovered cleanly", tc.name)
		})
	}
}
