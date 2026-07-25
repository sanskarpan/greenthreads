package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskar/greenthreads/internal/runtime"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

// TestE2EFullLifecycle drives the complete user-facing flow against a real
// server with a real listener: connect WS -> init runtime -> spawn fibers ->
// receive update broadcasts with fiber data -> verify metrics -> stop runtime
// -> verify readyz goes back to 503 -> graceful shutdown. This is the
// end-to-end integration test that unit tests cannot replace.
func TestE2EFullLifecycle(t *testing.T) {
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
	server := NewServer(rt)

	addr := randomAddr(t)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(addr) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	// Wait for the listener to be ready.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	baseURL := "http://" + addr

	// 1. Connect WebSocket.
	wsURL := "ws://" + addr + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("1. CONNECT failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// 2. Init runtime via WS.
	mustWrite(t, conn, map[string]interface{}{
		"type": "init", "payload": map[string]interface{}{"schedulerType": "fifo", "numWorkers": 4},
	})
	if !waitForMsg(t, conn, "initSuccess", 2*time.Second) {
		t.Fatal("2. INIT: never received initSuccess")
	}

	// 3. /readyz should now be 200.
	resp := mustGet(t, baseURL+"/readyz")
	if resp.StatusCode != 200 {
		t.Fatalf("3. READYZ after init = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 4. Spawn 3 fibers.
	for i := 0; i < 3; i++ {
		mustWrite(t, conn, map[string]interface{}{
			"type": "spawn",
			"payload": map[string]interface{}{
				"name": "worker", "priority": i, "duration": 200,
			},
		})
	}

	// 5. Collect 3 success responses.
	for i := 0; i < 3; i++ {
		if !waitForMsg(t, conn, "success", 3*time.Second) {
			t.Fatalf("4. SPAWN: only got %d/3 success responses", i)
		}
	}

	// 6. Verify the broadcast loop delivers updates with fiber data.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	sawFibers := false
	for i := 0; i < 20; i++ {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var msg map[string]interface{}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		if msg["type"] == "update" || msg["type"] == "stateUpdate" {
			if payload, ok := msg["payload"].(map[string]interface{}); ok {
				if fibers, ok := payload["fibers"].([]interface{}); ok && len(fibers) > 0 {
					sawFibers = true
					break
				}
			}
		}
	}
	if !sawFibers {
		t.Fatal("5. UPDATE: never saw fiber data in broadcasts")
	}

	// 7. Wait for completion and check /metrics.
	time.Sleep(500 * time.Millisecond)
	resp = mustGet(t, baseURL+"/metrics")
	body := readBody(resp)
	if !containsStr(body, "greenthreads_fibers_completed 3") {
		t.Fatalf("6. METRICS: expected 3 completed in:\n%s", body)
	}
	if !containsStr(body, "# HELP greenthreads_runtime_running") {
		t.Fatalf("6. METRICS: missing HELP lines")
	}

	// 8. Stop runtime via WS.
	mustWrite(t, conn, map[string]interface{}{
		"type": "stop", "payload": map[string]interface{}{},
	})
	if !waitForMsg(t, conn, "success", 2*time.Second) {
		t.Fatal("7. STOP: never received success")
	}

	// 9. /readyz should be 503 again.
	resp = mustGet(t, baseURL+"/readyz")
	if resp.StatusCode != 503 {
		t.Fatalf("8. READYZ after stop = %d, want 503", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 10. Graceful shutdown of the server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("9. SHUTDOWN failed: %v", err)
	}
}

// TestE2EConcurrentClients drives multiple WS clients simultaneously against a
// real server to surface races or contention the unit suite might miss.
func TestE2EConcurrentClients(t *testing.T) {
	rt := runtime.NewRuntime(scheduler.TypeWorkStealing, 4)
	server := NewServer(rt)

	addr := randomAddr(t)
	go func() { _ = server.Start(addr) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	// Wait for listener.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	const numClients = 5
	// Step 1: client 0 inits the runtime.
	conn0, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn0.Close() }()
	mustWrite(t, conn0, map[string]interface{}{
		"type": "init", "payload": map[string]interface{}{"schedulerType": "workstealing", "numWorkers": 4},
	})
	if !waitForMsg(t, conn0, "initSuccess", 5*time.Second) {
		t.Fatal("init failed")
	}
	_ = conn0.Close()

	// Step 2: multiple clients connect and spawn concurrently.
	done := make(chan error, numClients)
	for i := 0; i < numClients; i++ {
		go func(id int) {
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
			if err != nil {
				done <- fmt.Errorf("client %d connect: %w", id, err)
				return
			}
			defer func() { _ = conn.Close() }()
			for j := 0; j < 3; j++ {
				mustWrite(t, conn, map[string]interface{}{
					"type": "spawn",
					"payload": map[string]interface{}{
						"name": fmt.Sprintf("c%d-w%d", id, j), "priority": 0, "duration": 50,
					},
				})
			}
			// Read until we see 3 successes.
			successes := 0
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			for successes < 3 {
				_, data, err := conn.ReadMessage()
				if err != nil {
					done <- fmt.Errorf("client %d: read after %d/3: %w", id, successes, err)
					return
				}
				var msg map[string]interface{}
				if json.Unmarshal(data, &msg) == nil && msg["type"] == "success" {
					successes++
				}
			}
			done <- nil
		}(i)
	}

	for i := 0; i < numClients; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("client failed: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("timeout waiting for concurrent clients")
		}
	}
}

func mustWrite(t *testing.T, conn *websocket.Conn, msg map[string]interface{}) {
	t.Helper()
	data, _ := json.Marshal(msg)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write failed: %v", err)
	}
}

func waitForMsg(t *testing.T, conn *websocket.Conn, wantType string, timeout time.Duration) bool {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return false
		}
		var msg map[string]interface{}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		if msg["type"] == wantType {
			return true
		}
	}
}

//nolint:gosec // G107: test URLs are constants, not user-controlled.
func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	return resp
}

func readBody(resp *http.Response) string {
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return string(body)
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestBroadcastSurvivesConcurrentDisconnect is a regression guard for the
// deep-scrutiny finding that broadcastLoop could panic on send-to-closed-channel
// when a client disconnects during a broadcast tick. The fix replaced
// close(sendCh) with a separate stop channel. This test repeatedly connects
// and disconnects clients while the broadcast loop is running, verifying no
// panic occurs.
func TestBroadcastSurvivesConcurrentDisconnect(t *testing.T) {
	t.Parallel()
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	server := NewServer(rt)
	addr := randomAddr(t)
	go func() { _ = server.Start(addr) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	// Wait for listener.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Init the runtime so the broadcast loop has data to send.
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, conn, map[string]interface{}{
		"type": "init", "payload": map[string]interface{}{"schedulerType": "fifo", "numWorkers": 1},
	})
	if !waitForMsg(t, conn, "initSuccess", 2*time.Second) {
		t.Fatal("init failed")
	}
	// Spawn a long-running fiber so the broadcast loop keeps sending updates.
	mustWrite(t, conn, map[string]interface{}{
		"type": "spawn", "payload": map[string]interface{}{"name": "long", "priority": 0, "duration": 3000},
	})

	// Rapidly connect and disconnect 20 clients while broadcasts are active.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			c, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
			if err != nil {
				continue
			}
			// Immediately disconnect — this races with the broadcast tick.
			_ = c.Close()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Wait for the disconnector to finish, then verify the server is still
	// alive and responsive (no panic crashed the broadcast loop).
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("disconnect loop timed out")
	}

	// The original client should still be receiving updates.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	gotUpdate := false
	for i := 0; i < 10; i++ {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var msg map[string]interface{}
		if json.Unmarshal(data, &msg) == nil && msg["type"] == "update" {
			gotUpdate = true
			break
		}
	}
	if !gotUpdate {
		t.Fatal("broadcast loop stopped after concurrent disconnects (panic?)")
	}
	_ = conn.Close()
}
