package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// TestHandleStopBoundedByContext is a regression test for the critical bug
// where handleStop called rt.Stop(context.Background()), which never
// cancels. A fiber that never returns would hang the entire WebSocket
// control plane. The fix bounds the call by StopTimeout.
func TestHandleStopBoundedByContext(t *testing.T) {
	t.Parallel()
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfig(rt, ServerConfig{StopTimeout: 100 * time.Millisecond})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	// Open a WS, init the runtime, and spawn a fiber that never returns.
	conn, _, err := websocket.DefaultDialer.Dial("ws"+ts.URL[len("http"):]+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	send := func(t *testing.T, m map[string]interface{}) {
		t.Helper()
		data, _ := json.Marshal(m)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			t.Fatal(err)
		}
	}
	recv := func(t *testing.T, wantType string, timeout time.Duration) map[string]interface{} {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var msg map[string]interface{}
			if json.Unmarshal(data, &msg) != nil {
				continue
			}
			if msg["type"] == wantType {
				return msg
			}
		}
	}

	send(t, map[string]interface{}{"type": "init", "payload": map[string]interface{}{"schedulerType": "fifo", "numWorkers": 1}})
	recv(t, "initSuccess", time.Second)

	send(t, map[string]interface{}{"type": "spawn", "payload": map[string]interface{}{"name": "stuck", "priority": 0, "duration": 60000}})
	recv(t, "success", time.Second)
	time.Sleep(50 * time.Millisecond) // let the fiber start

	// Issue stop; it must return an error within StopTimeout even though
	// the fiber is stuck.
	stopStart := time.Now()
	send(t, map[string]interface{}{"type": "stop", "payload": map[string]interface{}{}})

	// Read messages until we get an error (the stop call returns an error
	// response because the fiber is stuck) or a success (if the fiber
	// managed to drain).
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	gotResponse := false
	for !gotResponse {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("control plane did not respond within 2s; stop blocked for %v (the bug was unbounded context)", time.Since(stopStart))
		}
		var msg map[string]interface{}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		if msg["type"] == "success" || msg["type"] == "error" {
			gotResponse = true
		}
	}
	if elapsed := time.Since(stopStart); elapsed > 2*time.Second {
		t.Fatalf("handleStop took %v; expected <2s with a 100ms StopTimeout", elapsed)
	}
}

// TestShutdownContinuesAfterRuntimeStopTimeout is a regression test for the
// critical bug where Server.Shutdown returned early on rt.Stop timeout,
// leaving the HTTP listener open and skipping the broadcast goroutine
// join. The fix removes the early return so httpServer.Shutdown and
// broadcastWG.Wait always run.
func TestShutdownContinuesAfterRuntimeStopTimeout(t *testing.T) {
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(rt)
	addr := randomAddr(t)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(addr) }()
	defer func() { _ = server.Shutdown(context.Background()) }()

	// Wait for listener.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Spawn a fiber that never returns, so rt.Stop will time out.
	if _, err := rt.Spawn(func() { select {} }, "stuck"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// Now Shutdown with a tiny context; rt.Stop will time out, but the
	// HTTP listener must still be closed.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); err == nil {
		t.Fatal("expected shutdown to report a runtime-stop error, but it returned nil")
	}

	// Verify the HTTP listener is actually closed: a new GET must fail.
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			return // listener is closed; the test passes
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("HTTP listener still accepting connections after Shutdown returned (the early-return bug)")
}

// TestStaticHandlerSetsCacheControl is a regression test for the missing
// Cache-Control header on static responses. Without it, the visualization
// client can cache app.js indefinitely, making future bug fixes invisible
// to existing users.
func TestStaticHandlerSetsCacheControl(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want %q (browser may cache old visualization code)", got, "no-store")
	}
}
