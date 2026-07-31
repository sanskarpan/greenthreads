package web

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

// startServerForTest starts s on a fresh loopback address and returns the ws://
// base URL plus a cleanup func. It blocks until /healthz responds.
func startServerForTest(t *testing.T, s *Server) (wsBase string, cleanup func()) {
	t.Helper()
	addr := randomAddr(t)
	go func() { _ = s.Start(addr) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}
	return "ws://" + addr + "/ws", cleanup
}

// TestReadOnlyClientDeniedDestructiveCommands is the regression test for the
// SEC-2 read-only authorization control (previously 0 tests): a client
// authenticating with ReadOnlyToken must be refused every destructive command,
// while an admin client with AuthToken is allowed.
func TestReadOnlyClientDeniedDestructiveCommands(t *testing.T) {
	const adminTok = "admin-token-that-is-at-least-32-chars"
	const roTok = "readonly-token-that-is-at-least-32chars"

	cfg := DefaultConfig()
	cfg.AuthToken = adminTok
	cfg.ReadOnlyToken = roTok

	rt := runtime.NewRuntime(scheduler.TypeFIFO, 2)
	if err := rt.Start(); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	server := NewServerWithConfig(rt, cfg)
	wsBase, cleanup := startServerForTest(t, server)
	defer cleanup()

	// Read-only client: every destructive command must be rejected.
	roConn, _, err := websocket.DefaultDialer.Dial(wsBase, http.Header{
		"Authorization": []string{"Bearer " + roTok},
	})
	if err != nil {
		t.Fatalf("read-only dial failed: %v", err)
	}
	defer func() { _ = roConn.Close() }()

	for _, cmd := range []string{"spawn", "init", "stop", "reset"} {
		mustWrite(t, roConn, map[string]interface{}{
			"type":    cmd,
			"payload": map[string]interface{}{"name": "x", "schedulerType": "fifo", "numWorkers": 1},
		})
		if !waitForMsg(t, roConn, "error", 2*time.Second) {
			t.Fatalf("read-only client was NOT refused destructive command %q", cmd)
		}
	}

	// getState (non-destructive) must be allowed for a read-only client — it
	// must not produce an error response.
	mustWrite(t, roConn, map[string]interface{}{"type": "getState", "payload": map[string]interface{}{}})
	if waitForMsg(t, roConn, "error", 500*time.Millisecond) {
		t.Fatal("read-only client got an error for the allowed getState command")
	}

	// Admin client: the same destructive command must succeed.
	adminConn, _, err := websocket.DefaultDialer.Dial(wsBase, http.Header{
		"Authorization": []string{"Bearer " + adminTok},
	})
	if err != nil {
		t.Fatalf("admin dial failed: %v", err)
	}
	defer func() { _ = adminConn.Close() }()

	mustWrite(t, adminConn, map[string]interface{}{
		"type":    "spawn",
		"payload": map[string]interface{}{"name": "worker", "priority": 0, "duration": 100},
	})
	if !waitForMsg(t, adminConn, "success", 2*time.Second) {
		t.Fatal("admin client's spawn did not succeed")
	}
}

// TestMaxClientsRejectsExcessConnections is the regression test for the
// MaxClients admission cap (previously 0 tests): once the cap is reached, the
// next WebSocket upgrade must be refused, and a slot must free on disconnect.
func TestMaxClientsRejectsExcessConnections(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClients = 2

	server := NewServerWithConfig(runtime.NewRuntime(scheduler.TypeFIFO, 1), cfg)
	wsBase, cleanup := startServerForTest(t, server)
	defer cleanup()

	// Fill both slots.
	c1, _, err := websocket.DefaultDialer.Dial(wsBase, nil)
	if err != nil {
		t.Fatalf("client 1 dial: %v", err)
	}
	defer func() { _ = c1.Close() }()
	c2, _, err := websocket.DefaultDialer.Dial(wsBase, nil)
	if err != nil {
		t.Fatalf("client 2 dial: %v", err)
	}

	// Third connection must be refused with 503.
	_, resp, err := websocket.DefaultDialer.Dial(wsBase, nil)
	if err == nil {
		t.Fatal("third client was admitted; MaxClients not enforced")
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		got := 0
		if resp != nil {
			got = resp.StatusCode
		}
		t.Fatalf("third client rejected with status %d, want 503", got)
	}

	// Free a slot and confirm a new client is admitted.
	_ = c2.Close()
	deadline := time.Now().Add(2 * time.Second)
	var admitted bool
	for time.Now().Before(deadline) {
		c3, _, derr := websocket.DefaultDialer.Dial(wsBase, nil)
		if derr == nil {
			_ = c3.Close()
			admitted = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !admitted {
		t.Fatal("a slot did not free after a client disconnected")
	}
}

// TestRateLimitRejectsFlood is the regression test for the per-client message
// rate limit (previously untested for enforcement): a client exceeding
// MessagesPerSecond must receive a rate-limit error rather than have every
// message processed.
func TestRateLimitRejectsFlood(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MessagesPerSecond = 5

	server := NewServerWithConfig(runtime.NewRuntime(scheduler.TypeFIFO, 1), cfg)
	wsBase, cleanup := startServerForTest(t, server)
	defer cleanup()

	conn, _, err := websocket.DefaultDialer.Dial(wsBase, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send well over the limit within one window.
	for i := 0; i < 40; i++ {
		mustWrite(t, conn, map[string]interface{}{"type": "getState", "payload": map[string]interface{}{}})
	}

	// At least one response must be the rate-limit error.
	if !waitForMsg(t, conn, "error", 2*time.Second) {
		t.Fatal("flooding client never received a rate-limit error")
	}
}
