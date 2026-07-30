package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
)

func TestHealthAndReadinessEndpoints(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	response, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", response.StatusCode)
	}
	_ = response.Body.Close()

	response, err = http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d, want 503", response.StatusCode)
	}
	_ = response.Body.Close()
}

func TestWebSocketRejectsCrossOrigin(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	url := "ws" + ts.URL[len("http"):] + "/ws"
	_, response, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": []string{"https://evil.example"}})
	if err == nil {
		t.Fatal("cross-origin WebSocket unexpectedly connected")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %v, want 403", response)
	}
}

func TestWebSocketMalformedMessageReturnsError(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	url := "ws" + ts.URL[len("http"):] + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"init","payload":{"schedulerType":null,"numWorkers":"bad"}}`)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var response map[string]interface{}
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response["type"] != "error" {
		t.Fatalf("response type = %v, want error", response["type"])
	}
}

func TestWebSocketAuthToken(t *testing.T) {
	t.Parallel()
	server := NewServerWithConfig(nil, ServerConfig{AuthToken: "secret"})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	url := "ws" + ts.URL[len("http"):] + "/ws"
	if _, _, err := websocket.DefaultDialer.Dial(url, nil); err == nil {
		t.Fatal("unauthenticated WebSocket unexpectedly connected")
	}
	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": []string{"Bearer secret"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestReadyAfterRuntimeStart(t *testing.T) {
	t.Parallel()
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	server := NewServer(rt)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()
	response, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("readiness status = %d", response.StatusCode)
	}
	_ = response.Body.Close()
}

func wsURL(httpURL string) string { return "ws" + httpURL[len("http"):] + "/ws" }

func writeWSMessage(conn *websocket.Conn, msgType string, payload map[string]interface{}) error {
	return conn.WriteJSON(map[string]interface{}{"type": msgType, "payload": payload})
}

// TestEnqueueDropsWhenFullAndNeverBlocks is a regression guard for AUDIT ID 3:
// the per-client send queue is bounded and enqueue must never block the
// producer. A full queue drops (with a log) instead of holding the broadcast
// loop, so one slow client cannot stall delivery to other clients.
func TestEnqueueDropsWhenFullAndNeverBlocks(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	cl := newClient(nil)

	for i := 0; i < clientSendBuffer; i++ {
		select {
		case cl.sendCh <- i:
		default:
			t.Fatalf("queue should accept up to its buffer (%d)", clientSendBuffer)
		}
	}

	// Further enqueues must drop immediately, not block.
	done := make(chan struct{})
	go func() {
		server.enqueue(cl, "overflow-1")
		server.enqueue(cl, "overflow-2")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked on a full queue; slow client would stall broadcasts")
	}

	if got := len(cl.sendCh); got != clientSendBuffer {
		t.Fatalf("queue size = %d, want %d (drops must not grow the queue)", got, clientSendBuffer)
	}
}

// TestPingKeepsIdleConnectionAlive is a regression guard for AUDIT ID 4: with
// a short read idle timeout and a shorter ping interval, an idle connection
// must stay alive past the read deadline because the server sends pings and
// the PongHandler resets the deadline. Before the fix (no pings), the idle
// connection was closed at the read deadline.
func TestPingKeepsIdleConnectionAlive(t *testing.T) {
	t.Parallel()
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	server := NewServerWithConfig(rt, ServerConfig{
		PingInterval:    100 * time.Millisecond,
		ReadIdleTimeout: 250 * time.Millisecond,
	})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(ts.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if err := writeWSMessage(conn, "init", map[string]interface{}{
		"schedulerType": "fifo", "numWorkers": 1,
	}); err != nil {
		t.Fatal(err)
	}

	// Sleep well past the read idle timeout. Without pings, the server would
	// have closed the connection at the deadline; with pings it stays open.
	time.Sleep(600 * time.Millisecond)

	// The connection must still be usable: a getState request must elicit a
	// response.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := writeWSMessage(conn, "getState", map[string]interface{}{}); err != nil {
		t.Fatalf("getState write failed after idle (connection was likely killed): %v", err)
	}
	for {
		var msg map[string]interface{}
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatalf("connection died during idle period (no ping keepalive?): %v", err)
		}
		if msg["type"] == "stateUpdate" {
			break
		}
	}
}

// TestMetricsRequiresAuthWhenTokenConfigured is a regression guard for AUDIT
// ID 6: /metrics must require the auth token on non-loopback-style configs
// (i.e. when a token is configured), so operational counters are not exposed
// to unauthenticated callers.
func TestMetricsRequiresAuthWhenTokenConfigured(t *testing.T) {
	t.Parallel()
	server := NewServerWithConfig(nil, ServerConfig{AuthToken: "secret"})
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unauthenticated /metrics status = %d, want 403", resp.StatusCode)
	}
	_ = resp.Body.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated /metrics status = %d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// TestMetricsEndpointHasPrometheusMetadata is a regression guard for AUDIT ID
// 16: /metrics must emit # HELP and # TYPE lines for every metric so the
// exposition is self-describing for Prometheus, and must expose the additional
// counters that exist in the snapshot (steal, blocked, peak, etc.).
func TestMetricsEndpointHasPrometheusMetadata(t *testing.T) {
	t.Parallel()
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 1)
	server := NewServer(rt)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	if err := rt.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, name := range []string{
		"# HELP greenthreads_runtime_running",
		"# TYPE greenthreads_runtime_running gauge",
		"# HELP greenthreads_fibers_blocked",
		"# HELP greenthreads_steal_attempts",
		"# HELP greenthreads_peak_fiber_count",
		"# HELP greenthreads_uptime_seconds",
	} {
		if !strings.Contains(text, name) {
			t.Errorf("metrics body missing %q\n%s", name, text)
		}
	}
}

// TestSecurityHeadersAppliedToResponses is a regression guard for AUDIT ID 21:
// every response carries defense-in-depth headers (CSP, nosniff, frame deny).
func TestSecurityHeadersAppliedToResponses(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	checks := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
		"Content-Security-Policy": "default-src 'self'",
	}
	for h, want := range checks {
		if got := resp.Header.Get(h); !strings.HasPrefix(got, want) {
			t.Errorf("header %s = %q, want prefix %q", h, got, want)
		}
	}
}

// TestIsLoopbackAddress is a regression guard for AUDIT ID 20: the canonical
// helper must classify loopback, wildcard, and external binds consistently.
func TestIsLoopbackAddress(t *testing.T) {
	t.Parallel()
	cases := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:8080", true},
		{"localhost:8080", true},
		{"[::1]:8080", true},
		{"::1:8080", false}, // ambiguous without brackets; not a valid host:port split -> false
		{":8080", false},   // wildcard bind, not loopback
		{"0.0.0.0:8080", false},
		{"10.0.0.1:8080", false},
		{"not-an-address", false},
	}
	for _, c := range cases {
		if got := IsLoopbackAddress(c.addr); got != c.want {
			t.Errorf("IsLoopbackAddress(%q) = %v, want %v", c.addr, got, c.want)
		}
	}
}
