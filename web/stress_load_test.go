package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// randomAddr returns a localhost address on a free port. There is a brief race
// between closing the probe listener and the caller binding the same port, but
// for test isolation this is acceptable.
func randomAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("randomAddr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// startServerOn starts s on addr and blocks until /healthz is responsive or the
// deadline passes. It returns a cleanup that shuts the server down.
func startServerOn(t *testing.T, s *Server, addr string) func() {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Start(addr) }()
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return cleanup
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cleanup()
	t.Fatalf("server on %s never became healthy", addr)
	return nil
}

// drain reads all messages from conn in the background until conn closes,
// discarding them. Keeps the server's send queue from filling on a long-lived
// single client that never reads.
func drain(conn *websocket.Conn, done <-chan struct{}) {
	go func() {
		_ = conn.SetReadDeadline(time.Time{})
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, _, err := conn.ReadMessage(); err != nil {
				select {
				case <-done:
					return
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}()
}

// readMsg reads one JSON message within timeout. Returns the parsed message
// and any error.
func readMsg(conn *websocket.Conn, timeout time.Duration) (map[string]interface{}, error) {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// writeJSONMsg writes a JSON envelope on conn.
func writeJSONMsg(t *testing.T, conn *websocket.Conn, typ string, payload map[string]interface{}) {
	t.Helper()
	if err := conn.WriteJSON(map[string]interface{}{"type": typ, "payload": payload}); err != nil {
		t.Fatalf("write %s: %v", typ, err)
	}
}

// expectMsgType reads messages until one matching wantType arrives or timeout.
func expectMsgType(t *testing.T, conn *websocket.Conn, wantType string, timeout time.Duration) map[string]interface{} {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, err := readMsg(conn, time.Until(deadline))
		if err != nil {
			t.Fatalf("expectMsgType %q: read error: %v", wantType, err)
		}
		if msg["type"] == wantType {
			return msg
		}
	}
	t.Fatalf("expectMsgType %q: timed out", wantType)
	return nil
}

// expectErrorMsg reads messages until an error arrives and returns its message.
func expectErrorMsg(t *testing.T, conn *websocket.Conn, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, err := readMsg(conn, time.Until(deadline))
		if err != nil {
			t.Fatalf("expectErrorMsg: read error: %v", err)
		}
		if msg["type"] == "error" {
			if p, ok := msg["payload"].(map[string]interface{}); ok {
				if m, ok := p["message"].(string); ok {
					return m
				}
			}
			return ""
		}
	}
	t.Fatalf("expectErrorMsg: timed out")
	return ""
}

// metricsValue extracts a numeric metric from the /metrics text body.
func metricsValue(body, name string) (string, bool) {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, name+" ") {
			return strings.TrimSpace(line[len(name)+1:]), true
		}
	}
	return "", false
}

func getMetrics(t *testing.T, baseURL string) string {
	t.Helper()
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return string(body)
}

// TestStressSoakLoad runs a single client continuously spawning short-lived
// fibers for 10s and verifies: no panics, healthz stays 200 throughout, and
// after the soak fibers_created > 0 and fibers_active returns to 0.
func TestStressSoakLoad(t *testing.T) {
	server := NewServerWithConfig(nil, ServerConfig{
		MaxClients:          64,
		MessagesPerSecond:   10000,
		MaxFibersPerRuntime: 100000,
	})
	addr := randomAddr(t)
	cleanup := startServerOn(t, server, addr)
	defer cleanup()
	baseURL := "http://" + addr

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writeJSONMsg(t, conn, "init", map[string]interface{}{
		"schedulerType": "workstealing", "numWorkers": 8,
	})
	expectMsgType(t, conn, "initSuccess", 3*time.Second)

	stopDrain := make(chan struct{})
	defer close(stopDrain)
	drain(conn, stopDrain)

	// Healthz watchdog: must stay 200 throughout the soak.
	var healthOK int64 = 1
	var hwg sync.WaitGroup
	hwg.Add(1)
	soakEnd := time.Now().Add(10 * time.Second)
	go func() {
		defer hwg.Done()
		for time.Now().Before(soakEnd) {
			resp, err := http.Get(baseURL + "/healthz")
			if err != nil || resp.StatusCode != 200 {
				if resp != nil {
					_ = resp.Body.Close()
				}
				atomic.StoreInt64(&healthOK, 0)
				return
			}
			_ = resp.Body.Close()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	// Spawn loop: continuously spawn fibers that sleep 50ms.
	spawnErrCh := make(chan error, 1)
	var swg sync.WaitGroup
	swg.Add(1)
	go func() {
		defer swg.Done()
		defer func() {
			if r := recover(); r != nil {
				spawnErrCh <- fmt.Errorf("spawn goroutine panic: %v", r)
			}
		}()
		count := 0
		for time.Now().Before(soakEnd) {
			if err := conn.WriteJSON(map[string]interface{}{
				"type": "spawn",
				"payload": map[string]interface{}{
					"name": "soak", "priority": 0, "duration": 50,
				},
			}); err != nil {
				spawnErrCh <- fmt.Errorf("write spawn %d: %w", count, err)
				return
			}
			count++
			time.Sleep(15 * time.Millisecond)
		}
	}()
	swg.Wait()
	hwg.Wait()

	// Allow in-flight fibers to finish.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		body := getMetrics(t, baseURL)
		if v, ok := metricsValue(body, "greenthreads_fibers_active"); ok && v == "0" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	body := getMetrics(t, baseURL)
	created, _ := metricsValue(body, "greenthreads_fibers_created")
	active, _ := metricsValue(body, "greenthreads_fibers_active")
	if created == "0" {
		t.Errorf("soak: expected fibers_created > 0, got %s\n%s", created, body)
	}
	if active != "0" {
		t.Errorf("soak: expected fibers_active=0 after drain, got %s\n%s", active, body)
	}
	if atomic.LoadInt64(&healthOK) != 1 {
		t.Errorf("soak: healthz was not 200 throughout the run")
	}
	select {
	case err := <-spawnErrCh:
		t.Errorf("soak: spawn loop reported error: %v", err)
	default:
	}
}

// TestStressRapidConnectDisconnect rapidly connects and closes 50 WS clients
// with no init. Then verifies a real client can still init+spawn.
func TestStressRapidConnectDisconnect(t *testing.T) {
	t.Parallel()
	server := NewServer(nil)
	addr := randomAddr(t)
	cleanup := startServerOn(t, server, addr)
	defer cleanup()
	baseURL := "http://" + addr

	const n = 50
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
			if err != nil {
				errOnce.Do(func() { firstErr = fmt.Errorf("client %d dial: %w", id, err) })
				return
			}
			_ = conn.Close()
		}(i)
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("rapid connect/disconnect failed: %v", firstErr)
	}

	// Server must still be responsive.
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("healthz after churn: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("healthz status=%d after churn", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Real client can init + spawn.
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("real client dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	writeJSONMsg(t, conn, "init", map[string]interface{}{
		"schedulerType": "fifo", "numWorkers": 2,
	})
	expectMsgType(t, conn, "initSuccess", 3*time.Second)
	writeJSONMsg(t, conn, "spawn", map[string]interface{}{
		"name": "post-churn", "priority": 0, "duration": 50,
	})
	expectMsgType(t, conn, "success", 3*time.Second)
}

// TestStressBoundaryPayloads sends spawn/init payloads at the edges of the
// documented limits and verifies each returns success or error as expected.
func TestStressBoundaryPayloads(t *testing.T) {
	t.Parallel()
	server := NewServerWithConfig(nil, ServerConfig{
		MessagesPerSecond: 10000,
	})
	addr := randomAddr(t)
	cleanup := startServerOn(t, server, addr)
	defer cleanup()

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writeJSONMsg(t, conn, "init", map[string]interface{}{
		"schedulerType": "fifo", "numWorkers": 4,
	})
	expectMsgType(t, conn, "initSuccess", 3*time.Second)

	name128 := strings.Repeat("a", 128)
	name129 := strings.Repeat("a", 129)

	type tc struct {
		name   string
		typ    string
		payload map[string]interface{}
		want    string // "success" or "error"
	}
	cases := []tc{
		{"empty-name-defaults", "spawn", map[string]interface{}{"name": "", "priority": 0, "duration": 10}, "success"},
		{"name-128-ok", "spawn", map[string]interface{}{"name": name128, "priority": 0, "duration": 10}, "success"},
		{"name-129-err", "spawn", map[string]interface{}{"name": name129, "priority": 0, "duration": 10}, "error"},
		{"priority-100-ok", "spawn", map[string]interface{}{"name": "p", "priority": 100, "duration": 10}, "success"},
		{"priority-101-err", "spawn", map[string]interface{}{"name": "p", "priority": 101, "duration": 10}, "error"},
		{"duration-1-ok", "spawn", map[string]interface{}{"name": "p", "priority": 0, "duration": 1}, "success"},
		{"duration-60000-ok", "spawn", map[string]interface{}{"name": "p", "priority": 0, "duration": 60000}, "success"},
		{"duration-60001-err", "spawn", map[string]interface{}{"name": "p", "priority": 0, "duration": 60001}, "error"},
		{"init-numWorkers-0-err", "init", map[string]interface{}{"schedulerType": "fifo", "numWorkers": 0}, "error"},
		{"init-bad-scheduler-err", "init", map[string]interface{}{"schedulerType": "badtype", "numWorkers": 4}, "error"},
	}

	// The duration=60000 case keeps a fiber alive for 60s. That's fine for
	// server responsiveness; we just won't wait for it. Track successes.
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			writeJSONMsg(t, conn, c.typ, c.payload)
			msg, err := readMsg(conn, 3*time.Second)
			if err != nil {
				t.Errorf("%s: read response: %v", c.name, err)
				return
			}
			got, _ := msg["type"].(string)
			// Skip broadcast "update"/"stateUpdate" messages, read next.
			for got == "update" || got == "stateUpdate" {
				msg, err = readMsg(conn, 3*time.Second)
				if err != nil {
					t.Errorf("%s: read response (skip): %v", c.name, err)
					return
				}
				got, _ = msg["type"].(string)
			}
			if got != c.want {
				t.Errorf("%s: got type %q, want %q (payload=%v)", c.name, got, c.want, c.payload)
			}
		})
	}

	// Server still responsive after boundary payloads.
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("healthz after boundary: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("healthz status=%d after boundary", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// TestStressNonLoopbackAuth verifies the auth boundary on /ws and /metrics when
// an AuthToken is configured.
func TestStressNonLoopbackAuth(t *testing.T) {
	t.Parallel()
	server := NewServerWithConfig(nil, ServerConfig{AuthToken: "testsecret"})
	addr := randomAddr(t)
	cleanup := startServerOn(t, server, addr)
	defer cleanup()
	baseURL := "http://" + addr

	// WS without token: 403.
	if _, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil); err == nil {
		t.Errorf("WS connect without token unexpectedly succeeded")
	}
	// WS with wrong token: 403.
	if _, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws",
		http.Header{"Authorization": []string{"Bearer wrong"}}); err == nil {
		t.Errorf("WS connect with wrong token unexpectedly succeeded")
	}
	// WS with correct Bearer: success.
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws",
		http.Header{"Authorization": []string{"Bearer testsecret"}})
	if err != nil {
		t.Fatalf("WS connect with correct token failed: %v", err)
	}
	_ = conn.Close()

	// GET /metrics without token: 403.
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics without token: %v", err)
	}
	if resp.StatusCode != 403 {
		t.Errorf("/metrics without token status=%d, want 403", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// GET /metrics with correct Bearer: 200.
	req, _ := http.NewRequest("GET", baseURL+"/metrics", nil)
	req.Header.Set("Authorization", "Bearer testsecret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /metrics with token: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("/metrics with token status=%d, want 200", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// TestStressSpawnToLimit verifies the per-runtime fiber cap is enforced on
// concurrently-active fibers and lifts once they complete.
func TestStressSpawnToLimit(t *testing.T) {
	t.Parallel()
	server := NewServerWithConfig(nil, ServerConfig{
		MaxFibersPerRuntime: 5,
		MessagesPerSecond:   10000,
	})
	addr := randomAddr(t)
	cleanup := startServerOn(t, server, addr)
	defer cleanup()

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writeJSONMsg(t, conn, "init", map[string]interface{}{
		"schedulerType": "fifo", "numWorkers": 2,
	})
	expectMsgType(t, conn, "initSuccess", 3*time.Second)

	// Spawn 5 long-lived fibers (500ms) so they stay active.
	for i := 0; i < 5; i++ {
		writeJSONMsg(t, conn, "spawn", map[string]interface{}{
			"name": "lim", "priority": 0, "duration": 500,
		})
		expectMsgType(t, conn, "success", 3*time.Second)
	}

	// 6th should be rejected with "fiber limit reached".
	writeJSONMsg(t, conn, "spawn", map[string]interface{}{
		"name": "lim", "priority": 0, "duration": 500,
	})
	errMsg := expectErrorMsg(t, conn, 3*time.Second)
	if !strings.Contains(errMsg, "fiber limit reached") {
		t.Errorf("6th spawn error=%q, want substring 'fiber limit reached'", errMsg)
	}

	// Wait for the 5 to complete, then spawn should succeed again.
	time.Sleep(2 * time.Second)
	writeJSONMsg(t, conn, "spawn", map[string]interface{}{
		"name": "after", "priority": 0, "duration": 10,
	})
	expectMsgType(t, conn, "success", 3*time.Second)

	// active count should be back near 0.
	time.Sleep(500 * time.Millisecond)
	body := getMetrics(t, "http://"+addr)
	if v, ok := metricsValue(body, "greenthreads_fibers_active"); ok && v != "0" {
		t.Errorf("after drain fibers_active=%s, want 0\n%s", v, body)
	}
}
