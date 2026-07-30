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

func BenchmarkWebSocketInit(b *testing.B) {
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	server := NewServer(rt)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			b.Fatal(err)
		}
		initMsg := map[string]interface{}{
			"type": "init",
			"payload": map[string]interface{}{
				"schedulerType": "fifo",
				"numWorkers":    float64(1),
			},
		}
		if err := conn.WriteJSON(initMsg); err != nil {
			b.Fatal(err)
		}
		var resp map[string]interface{}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := conn.ReadJSON(&resp); err != nil {
			b.Fatal(err)
		}
		_ = conn.Close()
	}
}

func BenchmarkWebSocketSpawn(b *testing.B) {
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	server := NewServer(rt)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		b.Fatal(err)
	}
	initMsg := map[string]interface{}{
		"type": "init",
		"payload": map[string]interface{}{
			"schedulerType": "fifo",
			"numWorkers":    float64(1),
		},
	}
	if err := conn.WriteJSON(initMsg); err != nil {
		b.Fatal(err)
	}
	var initResp map[string]interface{}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := conn.ReadJSON(&initResp); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		spawnMsg := map[string]interface{}{
			"type": "spawn",
			"payload": map[string]interface{}{
				"name":     "bench",
				"priority": float64(0),
				"duration": float64(1),
			},
		}
		if err := conn.WriteJSON(spawnMsg); err != nil {
			b.Fatal(err)
		}
		var resp map[string]interface{}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err := conn.ReadJSON(&resp); err != nil {
			b.Fatal(err)
		}
	}
	_ = conn.Close()
}

func BenchmarkMetricsEndpoint(b *testing.B) {
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
	if err := rt.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rt.Stop(context.Background()) }()

	server := NewServer(rt)
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, req)
		resp := w.Result()
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}
