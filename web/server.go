// Package web exposes the bounded visualization and control plane.
package web

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"runtime/debug"

	"github.com/gorilla/websocket"
	"github.com/sanskar/greenthreads/internal/metrics"
	"github.com/sanskar/greenthreads/internal/runtime"
	"github.com/sanskar/greenthreads/internal/scheduler"
)

// Static assets are embedded so serving does not depend on the process working
// directory.
//
//go:embed static/*
var embeddedStatic embed.FS

const (
	defaultMaxClients          = 64
	defaultMaxMessageBytes     = 32 * 1024
	defaultMessagesPerSecond   = 30
	defaultMaxFibersPerRuntime = 10000
	defaultPingInterval        = 30 * time.Second
	defaultReadIdleTimeout     = 60 * time.Second
	clientSendBuffer           = 16
	writeTimeout               = 2 * time.Second
)

// ServerConfig bounds the control plane and defines its trust boundary.
type ServerConfig struct {
	AllowedOrigins      []string
	AuthToken           string
	MaxClients          int
	MaxMessageBytes     int64
	MessagesPerSecond   int
	MaxFibersPerRuntime int64
	PingInterval        time.Duration
	ReadIdleTimeout     time.Duration
	// StopTimeout caps how long runtime Stop is allowed to block the control
	// plane while waiting for in-flight fibers to drain. A fiber that never
	// returns must not freeze the WebSocket handler that triggered it.
	StopTimeout time.Duration
	// AllowTokenInQuery permits the auth token via ?token= in the URL. Browser
	// WebSocket clients cannot set Authorization headers, so this is the only
	// way for a same-origin browser UI to authenticate. The token then leaks
	// into access logs, browser history, and Referer headers; it is opt-in and
	// defaults to true for the loopback-oriented default config so the
	// visualization keeps working out of the box. Disable it for deployments
	// that authenticate non-browser clients only.
	AllowTokenInQuery bool
}

func defaultConfig() ServerConfig {
	return ServerConfig{
		AuthToken:           os.Getenv("GREENTHREADS_AUTH_TOKEN"),
		MaxClients:          defaultMaxClients,
		MaxMessageBytes:     defaultMaxMessageBytes,
		MessagesPerSecond:   defaultMessagesPerSecond,
		MaxFibersPerRuntime: defaultMaxFibersPerRuntime,
		PingInterval:        defaultPingInterval,
		ReadIdleTimeout:     defaultReadIdleTimeout,
		StopTimeout:         5 * time.Second,
		AllowTokenInQuery:   true,
	}
}

type client struct {
	conn         *websocket.Conn
	sendCh       chan interface{}
	stop         chan struct{}
	stopOnce     sync.Once
	done         chan struct{}
	windowStart  time.Time
	messageCount int
}

// newClient allocates a client with a bounded send queue and a done signal.
// The caller starts the writePump goroutine after registering the client.
// sendCh is never closed; the stop channel (closed once via stopOnce) signals
// writePump to exit, so broadcastLoop can enqueue without risking a
// send-on-closed-channel panic.
func newClient(conn *websocket.Conn) *client {
	return &client{
		conn:        conn,
		sendCh:      make(chan interface{}, clientSendBuffer),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		windowStart: time.Now(),
	}
}

// shutdown signals the writePump to exit via the stop channel. It is safe to
// call from multiple paths (closeClients, unregister) because stopOnce
// guarantees the channel is closed exactly once.
func (cl *client) shutdown() {
	cl.stopOnce.Do(func() { close(cl.stop) })
}

// Server exposes the local visualization and its bounded control protocol.
type Server struct {
	runtime   *runtime.Runtime
	runtimeMu sync.RWMutex
	clients   map[*websocket.Conn]*client
	clientsMu sync.RWMutex
	// clientCount is an atomic admission counter so a slot can be reserved
	// without holding clientsMu during the (potentially slow) WS handshake.
	clientCount int64

	config ServerConfig
	logger *slog.Logger

	httpServer  *http.Server
	serverMu    sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	broadcastWG sync.WaitGroup
}

// NewServer creates a server. By default it allows same-origin browser access
// and is intended to be bound to loopback; deployments on other interfaces
// must set GREENTHREADS_AUTH_TOKEN and a deliberate origin policy.
func NewServer(rt *runtime.Runtime) *Server {
	return NewServerWithConfig(rt, defaultConfig())
}

// NewServerWithConfig creates a server with explicit security and resource
// limits.
func NewServerWithConfig(rt *runtime.Runtime, config ServerConfig) *Server {
	if config.MaxClients <= 0 {
		config.MaxClients = defaultMaxClients
	}
	if config.MaxMessageBytes <= 0 || config.MaxMessageBytes > 1<<20 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.MessagesPerSecond <= 0 {
		config.MessagesPerSecond = defaultMessagesPerSecond
	}
	if config.MaxFibersPerRuntime <= 0 {
		config.MaxFibersPerRuntime = defaultMaxFibersPerRuntime
	}
	if config.PingInterval <= 0 {
		config.PingInterval = defaultPingInterval
	}
	if config.ReadIdleTimeout <= 0 {
		config.ReadIdleTimeout = defaultReadIdleTimeout
	}
	if config.StopTimeout <= 0 {
		config.StopTimeout = 5 * time.Second
	}
	return &Server{
		runtime: rt,
		clients: make(map[*websocket.Conn]*client),
		config:  config,
		logger:  slog.Default(),
	}
}

// Handler returns a private HTTP mux suitable for http.Server or httptest.
// Health and readiness are intentionally public (probes). /metrics requires
// the same authorization as the WebSocket control channel when a token is
// configured, so operational counters are not exposed on non-loopback binds.
// Static assets and the WebSocket endpoint apply origin and (for /ws) upgrade
// checks at their handlers.
func (s *Server) Handler() http.Handler {
	staticRoot, err := fs.Sub(embeddedStatic, "static")
	var staticHandler http.Handler
	if err != nil {
		staticHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})
	} else {
		staticHandler = noCache(http.FileServer(http.FS(staticRoot)))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", s.authorizedHandler(s.handleMetrics))
	mux.Handle("/", staticHandler)
	return securityHeaders(requestID(mux))
}

// noCache sets Cache-Control: no-store on every response so browser clients
// always see the latest visualization assets. Without it, http.FileServer
// emits no cache directives and the browser may cache app.js indefinitely,
// making future visualization fixes invisible without a hard reload.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// Start serves until Shutdown or a listener error.
func (s *Server) Start(addr string) error {
	if strings.TrimSpace(addr) == "" {
		return fmt.Errorf("listen address must not be empty")
	}
	if !IsLoopbackAddress(addr) && s.config.AuthToken == "" {
		return fmt.Errorf("GREENTHREADS_AUTH_TOKEN is required for non-loopback listeners")
	}
	s.serverMu.Lock()
	if s.httpServer != nil {
		s.serverMu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	httpServer := s.httpServer
	s.broadcastWG.Add(1)
	s.serverMu.Unlock()
	go s.broadcastLoop(s.ctx)

	s.logger.Info("server starting", "addr", addr)
	err := httpServer.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// IsLoopbackAddress reports whether addr (host:port) binds to a loopback
// interface. It is the single canonical helper shared by cmd/server and the
// web server so the two cannot diverge on edge cases (empty host, IPv6,
// bracketed hosts). An empty host means a wildcard bind and is not loopback.
func IsLoopbackAddress(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
	}
// Shutdown stops HTTP serving, runtime admission, client connections, and
// the update broadcaster within the supplied context. Each phase is bounded
// by ctx; runtime stop errors do not short-circuit the HTTP/broadcast drain
// because the listener must be closed even if in-flight fibers were abandoned.
func (s *Server) Shutdown(ctx context.Context) error {
	s.serverMu.Lock()
	httpServer := s.httpServer
	cancel := s.cancel
	s.httpServer = nil
	s.cancel = nil
	s.serverMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.closeClients()
	s.runtimeMu.Lock()
	rt := s.runtime
	s.runtimeMu.Unlock()
	var firstErr error
	if rt != nil {
		if err := rt.Stop(ctx); err != nil {
			firstErr = err
			s.logFor(httptestReq()).Warn("runtime stop reported error during shutdown", "error", err)
		}
	}
	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.broadcastWG.Wait()
	return firstErr
}

// httptestReq returns a synthetic request used only to attach a request ID
// to shutdown log lines. Shutdown is not request-scoped, but operators need a
// correlation tag.
func httptestReq() *http.Request { return httptest.NewRequest(http.MethodGet, "/shutdown", nil) }

type requestIDKey struct{}

// requestID middleware assigns (or accepts) a bounded X-Request-ID and stashes
// it in the request context so handlers can correlate structured log lines.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" || len(id) > 128 {
			id = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// securityHeaders adds defense-in-depth response headers. The visualization
// client builds DOM with textContent (no inline scripts), so a restrictive CSP
// is safe and shrinks the blast radius of any future DOM-sink regression.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// authorizedHandler wraps a handler with the same authorization check used by
// the WebSocket endpoint. When no token is configured (loopback dev mode) the
// handler is open. Health and readiness are intentionally left unwrapped.
func (s *Server) authorizedHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// logFor returns a logger enriched with the request ID from the request
// context, so handler log lines can be correlated to a failing client action.
func (s *Server) logFor(r *http.Request) *slog.Logger {
	if id, ok := r.Context().Value(requestIDKey{}).(string); ok && id != "" {
		return s.logger.With("request_id", id)
	}
	return s.logger
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	s.runtimeMu.RLock()
	ready := s.runtime != nil && s.runtime.IsRunning()
	s.runtimeMu.RUnlock()
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.runtimeMu.RLock()
	rt := s.runtime
	s.runtimeMu.RUnlock()
	// Prometheus text exposition format requires # HELP and # TYPE lines for
	// every metric. We emit them once per scrape so the exposition is
	// self-describing and type-safe.
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder
	writeMetric := func(name, help, typ string, value int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, typ, name, value)
	}
	writeFloat := func(name, help, typ string, value float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %g\n", name, help, name, typ, name, value)
	}
	running := int64(0)
	var m metrics.MetricsSnapshot
	if rt != nil {
		m = rt.GetMetrics()
		if rt.IsRunning() {
			running = 1
		}
	}
	writeMetric("greenthreads_runtime_running", "1 when the runtime is admitting work, 0 otherwise.", "gauge", running)
	writeMetric("greenthreads_fibers_created", "Total fibers created since the runtime started.", "counter", m.TotalFibersCreated)
	writeMetric("greenthreads_fibers_completed", "Total fibers that reached a terminal state.", "counter", m.TotalFibersCompleted)
	writeMetric("greenthreads_fibers_active", "Currently admitted fibers that have not completed.", "gauge", m.ActiveFibers)
	writeMetric("greenthreads_fibers_blocked", "Fibers currently in the Blocked state.", "gauge", m.BlockedFibers)
	writeMetric("greenthreads_context_switches", "Total scheduler dispatches.", "counter", m.TotalContextSwitches)
	writeMetric("greenthreads_fiber_yields", "Total cooperative yields recorded.", "counter", m.TotalYields)
	writeMetric("greenthreads_steal_attempts", "Work-stealing steal attempts (work-stealing scheduler only).", "counter", m.TotalStealAttempts)
	writeMetric("greenthreads_steal_successes", "Successful work-stealing steals.", "counter", m.TotalStealSuccesses)
	writeFloat("greenthreads_steal_success_rate", "Fraction of steal attempts that succeeded.", "gauge", m.StealSuccessRate)
	writeMetric("greenthreads_peak_fiber_count", "Peak active fiber count observed.", "gauge", m.PeakFiberCount)
	writeMetric("greenthreads_total_stack_memory_bytes", "Simulated stack memory currently retained, in bytes.", "gauge", m.TotalStackMemory)
	writeMetric("greenthreads_uptime_seconds", "Runtime uptime in seconds.", "gauge", int64(m.Uptime.Seconds()))
	_, _ = w.Write([]byte(b.String()))
}

var upgrader = websocket.Upgrader{
	EnableCompression: false,
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !s.allowedOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	// Reserve an admission slot atomically without holding clientsMu, so a
	// slow/hanging WebSocket handshake cannot block other connection admissions.
	if !s.reserveClient() {
		http.Error(w, "too many clients", http.StatusServiceUnavailable)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.releaseClient()
		s.logFor(r).Warn("websocket upgrade failed", "error", err)
		return
	}
	cl := newClient(conn)
	s.clientsMu.Lock()
	s.clients[conn] = cl
	s.clientsMu.Unlock()
	defer s.unregister(cl, conn)
	defer func() { _ = conn.Close() }()

	s.serverMu.Lock()
	ctx := s.ctx
	s.serverMu.Unlock()
	go s.writePump(cl, ctx)

	conn.SetReadLimit(s.config.MaxMessageBytes)
	readTimeout := s.config.ReadIdleTimeout
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})
	s.sendInitialState(conn)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if !s.allowMessage(conn) {
			s.sendError(conn, "message rate limit exceeded")
			continue
		}
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			s.sendError(conn, "invalid JSON message")
			continue
		}
		s.handleMessage(conn, msg)
	}
}

// Message is the wire envelope for the WebSocket control protocol.
type Message struct {
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

func (s *Server) handleMessage(conn *websocket.Conn, msg Message) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in handleMessage", "panic", r, "stack", string(debug.Stack()))
			s.sendError(conn, "invalid request")
		}
	}()
	switch msg.Type {
	case "init":
		s.handleInit(conn, msg)
	case "spawn":
		s.handleSpawn(conn, msg)
	case "stop":
		s.handleStop(conn, msg)
	case "reset":
		s.handleReset(conn, msg)
	case "getState":
		s.handleGetState(conn)
	default:
		s.sendError(conn, "unknown message type")
	}
}

func (s *Server) handleInit(conn *websocket.Conn, msg Message) {
	schedulerName, err := payloadString(msg.Payload, "schedulerType", 32)
	if err != nil {
		s.sendError(conn, "schedulerType is required")
		return
	}
	numWorkers, err := payloadInt(msg.Payload, "numWorkers", 1, 64)
	if err != nil {
		s.sendError(conn, "numWorkers must be an integer from 1 to 64")
		return
	}
	sType, ok := parseScheduler(schedulerName)
	if !ok {
		s.sendError(conn, "unsupported scheduler type")
		return
	}
	newRuntime := runtime.NewRuntime(sType, numWorkers)
	if err := newRuntime.Start(); err != nil {
		s.sendError(conn, "failed to start runtime")
		return
	}
	// Take a write lock on the runtime field for the entire swap. Without
	// this, concurrent handleSpawn calls captured the old runtime just
	// before the swap and continued to admit fibers into a runtime that
	// was about to be stopped (the fibers would be queued but never
	// dispatched; see handleInit M1 in the audit).
	s.runtimeMu.Lock()
	old := s.runtime
	s.runtime = newRuntime
	s.runtimeMu.Unlock()
	if old != nil {
		// Bound the old runtime's drain so a stuck fiber in the prior runtime
		// cannot freeze the re-init handler and this client's connection.
		stopCtx, stopCancel := s.stopContext()
		if err := old.Stop(stopCtx); err != nil {
			s.logFor(httptestReq()).Warn("old runtime did not drain during re-init", "error", err)
		}
		stopCancel()
	}
	s.sendJSON(conn, map[string]interface{}{
		"type":    "initSuccess",
		"payload": map[string]interface{}{"schedulerType": strings.ToLower(schedulerName), "numWorkers": numWorkers},
	})
}

func (s *Server) handleSpawn(conn *websocket.Conn, msg Message) {
	s.runtimeMu.RLock()
	rt := s.runtime
	s.runtimeMu.RUnlock()
	if rt == nil || !rt.IsRunning() {
		s.sendError(conn, "runtime not initialized")
		return
	}
	// Cap on concurrently-active fibers (created minus completed), not the
	// cumulative lifetime count: a long-lived runtime should be able to spawn
	// indefinitely as old fibers finish.
	if rt.GetMetrics().ActiveFibers >= s.config.MaxFibersPerRuntime {
		s.sendError(conn, "fiber limit reached")
		return
	}
	rawName, _ := msg.Payload["name"].(string)
	name := strings.TrimSpace(rawName)
	if name == "" {
		name = "Worker"
	} else if len(name) > 128 {
		s.sendError(conn, "name must be 1-128 characters")
		return
	}
	priority, err := payloadInt(msg.Payload, "priority", -100, 100)
	if err != nil {
		s.sendError(conn, "priority must be an integer from -100 to 100")
		return
	}
	duration, err := payloadInt(msg.Payload, "duration", 1, 60000)
	if err != nil {
		s.sendError(conn, "duration must be an integer from 1 to 60000 milliseconds")
		return
	}
	fiberID, err := rt.Spawn(func() { time.Sleep(time.Duration(duration) * time.Millisecond) }, name)
	if err != nil {
		s.sendError(conn, "failed to spawn fiber")
		return
	}
	// Apply priority through the scheduler-owned API so the priority heap is
	// re-heapified atomically. If the fiber has already been dispatched or
	// completed by the time we get here, the priority update is silently
	// dropped (the fiber ran at priority 0). We surface that to the operator
	// via the success payload so the client can decide whether to retry.
	priorityApplied := true
	if priority != 0 {
		if ps, ok := rt.GetScheduler().(*scheduler.PriorityScheduler); ok {
			if err := ps.UpdatePriority(fiberID, priority); err != nil {
				priorityApplied = false
				s.logger.Warn("priority update failed; fiber already dispatched", "fiberId", fiberID, "error", err)
			}
		}
	}
	s.sendJSON(conn, map[string]interface{}{
		"type": "success",
		"payload": map[string]interface{}{
			"fiberID":         fiberID,
			"name":            name,
			"priorityApplied": priorityApplied,
		},
	})
}

func (s *Server) handleStop(conn *websocket.Conn, _ Message) {
	s.runtimeMu.RLock()
	rt := s.runtime
	s.runtimeMu.RUnlock()
	if rt != nil {
		// Bound the drain so a stuck fiber cannot freeze this client's
		// connection. The control plane must remain responsive even when
		// user-supplied work hangs.
		stopCtx, stopCancel := s.stopContext()
		defer stopCancel()
		if err := rt.Stop(stopCtx); err != nil {
			s.sendError(conn, "failed to stop runtime")
			return
		}
	}
	s.sendSuccess(conn, nil)
}

func (s *Server) handleReset(conn *websocket.Conn, _ Message) {
	s.runtimeMu.RLock()
	rt := s.runtime
	s.runtimeMu.RUnlock()
	if rt != nil {
		stopCtx, stopCancel := s.stopContext()
		defer stopCancel()
		if err := rt.Stop(stopCtx); err != nil {
			s.sendError(conn, "failed to stop runtime")
			return
		}
		rt.Reset()
	}
	s.sendSuccess(conn, nil)
}

// stopContext returns a bounded context for runtime Stop operations invoked
// from control-plane handlers. It is shared so callers can override it in
// tests; production handlers use a 5 s timeout to keep the control plane
// responsive even when user-supplied fibers never return.
func (s *Server) stopContext() (context.Context, context.CancelFunc) {
	timeout := s.config.StopTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	s.serverMu.Lock()
	parent := s.ctx
	s.serverMu.Unlock()
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, timeout)
}

func (s *Server) handleGetState(conn *websocket.Conn) {
	s.runtimeMu.RLock()
	rt := s.runtime
	s.runtimeMu.RUnlock()
	if rt == nil {
		s.sendError(conn, "runtime not initialized")
		return
	}
	s.sendJSON(conn, map[string]interface{}{
		"type": "stateUpdate",
		"payload": convertRuntimeUpdate(&runtime.RuntimeUpdate{
			Fibers: rt.GetAllFibers(), Metrics: rt.GetMetrics(), Events: rt.GetEvents(100),
			SchedulerType: rt.GetScheduler().Type().String(), Timestamp: time.Now(),
		}),
	})
}

func (s *Server) sendInitialState(conn *websocket.Conn) {
	s.runtimeMu.RLock()
	rt := s.runtime
	s.runtimeMu.RUnlock()
	if rt == nil {
		return
	}
	s.sendJSON(conn, map[string]interface{}{
		"type": "stateUpdate",
		"payload": convertRuntimeUpdate(&runtime.RuntimeUpdate{
			Fibers: rt.GetAllFibers(), Metrics: rt.GetMetrics(), Events: rt.GetEvents(100),
			SchedulerType: rt.GetScheduler().Type().String(), Timestamp: time.Now(),
		}),
	})
}

func (s *Server) sendSuccess(conn *websocket.Conn, payload interface{}) {
	s.sendJSON(conn, map[string]interface{}{"type": "success", "payload": payload})
}

func (s *Server) sendError(conn *websocket.Conn, message string) {
	s.sendJSON(conn, map[string]interface{}{"type": "error", "payload": map[string]string{"message": message}})
}

// writePump is the single owner of conn writes for one client. It drains the
// bounded sendCh and emits periodic ping frames so an idle connection is not
// killed by the read deadline. It exits when sendCh closes or the server
// context is cancelled. A slow client only drops its own updates; it never
// blocks broadcasts to other clients.
func (s *Server) writePump(cl *client, ctx context.Context) {
	defer close(cl.done)
	pingInterval := s.config.PingInterval
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	pinger := time.NewTicker(pingInterval)
	defer pinger.Stop()
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case msg := <-cl.sendCh:
			_ = cl.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := cl.conn.WriteJSON(msg); err != nil {
				s.logger.Warn("websocket write failed", "error", err)
				return
			}
		case <-pinger.C:
			if err := cl.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				return
			}
		case <-cl.stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// enqueue delivers a message to a client's send queue without blocking. A
// full queue means the client is stuck; the message is dropped and logged so
// one slow client cannot stall the broadcast loop.
func (s *Server) enqueue(cl *client, value interface{}) {
	if cl == nil {
		return
	}
	select {
	case cl.sendCh <- value:
	default:
		s.logger.Warn("websocket send queue full; dropping message")
	}
}

func (s *Server) sendJSON(conn *websocket.Conn, value interface{}) {
	s.clientsMu.RLock()
	cl := s.clients[conn]
	s.clientsMu.RUnlock()
	s.enqueue(cl, value)
}

func (s *Server) broadcastLoop(ctx context.Context) {
	defer s.broadcastWG.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runtimeMu.RLock()
			rt := s.runtime
			s.runtimeMu.RUnlock()
			if rt == nil {
				continue
			}
			update := convertRuntimeUpdate(&runtime.RuntimeUpdate{
				Fibers: rt.GetAllFibers(), Metrics: rt.GetMetrics(), Events: rt.GetEvents(100),
				SchedulerType: rt.GetScheduler().Type().String(), Timestamp: time.Now(),
			})
			msg := map[string]interface{}{"type": "update", "payload": update}
			s.clientsMu.RLock()
			clients := make([]*client, 0, len(s.clients))
			for _, cl := range s.clients {
				clients = append(clients, cl)
			}
			s.clientsMu.RUnlock()
			for _, cl := range clients {
				s.enqueue(cl, msg)
			}
		}
	}
}

// reserveClient atomically claims an admission slot without holding clientsMu
// so the WebSocket handshake (a potentially slow network I/O) does not
// serialize new connections. Returns false if MaxClients is reached.
func (s *Server) reserveClient() bool {
	for {
		n := atomic.LoadInt64(&s.clientCount)
		if n >= int64(s.config.MaxClients) {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.clientCount, n, n+1) {
			return true
		}
	}
}

// releaseClient returns a reserved admission slot when registration does not
// complete (e.g. upgrade failure). Normal disconnection releases the slot in
// unregister.
func (s *Server) releaseClient() {
	atomic.AddInt64(&s.clientCount, -1)
}

func (s *Server) unregister(cl *client, conn *websocket.Conn) {
	s.clientsMu.Lock()
	if _, ok := s.clients[conn]; ok {
		delete(s.clients, conn)
		s.releaseClient()
	}
	s.clientsMu.Unlock()
	// Signal the writePump to exit. stop (via stopOnce) is closed exactly once
	// so concurrent closeClients/unregister calls are safe. sendCh is never
	// closed, so broadcastLoop's enqueue cannot panic on a disconnecting client.
	cl.shutdown()
	<-cl.done
}

func (s *Server) closeClients() {
	s.clientsMu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, cl := range s.clients {
		clients = append(clients, cl)
	}
	s.clients = make(map[*websocket.Conn]*client)
	s.clientsMu.Unlock()
	for _, cl := range clients {
		s.releaseClient()
		cl.shutdown()
		_ = cl.conn.Close()
		<-cl.done
	}
}

func (s *Server) allowMessage(conn *websocket.Conn) bool {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	cl := s.clients[conn]
	if cl == nil {
		return false
	}
	now := time.Now()
	if now.Sub(cl.windowStart) >= time.Second {
		cl.windowStart = now
		cl.messageCount = 0
	}
	if cl.messageCount >= s.config.MessagesPerSecond {
		return false
	}
	cl.messageCount++
	return true
}

func (s *Server) authorized(r *http.Request) bool {
	if s.config.AuthToken == "" {
		return true
	}
	// Prefer the Bearer header. The query-param fallback is gated behind
	// AllowTokenInQuery because ?token= leaks the secret into access logs,
	// browser history, and Referer headers; it is needed only for browser
	// WebSocket clients that cannot set headers.
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" && s.config.AllowTokenInQuery {
		token = r.URL.Query().Get("token")
	}
	if token == "" || len(token) != len(s.config.AuthToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.config.AuthToken)) == 1
}

func (s *Server) allowedOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	// An empty Origin is a non-browser client; browsers always send Origin
	// on WebSocket dials. The default config allows this so programmatic
	// clients (curl, custom HTTP libraries) work out of the box. Operators
	// that need to restrict non-browser clients can set a non-empty
	// AllowedOrigins list, in which case an empty Origin is rejected
	// because it matches no entry.
	if origin == "" {
		return len(s.config.AllowedOrigins) == 0
	}
	for _, allowed := range s.config.AllowedOrigins {
		if origin == allowed {
			return true
		}
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host != "" && strings.EqualFold(u.Host, r.Host) && (u.Scheme == "http" || u.Scheme == "https")
}

func payloadString(payload map[string]interface{}, key string, max int) (string, error) {
	value, ok := payload[key].(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	value = strings.TrimSpace(value)
	if value == "" || len(value) > max {
		return "", fmt.Errorf("%s has invalid length", key)
	}
	return value, nil
}

func payloadInt(payload map[string]interface{}, key string, min, max int) (int, error) {
	value, ok := payload[key].(float64)
	if !ok || value != float64(int(value)) || value < float64(min) || value > float64(max) {
		return 0, fmt.Errorf("%s must be an integer in range", key)
	}
	return int(value), nil
}

func parseScheduler(value string) (scheduler.SchedulerType, bool) {
	switch strings.ToLower(value) {
	case "fifo":
		return scheduler.TypeFIFO, true
	case "roundrobin":
		return scheduler.TypeRoundRobin, true
	case "priority":
		return scheduler.TypePriority, true
	case "workstealing":
		return scheduler.TypeWorkStealing, true
	default:
		return scheduler.TypeFIFO, false
	}
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// RuntimeUpdateJSON is the stable wire representation of a runtime snapshot.
type RuntimeUpdateJSON struct {
	Fibers        []FiberJSON `json:"fibers"`
	Metrics       MetricsJSON `json:"metrics"`
	Events        []EventJSON `json:"events"`
	SchedulerType string      `json:"schedulerType"`
	Timestamp     string      `json:"timestamp"`
}

// FiberJSON is the public visualization representation of one fiber.
type FiberJSON struct {
	ID            uint64 `json:"id"`
	Name          string `json:"name"`
	State         string `json:"state"`
	Priority      int    `json:"priority"`
	ScheduleCount int64  `json:"scheduleCount"`
	YieldCount    int64  `json:"yieldCount"`
	CPUTime       int64  `json:"cpuTime"`
	BlockReason   string `json:"blockReason"`
	Color         string `json:"color"`
}

// MetricsJSON is the public visualization representation of metrics.
type MetricsJSON struct {
	TotalFibersCreated   int64   `json:"totalFibersCreated"`
	TotalFibersCompleted int64   `json:"totalFibersCompleted"`
	ActiveFibers         int64   `json:"activeFibers"`
	BlockedFibers        int64   `json:"blockedFibers"`
	TotalContextSwitches int64   `json:"totalContextSwitches"`
	TotalYields          int64   `json:"totalYields"`
	StealSuccessRate     float64 `json:"stealSuccessRate"`
}

// EventJSON is the public visualization representation of one event.
type EventJSON struct {
	FiberID   uint64 `json:"fiberId"`
	EventType string `json:"eventType"`
	Timestamp string `json:"timestamp"`
	Details   string `json:"details"`
}

// convertRuntimeUpdate converts runtime snapshots without leaking internal
// types or pointers into the WebSocket protocol.
func convertRuntimeUpdate(update *runtime.RuntimeUpdate) RuntimeUpdateJSON {
	if update == nil {
		return RuntimeUpdateJSON{}
	}
	fibers := make([]FiberJSON, len(update.Fibers))
	for i, f := range update.Fibers {
		fibers[i] = FiberJSON{ID: uint64(f.ID), Name: f.Name, State: f.State.String(), Priority: f.Priority, ScheduleCount: f.ScheduleCount, YieldCount: f.YieldCount, CPUTime: f.CPUTime.Milliseconds(), BlockReason: f.BlockReason, Color: f.Color}
	}
	events := make([]EventJSON, len(update.Events))
	for i, e := range update.Events {
		events[i] = EventJSON{FiberID: uint64(e.FiberID), EventType: e.EventType.String(), Timestamp: e.Timestamp.Format(time.RFC3339Nano), Details: e.Details}
	}
	return RuntimeUpdateJSON{
		Fibers:  fibers,
		Metrics: MetricsJSON{TotalFibersCreated: update.Metrics.TotalFibersCreated, TotalFibersCompleted: update.Metrics.TotalFibersCompleted, ActiveFibers: update.Metrics.ActiveFibers, BlockedFibers: update.Metrics.BlockedFibers, TotalContextSwitches: update.Metrics.TotalContextSwitches, TotalYields: update.Metrics.TotalYields, StealSuccessRate: update.Metrics.StealSuccessRate},
		Events:  events, SchedulerType: update.SchedulerType, Timestamp: update.Timestamp.Format(time.RFC3339Nano),
	}
}
