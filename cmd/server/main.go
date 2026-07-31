package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // #nosec G108 -- pprof only activated when -pprof-addr flag is set
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
	"github.com/sanskarpan/greenthreads/internal/tracing"
	"github.com/sanskarpan/greenthreads/web"
)

// Build metadata. version is injected at release build time via
//
//	-ldflags "-X main.version=v1.2.3"
//
// commit and date fall back to the values the Go toolchain embeds from VCS
// (available when built from a git checkout), so an unstamped `go build` still
// self-reports something useful.
var (
	version = "dev"
	commit  = ""
	date    = ""
)

// buildInfo returns a single-line version string, filling commit/date from the
// embedded build info when they were not injected via ldflags.
func buildInfo() string {
	c, d := commit, date
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if c == "" {
					c = s.Value
				}
			case "vcs.time":
				if d == "" {
					d = s.Value
				}
			}
		}
	}
	if len(c) > 12 {
		c = c[:12]
	}
	out := "greenthreads " + version
	if c != "" {
		out += " (" + c
		if d != "" {
			out += ", " + d
		}
		out += ")"
	}
	return out
}

func main() {
	port := flag.String("port", "8080", "TCP port used when -listen is not set")
	listen := flag.String("listen", "", "listen address; defaults to 127.0.0.1:<port>")
	pprofAddr := flag.String("pprof-addr", "", "address to serve pprof on (e.g. localhost:6060); empty disables pprof")
	tlsCert := flag.String("tls-cert", "", "path to TLS certificate PEM file (enables TLS when set with -tls-key)")
	tlsKey := flag.String("tls-key", "", "path to TLS private key PEM file (enables TLS when set with -tls-cert)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(buildInfo())
		return
	}

	addr := *listen
	if addr == "" {
		parsed, err := strconv.Atoi(*port)
		if err != nil || parsed < 1 || parsed > 65535 {
			slog.Error("invalid port", "port", *port)
			os.Exit(2)
		}
		addr = fmt.Sprintf("127.0.0.1:%d", parsed)
	}
	if !web.IsLoopbackAddress(addr) && os.Getenv("GREENTHREADS_AUTH_TOKEN") == "" {
		slog.Error("GREENTHREADS_AUTH_TOKEN is required when listening beyond loopback", "addr", addr)
		os.Exit(2)
	}

	logLevel := slog.LevelInfo
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "WARN", "WARNING":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	}
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	logger.Info("starting greenthreads", "version", version, "build", buildInfo())

	// Opt-in OpenTelemetry tracing (enabled only when OTEL_EXPORTER_OTLP_ENDPOINT
	// is set). Safe to shut down unconditionally.
	shutdownTracing, err := tracing.Setup(context.Background(), "greenthreads-server", version)
	if err != nil {
		logger.Error("failed to initialize tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()
	if tracing.Enabled() {
		logger.Info("OpenTelemetry tracing enabled")
	}

	if *pprofAddr != "" {
		go func() {
			logger.Info("starting pprof server", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil { // #nosec G114 -- pprof debug endpoint; read-only, not exposed in production
				logger.Error("pprof server failed", "error", err)
			}
		}()
	}

	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
	cfg := web.DefaultConfig()
	cfg.TLSCertFile = *tlsCert
	cfg.TLSKeyFile = *tlsKey
	server := web.NewServerWithConfig(rt, cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(addr) }()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		signal.Stop(stop)
		if err != nil {
			logger.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	case sig := <-stop:
		signal.Stop(stop)
		logger.Info("shutdown requested", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := server.Shutdown(ctx); err != nil {
			cancel()
			logger.Error("graceful shutdown failed", "error", err)
			os.Exit(1)
		}
		cancel()
		<-errCh
	}
}
