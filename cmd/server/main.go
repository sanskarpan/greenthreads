package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	// pprof endpoints are an intentional production observability feature.
	// Access to /debug/pprof must be restricted via network policy in production.
	_ "net/http/pprof" //nolint:gosec // G108: profiling is intentional for production diagnostics
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sanskar/greenthreads/internal/runtime"
	"github.com/sanskar/greenthreads/internal/scheduler"
	"github.com/sanskar/greenthreads/web"
)

func main() {
	port := flag.String("port", "8080", "TCP port used when -listen is not set")
	listen := flag.String("listen", "", "listen address; defaults to 127.0.0.1:<port>")
	flag.Parse()

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

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	rt := runtime.NewRuntime(scheduler.TypeFIFO, 4)
	server := web.NewServer(rt)
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
