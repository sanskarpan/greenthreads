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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sanskarpan/greenthreads/internal/runtime"
	"github.com/sanskarpan/greenthreads/internal/scheduler"
	"github.com/sanskarpan/greenthreads/web"
)

func main() {
	port := flag.String("port", "8080", "TCP port used when -listen is not set")
	listen := flag.String("listen", "", "listen address; defaults to 127.0.0.1:<port>")
	pprofAddr := flag.String("pprof-addr", "", "address to serve pprof on (e.g. localhost:6060); empty disables pprof")
	tlsCert := flag.String("tls-cert", "", "path to TLS certificate PEM file (enables TLS when set with -tls-key)")
	tlsKey := flag.String("tls-key", "", "path to TLS private key PEM file (enables TLS when set with -tls-cert)")
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
