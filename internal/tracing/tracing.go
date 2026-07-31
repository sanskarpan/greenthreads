// Package tracing provides opt-in OpenTelemetry tracing setup.
//
// Tracing is enabled only when an OTLP endpoint is configured via the standard
// OTEL_EXPORTER_OTLP_ENDPOINT (or ..._TRACES_ENDPOINT) environment variable.
// When unset, Setup installs a no-op tracer provider, so instrumentation calls
// throughout the codebase are effectively free and the process makes no network
// connections. This keeps distributed tracing strictly opt-in.
package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// ScopeName is the instrumentation scope used for manually-created spans.
const ScopeName = "github.com/sanskarpan/greenthreads"

// ShutdownFunc flushes and stops the tracer provider. It is always safe to call
// (a no-op when tracing is disabled).
type ShutdownFunc func(context.Context) error

// Enabled reports whether an OTLP traces endpoint is configured.
func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Setup configures the global tracer provider and W3C context propagator.
// It returns a shutdown function that flushes buffered spans. When no OTLP
// endpoint is configured it installs a no-op provider and returns a no-op
// shutdown, so callers can always `defer shutdown(ctx)` unconditionally.
func Setup(ctx context.Context, serviceName, serviceVersion string) (ShutdownFunc, error) {
	// Always install the W3C propagator so trace context is honored on
	// incoming requests even before an exporter is wired up.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if !Enabled() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	// Endpoint, headers, protocol, etc. are read from the standard OTEL_* env
	// vars by the exporter itself.
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}

	res := resource.NewSchemaless(
		attribute.String("service.name", serviceName),
		attribute.String("service.version", serviceVersion),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the greenthreads instrumentation tracer from the global
// provider (no-op unless Setup enabled tracing).
func Tracer() trace.Tracer {
	return otel.Tracer(ScopeName)
}
