package tracing

import (
	"context"
	"testing"
)

// TestTracingDisabledByDefault verifies that with no OTLP endpoint configured,
// Setup installs a no-op provider, returns a safe shutdown, and the tracer is
// usable without panicking or making network connections.
func TestTracingDisabledByDefault(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	if Enabled() {
		t.Fatal("Enabled() = true with no endpoint configured")
	}

	shutdown, err := Setup(context.Background(), "greenthreads-test", "v0.0.0")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if shutdown == nil {
		t.Fatal("Setup returned a nil shutdown function")
	}

	// The no-op tracer must be usable without error.
	_, span := Tracer().Start(context.Background(), "unit-test-span")
	span.End()

	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestTracingEnabledReflectsEnv verifies the opt-in gate keys off the standard
// OTLP endpoint environment variables.
func TestTracingEnabledReflectsEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	if !Enabled() {
		t.Fatal("Enabled() = false when OTEL_EXPORTER_OTLP_ENDPOINT is set")
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318/v1/traces")
	if !Enabled() {
		t.Fatal("Enabled() = false when OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is set")
	}
}
