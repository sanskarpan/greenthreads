package main

import (
	"strings"
	"testing"
)

// TestBuildInfoIncludesVersion checks that the version string is well-formed and
// carries the injected version, and that an over-long commit is truncated.
func TestBuildInfoIncludesVersion(t *testing.T) {
	origV, origC, origD := version, commit, date
	t.Cleanup(func() { version, commit, date = origV, origC, origD })

	version = "v1.2.3"
	commit = "0123456789abcdefghij" // 20 chars, must truncate to 12
	date = "2026-07-31"

	got := buildInfo()
	if !strings.HasPrefix(got, "greenthreads v1.2.3") {
		t.Fatalf("buildInfo() = %q, want prefix %q", got, "greenthreads v1.2.3")
	}
	if !strings.Contains(got, "0123456789ab") {
		t.Fatalf("buildInfo() = %q, want truncated commit %q", got, "0123456789ab")
	}
	if strings.Contains(got, "0123456789abc") {
		t.Fatalf("buildInfo() = %q did not truncate commit to 12 chars", got)
	}
	if !strings.Contains(got, "2026-07-31") {
		t.Fatalf("buildInfo() = %q, want date", got)
	}
}

// TestBuildInfoDefault confirms an unstamped build still returns a usable string.
func TestBuildInfoDefault(t *testing.T) {
	origV, origC, origD := version, commit, date
	t.Cleanup(func() { version, commit, date = origV, origC, origD })

	version, commit, date = "dev", "", ""
	got := buildInfo()
	if !strings.HasPrefix(got, "greenthreads dev") {
		t.Fatalf("buildInfo() = %q, want prefix %q", got, "greenthreads dev")
	}
}
