#!/usr/bin/env bash
set -euo pipefail

# The threshold is intentionally explicit and overridable in CI. It prevents
# an accidental order-of-magnitude regression without pretending nanosecond
# measurements are identical across runners.
: "${FIFO_MAX_NS_PER_OP:=500000}"
output="$(go test -run='^$' -bench='BenchmarkFIFOScheduler$' -benchtime=100ms ./internal/scheduler)"
ns_per_op="$(awk '/BenchmarkFIFOScheduler/ {for (i = 1; i <= NF; i++) if ($i ~ /^[0-9.]+$/ && $(i+1) == "ns/op") {print $i; exit}}' <<<"$output")"
if [[ -z "$ns_per_op" ]]; then
  printf '%s\n' "benchmark result did not contain ns/op" >&2
  exit 1
fi
awk -v actual="$ns_per_op" -v maximum="$FIFO_MAX_NS_PER_OP" 'BEGIN { if (actual > maximum) { printf "FIFO benchmark %.0f ns/op exceeds %.0f ns/op\n", actual, maximum; exit 1 } }'
