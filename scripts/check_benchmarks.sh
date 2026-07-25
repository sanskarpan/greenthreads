#!/usr/bin/env bash
set -euo pipefail

# Per-scheduler regression thresholds (ns/op, overridable via env).
# Baselines measured on darwin/arm64 go1.26.5; set ~5x baseline for CI headroom.
: "${FIFO_MAX_NS_PER_OP:=2000000}"
: "${ROUNDROBIN_MAX_NS_PER_OP:=1500000}"
: "${PRIORITY_MAX_NS_PER_OP:=8000000}"
: "${WORKSTEALING_MAX_NS_PER_OP:=500000}"

output="$(go test -run='^$' -bench='Benchmark(FIFO|RoundRobin|Priority|WorkStealing)Scheduler$' -benchtime=100ms ./internal/scheduler)"

status=0
check() {
	local name="$1"  label="$2"  max="$3"
	local actual
	actual="$(awk -v label="$label" '/^Benchmark'"$label"'/ {for (i=1;i<=NF;i++) if ($i ~ /^[0-9.]+$/ && $(i+1)=="ns/op") {print $i; exit}}' <<<"$output")"
	if [[ -z "$actual" ]]; then
		printf 'benchmark %s did not produce ns/op\n' "$name" >&2
		status=1
		return
	fi
	awk -v actual="$actual" -v maximum="$max" -v name="$name" \
		'BEGIN { if (actual > maximum) { printf "%s benchmark %.0f ns/op exceeds %.0f ns/op\n", name, actual, maximum; exit 1 } }' || status=1
}

check "FIFO"        "FIFOScheduler"        "$FIFO_MAX_NS_PER_OP"
check "RoundRobin"  "RoundRobinScheduler"  "$ROUNDROBIN_MAX_NS_PER_OP"
check "Priority"    "PriorityScheduler"    "$PRIORITY_MAX_NS_PER_OP"
check "WorkStealing" "WorkStealingScheduler" "$WORKSTEALING_MAX_NS_PER_OP"

exit "$status"
