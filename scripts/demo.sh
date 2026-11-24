#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
run_dir="${1:-var/demo-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$run_dir/samples"
go run ./cmd/walgroup generate -seed 42 -out "$run_dir/samples/workload-42.json"
go run ./cmd/walgroup demo \
  -workload "$run_dir/samples/workload-42.json" \
  -data "$run_dir/engine" \
  -clean \
  -report "$run_dir/demo-report.json" > "$run_dir/demo-output.json"
go run ./cmd/walgroup crash-demo \
  -data "$run_dir/crash" \
  -report "$run_dir/crash-report.json" > "$run_dir/crash-output.json"
printf 'demo output: %s\ncrash output: %s\n' "$run_dir/demo-output.json" "$run_dir/crash-output.json"
