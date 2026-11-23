#!/usr/bin/env bash
# Runs every deterministic local demo. No network services, no containers.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
cd "$ROOT"
mkdir -p bin data
go build -o bin/arbiter ./cmd/arbiter

run() {
  local name="$1"; local port="$2"; local script="$3"
  printf '\n######## %s (port %s) ########\n' "$name" "$port"
  BASE_URL="http://127.0.0.1:${port}" DB_PATH="data/demo-${name}.db" "$script"
}

run basic 18090 "${HERE}/basic-flow.sh"
run concurrent 18091 "${HERE}/concurrent-edits.sh"
run matrix 18092 "${HERE}/snapshot-matrix.sh"
run precheck 18093 "${HERE}/precheck-reconcile.sh"
run crash 18094 "${HERE}/crash-recovery.sh"
printf '\nALL_DEMOS_OK\n'
