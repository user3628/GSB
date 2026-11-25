#!/usr/bin/env bash
# Independent-process launcher for force-kill recovery verification.
# Usage: scripts/run_server.sh [workdir] [addr]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="${1:-$ROOT/workspace}"
ADDR="${2:-127.0.0.1:8787}"
mkdir -p "$WORKDIR"
cd "$ROOT"
exec go run ./cmd/voxelrecon -workdir "$WORKDIR" -addr "$ADDR"
