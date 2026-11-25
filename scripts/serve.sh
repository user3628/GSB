#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export GOPROXY=off GOSUMDB=off
ADDR="${ADDR:-127.0.0.1:18761}"
mkdir -p bin
go build -o bin/recallab ./cmd/recallab
printf 'Health: http://%s/healthz\nRun endpoint: POST http://%s/v1/runs\n' "$ADDR" "$ADDR"
exec ./bin/recallab serve --addr "$ADDR"
