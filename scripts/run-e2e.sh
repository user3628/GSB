#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PORT="${PORT:-18087}"
DB="data/e2e-verify-$$.db"
mkdir -p bin data/logs samples
go build -o bin/meshpolicy ./cmd/meshpolicy
./bin/meshpolicy samples -out samples >/dev/null
./bin/meshpolicy serve -addr "127.0.0.1:$PORT" -db "$DB" >data/logs/e2e.out.log 2>data/logs/e2e.err.log &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
./bin/meshpolicy verify -base-url "http://127.0.0.1:$PORT" -samples samples
