#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d)"
PORT="${PORT:-18081}"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "== Go version =="
go version
echo "== Vet and tests =="
go vet ./...
go test ./... -count=1
go test -race ./internal/domain ./internal/format ./internal/engine ./internal/httpapi ./internal/integration -count=1

echo "== Windows amd64 cross-build (CGO disabled) =="
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o "$WORK/lagengine-windows-amd64.exe" ./cmd/lagengine
ls -lh "$WORK/lagengine-windows-amd64.exe"

echo "== Deterministic samples =="
go run ./cmd/samplegen -out ./samples
go build -trimpath -o "$WORK/lagengine" ./cmd/lagengine

echo "== JSONL import =="
"$WORK/lagengine" -db "$WORK/verify.db" -compat ./samples/compat.json \
  -import ./samples/observations.jsonl -format jsonl -batch verify-seed > "$WORK/import.json"
grep -q '"observation_count":13' "$WORK/import.json"
grep -q '"version":1' "$WORK/import.json"
grep -q '"current_state":"HEALTHY"' "$WORK/import.json"
echo "imported 13 records at version 1; both replicas ended HEALTHY"

echo "== Legacy compatibility degrade =="
"$WORK/lagengine" -db "$WORK/verify.db" -compat ./samples/compat.json \
  -import ./samples/legacy.jsonl -format jsonl -batch verify-legacy > "$WORK/legacy.json"
grep -q '"compatibility":"DEGRADE"' "$WORK/legacy.json"
grep -q 'missing reported version filled from compatibility default' "$WORK/legacy.json"
echo "legacy v0 accepted with recorded degradations"

echo "== Forced termination after durable intent =="
set +e
LAGENGINE_CRASH_AFTER_INTENT=1 "$WORK/lagengine" -db "$WORK/verify.db" -compat ./samples/compat.json \
  -import ./samples/legacy.jsonl -format jsonl -batch verify-crashed > "$WORK/crash.log" 2>&1
CRASH_STATUS=$?
set -e
if [[ "$CRASH_STATUS" -ne 137 ]]; then
  echo "expected child exit 137, got $CRASH_STATUS" >&2
  cat "$WORK/crash.log" >&2
  exit 1
fi
echo "process exited 137 after intent write"

"$WORK/lagengine" -db "$WORK/verify.db" -compat ./samples/compat.json \
  -addr "127.0.0.1:$PORT" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
READY=0
for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$PORT/healthz" > "$WORK/health.json" 2>/dev/null; then
    READY=1
    break
  fi
  sleep 0.1
done
if [[ "$READY" -ne 1 ]]; then
  echo "HTTP server did not become ready" >&2
  cat "$WORK/server.log" >&2
  exit 1
fi

curl -fsS "http://127.0.0.1:$PORT/v1/recovery/report" > "$WORK/recovery.json"
curl -fsS "http://127.0.0.1:$PORT/v1/replicas/replica-a/sequence" > "$WORK/sequence.json"
curl -fsS "http://127.0.0.1:$PORT/v1/replicas/replica-a/state" > "$WORK/state.json"
curl -fsS "http://127.0.0.1:$PORT/v1/export?format=binary" -o "$WORK/export.bin" -D "$WORK/export.headers"
grep -q '"pending_intents_found":1' "$WORK/recovery.json"
grep -q '"action":"ABORT_PENDING_INTENT"' "$WORK/recovery.json"
grep -q '"last_complete_version":2' "$WORK/recovery.json"
grep -q '"state":"HEALTHY"' "$WORK/state.json"
grep -qi 'X-Engine-Version: 2' "$WORK/export.headers"
echo "recovery aborted pending batch at version 2; loopback queries and binary export passed ($(wc -c < "$WORK/export.bin") bytes)"

echo "PASS: all verification steps completed"

