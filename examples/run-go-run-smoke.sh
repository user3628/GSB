#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PORT="${RELEASEGATE_GO_RUN_PORT:-18182}"
BASE="http://127.0.0.1:${PORT}"
TMP="$(mktemp -d)"
PID=''

cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    if command -v setsid >/dev/null 2>&1; then
      kill -- "-$PID" 2>/dev/null || true
    else
      kill "$PID" 2>/dev/null || true
    fi
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

go run ./cmd/fixturegen -out "$TMP/testdata" -seed 20261124 >/dev/null

if command -v setsid >/dev/null 2>&1; then
  setsid go run ./cmd/releasegate -listen "127.0.0.1:${PORT}" -db "$TMP/smoke.db" >"$TMP/server.log" 2>&1 &
else
  go run ./cmd/releasegate -listen "127.0.0.1:${PORT}" -db "$TMP/smoke.db" >"$TMP/server.log" 2>&1 &
fi
PID=$!

for _ in $(seq 1 120); do
  if curl -fsS "$BASE/healthz" 2>/dev/null | grep -q '"service":"releasegate"'; then
    break
  fi
  sleep 0.05
done
curl -fsS "$BASE/healthz" | grep -q '"service":"releasegate"'

curl -fsS -X POST -H 'Content-Type: application/json' --data-binary "@$TMP/testdata/release.json" "$BASE/v1/releases" >/dev/null
curl -fsS -X POST "$BASE/v1/releases/demo-progressive-release/start" >/dev/null
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary "@$TMP/testdata/observations-stage-1.json" \
  "$BASE/v1/releases/demo-progressive-release/observations" >/dev/null
STATS=$(curl -fsS "$BASE/v1/releases/demo-progressive-release/stats?version=1&scope=stage&stage=1")
GATE=$(curl -fsS -X POST -H 'Content-Type: application/json' --data '{"stats_version":1}' "$BASE/v1/releases/demo-progressive-release/gate")
RECOVERY=$(curl -fsS "$BASE/v1/releases/demo-progressive-release/recovery-report")

grep -q '"abs_drop_upper_ppm":38357' <<<"$STATS"
grep -q '"outcome":"PROMOTE"' <<<"$GATE"
grep -q '"scan_completed_after_restart":true' <<<"$RECOVERY"
echo 'go-run smoke: stats=38357 ppm gate=PROMOTE recovery-report=OK'
