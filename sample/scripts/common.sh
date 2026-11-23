#!/usr/bin/env bash
# Shared helpers for deterministic local demo scripts. Loopback HTTP only.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:18090}"
DB_PATH="${DB_PATH:-data/arbiter-demo.db}"
BIN="${BIN:-./bin/arbiter}"

log() { printf '\n=== %s ===\n' "$*"; }
post() {
  local path="$1"; local body="$2"
  curl -sS -X POST "${BASE_URL}${path}" \
    -H 'Content-Type: application/json' -d "${body}"
}
get() { curl -sS "${BASE_URL}$1"; }
code_of() { python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("error",{}).get("code","OK"))' <<<"$1"; }
start_server() {
  rm -f "$DB_PATH" "$DB_PATH-wal" "$DB_PATH-shm"
  "$BIN" -addr "${BASE_URL#http://}" -db "$DB_PATH" \
    -seed sample/seed/rows.json >"${DB_PATH}.log" 2>&1 &
  echo $!
}
wait_health() {
  for _ in $(seq 1 50); do
    if curl -sS "${BASE_URL}/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "server did not become healthy" >&2; exit 1
}
