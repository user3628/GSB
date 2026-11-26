#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
"$ROOT/scripts/build.sh"

AUTH_PORT="${AUTH_PORT:-25354}"
DNS_PORT="${DNS_PORT:-25353}"
ADMIN_PORT="${ADMIN_PORT:-28080}"
DB_PATH="${DB_PATH:-$ROOT/data/dnscache.db}"
mkdir -p "$(dirname "$DB_PATH")"

cleanup() {
  if [[ -n "${CACHE_PID:-}" ]]; then kill "$CACHE_PID" 2>/dev/null || true; fi
  if [[ -n "${AUTH_PID:-}" ]]; then kill "$AUTH_PID" 2>/dev/null || true; fi
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

"$ROOT/bin/authserver" \
  -zone "$ROOT/samples/zone-v1.json" \
  -udp "127.0.0.1:$AUTH_PORT" -tcp "127.0.0.1:$AUTH_PORT" \
  >"$ROOT/authserver.log" 2>&1 &
AUTH_PID=$!

"$ROOT/bin/dnscache" \
  -udp "127.0.0.1:$DNS_PORT" -tcp "127.0.0.1:$DNS_PORT" \
  -admin "127.0.0.1:$ADMIN_PORT" -db "$DB_PATH" \
  -upstream "127.0.0.1:$AUTH_PORT" -snapshot "$ROOT/samples/zone-v1.json" \
  >"$ROOT/dnscache.log" 2>&1 &
CACHE_PID=$!

for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$ADMIN_PORT/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.05
done

cat <<MSG
DNS UDP/TCP: 127.0.0.1:$DNS_PORT
Admin API:   http://127.0.0.1:$ADMIN_PORT
SQLite:      $DB_PATH
Probe:       $ROOT/bin/dnsprobe -server 127.0.0.1:$DNS_PORT -name www.example.test -type A
Logs:        $ROOT/dnscache.log $ROOT/authserver.log
Press Ctrl-C to stop.
MSG
wait "$CACHE_PID"
