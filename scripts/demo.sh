#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d /tmp/elastic-scale-demo-XXXXXX)"
mkdir -p artifacts
echo "[demo] work directory: $WORK"

cleanup() {
  for pid in "${SERVER_PID2:-}" "${SERVER_PID1:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

go build -o "$WORK/platform" ./cmd/platform
"$WORK/platform" generate --out artifacts/seed.json
"$WORK/platform" smoke --db artifacts/smoke.db

start_server() {
  local port_file="$1" db_path="$2" log_path="$3" pid_var="$4"
  "$WORK/platform" serve --addr 127.0.0.1:0 --db "$db_path" --port-file "$port_file" >"$log_path" 2>&1 &
  local pid=$!
  printf -v "$pid_var" '%s' "$pid"
  for _ in $(seq 1 100); do
    [[ -s "$port_file" ]] && return 0
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server exited; log:" >&2
      cat "$log_path" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for port file $port_file" >&2
  cat "$log_path" >&2
  return 1
}

start_server "$WORK/port1" "$WORK/platform.db" "$WORK/server1.log" SERVER_PID1
PORT1="$(cat "$WORK/port1")"
BASE1="http://127.0.0.1:$PORT1"
echo "[demo] first server: $BASE1 pid=$SERVER_PID1"

"$WORK/platform" demo --base-url "$BASE1" --seed artifacts/seed.json | tee "$WORK/demo-client.log"

"$WORK/platform" arm-crash --base-url "$BASE1" --seed artifacts/seed.json | tee "$WORK/crash-arm.log"
sleep 0.2
echo "[demo] kill -9 $SERVER_PID1"
kill -9 "$SERVER_PID1"
wait "$SERVER_PID1" 2>/dev/null || true
SERVER_PID1=""

if command -v sqlite3 >/dev/null 2>&1; then
  echo "[demo] on-disk intents immediately after SIGKILL:"
  sqlite3 "$WORK/platform.db" "select commit_id || ' ' || status from intents where status='pending';" || true
fi

start_server "$WORK/port2" "$WORK/platform.db" "$WORK/server2.log" SERVER_PID2
PORT2="$(cat "$WORK/port2")"
BASE2="http://127.0.0.1:$PORT2"
echo "[demo] recovered server: $BASE2 pid=$SERVER_PID2"

for _ in $(seq 1 100); do
  if curl -fsS "$BASE2/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

curl -fsS "$BASE2/v1/recovery" | tee "$WORK/recovery.json"
grep -q '"found_pending_intents":1' "$WORK/recovery.json"
grep -q 'inc-crash-global' "$WORK/recovery.json"
curl -fsS "$BASE2/healthz" | tee "$WORK/health.json"
grep -q '"current_version":3' "$WORK/health.json"
curl -fsS "$BASE2/v1/metrics/inventory-svc?observed_version=3" >"$WORK/metrics-v3.json"
curl -fsS "$BASE2/v1/rebuilds/latest" >"$WORK/rebuild-latest.json"

echo
echo "[demo] PASS: load, single-service incremental, global incremental, stale read, SIGKILL recovery"
echo "[demo] artifacts: $WORK"
