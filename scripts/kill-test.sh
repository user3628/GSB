#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PORT="${PORT:-18086}"
DB="data/kill-test-$$.db"
MARKER="$DB.marker"
LOG_DIR="data/logs"
mkdir -p "$LOG_DIR" bin data
go build -o bin/meshpolicy ./cmd/meshpolicy

cleanup() {
  set +e
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null
  [[ -n "${CURL_PID:-}" ]] && kill "$CURL_PID" 2>/dev/null
}
trap cleanup EXIT

wait_health() {
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.25
  done
  echo "server did not become healthy" >&2
  return 1
}

start_server() {
  MESHPOLICY_ENABLE_TEST_HOOKS=1 MESHPOLICY_TEST_MARKER="$MARKER" MESHPOLICY_TEST_PAUSE_SECONDS=120 \
    ./bin/meshpolicy serve -addr "127.0.0.1:$PORT" -db "$DB" \
    >"$LOG_DIR/kill-test.out.log" 2>"$LOG_DIR/kill-test.err.log" &
  SERVER_PID=$!
  wait_health
}

start_server
STATUS=$(curl -sS -o "$LOG_DIR/v1.body.json" -w '%{http_code}' -H 'X-Request-ID: kill-v1' \
  --data-binary @samples/v1-legacy.bin "http://127.0.0.1:$PORT/v1/import?truncation=floor")
[[ "$STATUS" == "201" ]] || { echo "v1 import failed: $STATUS"; cat "$LOG_DIR/v1.body.json"; exit 1; }

curl -sS -m 180 -o "$LOG_DIR/v2-paused.body.json" -H 'X-Request-ID: kill-v2' \
  -H 'X-Test-Pause-Mid-Rebuild: 1' --data-binary @samples/v2-current.jsonl \
  "http://127.0.0.1:$PORT/v1/import?truncation=floor" >/dev/null 2>&1 &
CURL_PID=$!

for _ in $(seq 1 120); do
  [[ -f "$MARKER" ]] && break
  sleep 0.25
done
[[ -f "$MARKER" ]] || { echo "mid-rebuild marker not written; paused response follows:" >&2; cat "$LOG_DIR/v2-paused.body.json" >&2 || true; exit 1; }
echo "mid-rebuild reached; kill -9 server pid $SERVER_PID"
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
kill "$CURL_PID" 2>/dev/null || true
CURL_PID=""

start_server
curl -sS "http://127.0.0.1:$PORT/v1/versions" > "$LOG_DIR/versions.json"
curl -sS "http://127.0.0.1:$PORT/v1/recovery-report" > "$LOG_DIR/recovery.json"
curl -sS "http://127.0.0.1:$PORT/v1/derived" > "$LOG_DIR/derived.json"
REPLAY_STATUS=$(curl -sS -o "$LOG_DIR/replay.json" -w '%{http_code}' -H 'X-Request-ID: kill-v2' \
  --data-binary @samples/v2-current.jsonl "http://127.0.0.1:$PORT/v1/import?truncation=floor")

python3 - "$LOG_DIR/versions.json" "$LOG_DIR/recovery.json" "$LOG_DIR/derived.json" "$LOG_DIR/replay.json" "$REPLAY_STATUS" <<'PY'
import json, sys
versions_path, recovery_path, derived_path, replay_path, replay_status = sys.argv[1:]
versions = json.load(open(versions_path, encoding="utf-8"))
recovery = json.load(open(recovery_path, encoding="utf-8"))
derived = json.load(open(derived_path, encoding="utf-8"))
replay = json.load(open(replay_path, encoding="utf-8"))
assert versions["activeVersion"] == 1, versions
assert len(versions["versions"]) == 1, versions
assert recovery["activeSnapshotVersion"] == 1, recovery
assert recovery["activeSnapshotIntegrity"]["complete"] is True, recovery
assert recovery["pendingIntentCount"] >= 1, recovery
assert recovery["factsSource"] == "sqlite_committed_rows_only", recovery
assert len(recovery["abortedBatches"]) >= 1, recovery
assert derived["snapshotVersion"] == 1, derived
assert replay_status == "503", replay_status
assert replay["error"]["code"] == "E_IMPORT_ABORTED", replay
print("PASS forced-termination recovery")
print("activeVersion=1, versions=1, abortedBatch=1, derivedVersion=1, replay=E_IMPORT_ABORTED")
print(json.dumps(recovery, indent=2, ensure_ascii=False))
PY
