#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-18190}"
DEMO_DIR="${DEMO_DIR:-$ROOT/var/demo}"
BASE="http://127.0.0.1:${PORT}"
BIN="$ROOT/.tmp/routerd"
CONFIG="$DEMO_DIR/config.json"
SERVER_LOG="$DEMO_DIR/server.log"
PID=""

if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  C_TITLE=$'\033[1;36m'; C_OK=$'\033[1;32m'; C_FAIL=$'\033[1;31m'; C_END=$'\033[0m'
else
  C_TITLE=""; C_OK=""; C_FAIL=""; C_END=""
fi
say() { printf '\n%s%s%s\n' "$C_TITLE" "$*" "$C_END"; }
ok() { printf '  %s✓%s %s\n' "$C_OK" "$C_END" "$*"; }
fail() { printf '  %s✗ %s%s\n' "$C_FAIL" "$*" "$C_END" >&2; exit 1; }

cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

http_post() {
  local path="$1" body="$2" out="$3"
  curl -sS --max-time 15 -o "$out" -w '%{http_code}' \
    -H 'Content-Type: application/json' --data-binary @"$body" "$BASE$path"
}

http_get() {
  local path="$1" out="$2"
  curl -sS --max-time 10 -o "$out" -w '%{http_code}' "$BASE$path"
}

wait_health() {
  local attempts=100
  for ((i=0; i<attempts; i++)); do
    if curl -fsS --max-time 1 "$BASE/api/health" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "$PID" ]] && ! kill -0 "$PID" 2>/dev/null; then
      echo "--- server log ---" >&2
      cat "$SERVER_LOG" >&2 || true
      fail "server exited before becoming healthy"
    fi
    sleep 0.1
  done
  fail "server did not become healthy at $BASE"
}

start_server() {
  "$BIN" serve -config "$CONFIG" >>"$SERVER_LOG" 2>&1 &
  PID=$!
  wait_health
}

json_code() {
  python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); print((p.get("error") or {}).get("code", ""))' "$1"
}

db_value() {
  python3 -c 'import sqlite3,sys; con=sqlite3.connect(sys.argv[1]); print(con.execute(sys.argv[2]).fetchone()[0])' "$DEMO_DIR/router.db" "$1"
}

json_value() {
  local file="$1" expression="$2"
  python3 - "$file" "$expression" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
value = data
for part in sys.argv[2].split('.'):
    if part.isdigit():
        value = value[int(part)]
    else:
        value = value[part]
print(value)
PY
}

assert_http() {
  local actual="$1" expected="$2" label="$3"
  [[ "$actual" == "$expected" ]] || fail "$label: expected HTTP $expected, got $actual"
}

assert_value() {
  local actual="$1" expected="$2" label="$3"
  [[ "$actual" == "$expected" ]] || fail "$label: expected '$expected', got '$actual'"
}

rm -rf "$DEMO_DIR"
mkdir -p "$DEMO_DIR/samples" "$ROOT/.tmp" "$ROOT/artifacts/validation"

say "0. Build binary and generate fixed-seed JSON samples"
(cd "$ROOT" && go build -o "$BIN" ./cmd/routerd)
"$BIN" samples -out "$DEMO_DIR/samples" >/dev/null
python3 - "$DEMO_DIR" "$PORT" <<'PY'
import json, os, sys
demo, port = sys.argv[1], int(sys.argv[2])
config = {
    "listen": f"127.0.0.1:{port}",
    "database": os.path.join(demo, "router.db"),
    "seed": 20261125,
}
json.dump(config, open(os.path.join(demo, "config.json"), "w"), indent=2)
snapshot = json.load(open(os.path.join(demo, "samples", "snapshot-v1.json")))
body = {"commit_id": "demo-snapshot-v1", "snapshot": snapshot, "seal_checksum": False}
json.dump(body, open(os.path.join(demo, "snapshot-commit.json"), "w"), indent=2)
PY
ok "binary=$BIN"
ok "samples=$DEMO_DIR/samples"
ok "config listen=127.0.0.1:$PORT database=$DEMO_DIR/router.db"

if curl -fsS --max-time 1 "$BASE/api/health" >/dev/null 2>&1; then
  fail "port $PORT already serves a healthy process; choose another PORT"
fi

say "1. Load full snapshot v1 over local HTTP"
start_server
ok "server pid=$PID health=ok"
status=$(http_post "/api/snapshots" "$DEMO_DIR/snapshot-commit.json" "$DEMO_DIR/commit-v1.json")
assert_http "$status" "200" "snapshot commit"
assert_value "$(json_value "$DEMO_DIR/commit-v1.json" 'data.version')" "1" "snapshot version"
ok "snapshot v1 committed; current=$(json_value "$DEMO_DIR/commit-v1.json" 'data.version')"

cat >"$DEMO_DIR/query-v1.json" <<'JSON'
{"table_id":"orders","observed_version":1,"kind":"eq","value":"1"}
JSON
status=$(http_post "/api/query" "$DEMO_DIR/query-v1.json" "$DEMO_DIR/route-v1-a.json")
assert_http "$status" "200" "v1 query"
status=$(http_post "/api/query" "$DEMO_DIR/query-v1.json" "$DEMO_DIR/route-v1-b.json")
assert_http "$status" "200" "v1 repeated query"
assert_value "$(json_value "$DEMO_DIR/route-v1-a.json" 'data.physical_tables.0')" "db0.orders_01" "v1 route"
python3 - "$DEMO_DIR/route-v1-a.json" "$DEMO_DIR/route-v1-b.json" <<'PY'
import json, sys
a = json.load(open(sys.argv[1]))["data"]
b = json.load(open(sys.argv[2]))["data"]
assert a["physical_tables"] == b["physical_tables"], (a, b)
assert a["hits"] == b["hits"], (a, b)
PY
ok "orders user=1 -> db0.orders_01; repeated compute is item-for-item identical"

say "2. Submit v2 increment affecting only orders ranges"
status=$(http_post "/api/increments" "$DEMO_DIR/samples/increment-v2.json" "$DEMO_DIR/commit-v2.json")
assert_http "$status" "200" "increment v2"
assert_value "$(json_value "$DEMO_DIR/commit-v2.json" 'data.version')" "2" "increment version"
assert_value "$(json_value "$DEMO_DIR/commit-v2.json" 'data.reused')" "False" "increment reuse"
ok "increment v2 committed with base_version=1; affected=$(python3 -c 'import json,sys; print(",".join(json.load(open(sys.argv[1]))["data"]["affected_tables"]))' "$DEMO_DIR/commit-v2.json")"

status=$(http_get "/api/caches" "$DEMO_DIR/caches-v2.json")
assert_http "$status" "200" "cache graph"
python3 - "$DEMO_DIR/caches-v2.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))["data"]
route = {item["table_id"]: item for item in data["route_caches"]}
views = {item["view_id"]: item for item in data["aggregate_caches"]}
assert route["orders"]["cache_version"] == 2, route["orders"]
assert route["order_items"]["cache_version"] == 1, route["order_items"]
assert route["events"]["cache_version"] == 1, route["events"]
assert views["order_overview"]["cache_version"] == 2, views["order_overview"]
assert views["audit_rollup"]["cache_version"] == 1, views["audit_rollup"]
assert [b["physical_table"] for b in route["orders"]["boundary_index"]] == ["db0.orders_00", "db1.orders_02", "db1.orders_03"]
PY
ok "orders cache and order_overview cascaded to v2 atomically; unrelated events/audit_rollup stay at v1"

say "3. Replaced-version read is rejected instead of returning stale routing"
cat >"$DEMO_DIR/query-stale.json" <<'JSON'
{"table_id":"orders","observed_version":1,"kind":"eq","value":"1"}
JSON
status=$(http_post "/api/query" "$DEMO_DIR/query-stale.json" "$DEMO_DIR/stale-read.json")
assert_http "$status" "409" "stale read"
assert_value "$(json_code "$DEMO_DIR/stale-read.json")" "E_STALE_READ" "stale read code"
python3 - "$DEMO_DIR/stale-read.json" <<'PY'
import json, sys
error = json.load(open(sys.argv[1]))["error"]
ctx = error["context"]
assert ctx["table_id"] == "orders" and ctx["cache_version"] == 2 and ctx["observed_version"] == 1, ctx
PY
ok "observed v1 -> E_STALE_READ; context cache_version=2 observed_version=1 table=orders"

cat >"$DEMO_DIR/query-v2.json" <<'JSON'
{"table_id":"orders","observed_version":2,"kind":"eq","value":"1"}
JSON
status=$(http_post "/api/query" "$DEMO_DIR/query-v2.json" "$DEMO_DIR/route-v2.json")
assert_http "$status" "200" "v2 query"
assert_value "$(json_value "$DEMO_DIR/route-v2.json" 'data.physical_tables.0')" "db0.orders_00" "v2 route"
ok "observed v2 -> db0.orders_00; used_version=$(json_value "$DEMO_DIR/route-v2.json" 'data.used_version')"

say "4. Persist intent, force SIGKILL, restart and recover"
set +e
status=$(http_post "/api/increments" "$DEMO_DIR/samples/increment-crash-v3.json" "$DEMO_DIR/interrupted.json")
curl_rc=$?
set -e
[[ $curl_rc -eq 0 ]] || fail "crash request did not return a JSON response (curl rc=$curl_rc)"
assert_http "$status" "409" "interrupt response"
assert_value "$(json_code "$DEMO_DIR/interrupted.json")" "E_REBUILD_INTERRUPTED" "interrupt code"
ok "received E_REBUILD_INTERRUPTED after intent persistence; waiting for forced termination"
set +e
wait "$PID"
server_rc=$?
set -e
PID=""
[[ $server_rc -ne 0 ]] || fail "crash injection did not terminate the server"
ok "server terminated with non-zero status $server_rc; port should now be closed"

pending_before=$(db_value "SELECT COUNT(*) FROM intents WHERE status='pending';")
max_before=$(db_value "SELECT MAX(version) FROM snapshots;")
assert_value "$pending_before" "1" "pending intents before restart"
assert_value "$max_before" "2" "committed snapshot version before restart"
ok "on-disk facts: pending intents=1, last complete snapshot=2"

start_server
ok "server restarted pid=$PID"
status=$(http_get "/api/recovery" "$DEMO_DIR/recovery.json")
assert_http "$status" "200" "recovery report"
python3 - "$DEMO_DIR/recovery.json" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))["data"]
report = data["report"]
assert data["status"] == "recovered", data
assert data["current_version"] == 2, data
assert report["pending_intents_found"] == 1, report
assert report["actions"][0]["decision"] == "aborted_incomplete", report
assert report["facts_only"] is True, report
PY
ok "recovery report: pending=1 decision=aborted_incomplete current_version=2"

status=$(http_post "/api/query" "$DEMO_DIR/query-v2.json" "$DEMO_DIR/route-after-recovery.json")
assert_http "$status" "200" "post-recovery v2 query"
assert_value "$(json_value "$DEMO_DIR/route-after-recovery.json" 'data.physical_tables.0')" "db0.orders_00" "post-recovery route"
intent_status=$(db_value "SELECT status FROM intents WHERE commit_id='inc-ledger-crash-v3';")
assert_value "$intent_status" "aborted" "aborted intent status"
ok "snapshot v2 remains active and interrupted v3 intent is aborted"

say "5. Five-code injection matrix"
python3 - "$DEMO_DIR" <<'PY'
import json, os, sys
demo = sys.argv[1]
snapshot = json.load(open(os.path.join(demo, "samples", "snapshot-v1.json")))
snapshot["version"] = 3
snapshot["checksum"] = "auto"
json.dump({"snapshot": snapshot, "seal_checksum": True, "inject": "mapping_missing"},
          open(os.path.join(demo, "fault-mapping.json"), "w"), indent=2)
json.dump({"snapshot": snapshot, "seal_checksum": True, "inject": "range_overlap"},
          open(os.path.join(demo, "fault-overlap.json"), "w"), indent=2)
inc = json.load(open(os.path.join(demo, "samples", "increment-v2.json")))
inc["commit_id"] = "demo-base-regression"
inc["inject"] = "base_regression"
inc["checksum"] = ""
json.dump(inc, open(os.path.join(demo, "fault-base.json"), "w"), indent=2)
json.dump({"table_id": "orders", "observed_version": 1, "kind": "eq", "value": "1", "inject": "observed_ahead"},
          open(os.path.join(demo, "fault-ahead.json"), "w"), indent=2)
PY

status=$(http_post "/api/snapshots" "$DEMO_DIR/fault-mapping.json" "$DEMO_DIR/fault-mapping-response.json"); assert_http "$status" "400" "mapping fault"
status=$(http_post "/api/snapshots" "$DEMO_DIR/fault-overlap.json" "$DEMO_DIR/fault-overlap-response.json"); assert_http "$status" "400" "overlap fault"
status=$(http_post "/api/increments" "$DEMO_DIR/fault-base.json" "$DEMO_DIR/fault-base-response.json"); assert_http "$status" "409" "base fault"
status=$(http_post "/api/query" "$DEMO_DIR/fault-ahead.json" "$DEMO_DIR/fault-ahead-response.json"); assert_http "$status" "409" "ahead fault"
declare -A CODES
CODES[mapping]=$(json_code "$DEMO_DIR/fault-mapping-response.json")
CODES[overlap]=$(json_code "$DEMO_DIR/fault-overlap-response.json")
CODES[base]=$(json_code "$DEMO_DIR/fault-base-response.json")
CODES[ahead]=$(json_code "$DEMO_DIR/fault-ahead-response.json")
CODES[interrupt]=$(json_code "$DEMO_DIR/interrupted.json")
assert_value "${CODES[mapping]}" "E_PHYSICAL_TABLE_MISSING" "mapping code"
assert_value "${CODES[overlap]}" "E_RANGE_OVERLAP" "overlap code"
assert_value "${CODES[base]}" "E_STALE_BASE" "base code"
assert_value "${CODES[ahead]}" "E_OBSERVED_AHEAD" "ahead code"
assert_value "${CODES[interrupt]}" "E_REBUILD_INTERRUPTED" "interrupt code"
python3 - "${CODES[mapping]}" "${CODES[overlap]}" "${CODES[base]}" "${CODES[ahead]}" "${CODES[interrupt]}" <<'PY'
import sys
codes = list(sys.argv[1:])
assert len(set(codes)) == 5, codes
print("  distinct codes:", ", ".join(codes))
PY
ok "all five injections returned distinct, contextual business codes"

say "Demo complete"
printf '  artifacts: %s\n' "$DEMO_DIR"
printf '  server log: %s\n' "$SERVER_LOG"
printf '  final current version: %s\n' "$(db_value 'SELECT MAX(version) FROM snapshots;')"
printf '  recovery report: %s\n' "$(python3 -c 'import json,sys; r=json.load(open(sys.argv[1]))["data"]["report"]; print(r["cache_action"], r["actions"][0]["decision"])' "$DEMO_DIR/recovery.json")"

# Stop the restarted server cleanly. The interrupt code in the matrix is the
# already-observed E_REBUILD_INTERRUPTED response from step 4.
cleanup
PID=""
