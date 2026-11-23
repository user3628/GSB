#!/usr/bin/env bash
# Forced-termination recovery. No containers: a local process is SIGKILLed and
# the same SQLite file is reopened, then the fact-based recovery report is
# checked against actual read/grant behavior.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "${HERE}/common.sh"

ADDR="${BASE_URL#http://}"
REPORT="${DB_PATH}.recovery.json"
rm -f "$DB_PATH" "$DB_PATH-wal" "$DB_PATH-shm" "$REPORT"

log "start server with fault injection enabled"
"$BIN" -addr "$ADDR" -db "$DB_PATH" -seed sample/seed/rows.json \
  -enable-fault-injection -recovery-report "$REPORT" >"${DB_PATH}.log" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT
wait_health

ROW="doc-102/title"
log "inject an uncommitted lease grant, then SIGKILL (no graceful shutdown)"
post /debug/fault/acquire "{\"row_id\":\"$ROW\",\"holder\":\"zombie-holder\",\"ttl_ms\":60000}" | python3 -m json.tool

log "also inject an uncommitted row-version write on doc-100/title"
LV="$(post /leases/acquire '{"row_id":"doc-100/title","holder":"alice","ttl_ms":60000}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])')"
BEFORE_VER="$(post /rows '{"row_id":"doc-100/title","snapshot_ver":1}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["current_commit_ver"])')"
post /debug/fault/commit "{\"row_id\":\"doc-100/title\",\"holder\":\"alice\",\"lease_version\":$LV,\"base_row_version\":1,\"content\":\"ORPHAN MUST VANISH\"}" | python3 -m json.tool

kill -9 "$PID"
wait "$PID" 2>/dev/null || true
log "process forcibly terminated; reopening the same SQLite file"

"$BIN" -addr "$ADDR" -db "$DB_PATH" -seed sample/seed/rows.json \
  -recovery-report "$REPORT" >"${DB_PATH}.restart.log" 2>&1 &
PID=$!
wait_health

log "recovery report (derived only from durable rows)"
cat "$REPORT" | python3 -m json.tool

log "assertions"
python3 - "$REPORT" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
actions = {(a["kind"], a["action"]) for a in report["actions"]}
assert ("acquire", "rollback_acquire") in actions, actions
assert ("commit", "discard_commit") in actions, actions
assert report["current_commit_ver"] >= 1
print("recovery actions verified:", sorted(actions))
PY

log "the killed grant must be fully grantable again (no E_WRITE_LEASE_HELD)"
GRANT="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"new-holder\",\"ttl_ms\":2000}")"
echo "$GRANT" | python3 -m json.tool
test "$(code_of "$GRANT")" = "OK"

log "the orphan row version must be invisible and commit_ver rewound"
SNAPSHOT_NOW="$(post /rows '{"row_id":"doc-100/title","snapshot_ver":1}')"
echo "$SNAPSHOT_NOW" | python3 -m json.tool
CONTENT="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["content"])' <<<"$SNAPSHOT_NOW")"
test "$CONTENT" != "ORPHAN MUST VANISH"
AFTER_VER="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["current_commit_ver"])' <<<"$SNAPSHOT_NOW")"
test "$AFTER_VER" = "$BEFORE_VER"

log "audit events are queryable over HTTP"
get /recovery/report | python3 -m json.tool
echo "CRASH_RECOVERY_OK"
