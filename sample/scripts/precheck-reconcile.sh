#!/usr/bin/env bash
# Requirement 8: fast precheck vs full-path revalidation, with itemized diffs.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "${HERE}/common.sh"

PID="$(start_server)"; trap 'kill "$PID" 2>/dev/null || true' EXIT
wait_health
ROW="doc-101/body"

log "panel open warms the cache"
OPEN="$(post /rows/open "{\"row_id\":\"$ROW\",\"holder\":\"alice\"}")"
BASE="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["row"]["row_version"])' <<<"$OPEN")"
LV="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"ttl_ms\":5000}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])')"

log "fast precheck completes inside budget and marks its mode/basis"
PRE="$(post /edits/precheck "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV,\"snapshot_ver\":0,\"budget_ms\":300,\"allow_lease_refresh\":true}")"
echo "$PRE" | python3 -m json.tool
python3 -c 'import json,sys;d=json.load(sys.stdin)["data"];assert d["completed"] and d["editable"] and d["mode"]' <<<"$PRE"

log "another client preempts the lease; full reconcile returns the itemized diff"
post /leases/release "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV}" >/dev/null
NEW_LV="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"bob\",\"ttl_ms\":5000}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])')"
VERDICT="$(post /edits/reconcile "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV,\"base_row_version\":$BASE}")"
echo "$VERDICT" | python3 -m json.tool
python3 - <<'PY' "$VERDICT"
import json,sys
d=json.loads(sys.argv[1])["data"]
assert d["pass"] is False and d["failure_code"] == "E_LEASE_LOST"
diff=d["diff"]
assert diff["actual_holder"]=="bob" and diff["actual_lease_version"] != diff["presented_lease_version"]
assert diff["actual_row_version"]==diff["expected_row_version"]
print("itemized diff fields verified")
PY

log "full commit with the stale lease fails E_LEASE_LOST and carries the same diff"
FAIL="$(post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV,\"base_row_version\":$BASE,\"content\":\"stale save\"}")"
echo "$FAIL" | python3 -m json.tool
test "$(code_of "$FAIL")" = "E_LEASE_LOST"

log "bob commits, then alice's base version is stale: E_EDIT_STALE with current row version"
post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"bob\",\"lease_version\":$NEW_LV,\"base_row_version\":$BASE,\"content\":\"bob was here\"}" | python3 -m json.tool
LV2="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"ttl_ms\":5000}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])')"
STALE="$(post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV2,\"base_row_version\":$BASE,\"content\":\"late save\"}")"
echo "$STALE" | python3 -m json.tool
test "$(code_of "$STALE")" = "E_EDIT_STALE"

log "zero-budget precheck returns the incomplete marker, never a mixed verdict"
INC="$(post /edits/precheck "{\"row_id\":\"doc-101/title\",\"holder\":\"alice\",\"lease_version\":1,\"budget_ms\":0}")"
echo "$INC" | python3 -m json.tool
test "$(code_of "$INC")" = "E_PRECHECK_INCOMPLETE"
