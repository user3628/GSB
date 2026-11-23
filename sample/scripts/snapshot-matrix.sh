#!/usr/bin/env bash
# Versioned read snapshot matrix: ahead/not-found/visibility/retired stability.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "${HERE}/common.sh"

PID="$(start_server)"; trap 'kill "$PID" 2>/dev/null || true' EXIT
wait_health
ROW="doc-100/title"

log "snapshot ahead of current commit version"
AHEAD="$(post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":9999}")"
echo "$AHEAD" | python3 -m json.tool
test "$(code_of "$AHEAD")" = "E_SNAPSHOT_AHEAD"

log "pin snapshot 1 (seed version) as a long-lived reader"
post /snapshots "{\"snapshot_ver\":1,\"holder\":\"alice\"}" | python3 -m json.tool
FIRST="$(post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":1}")"
echo "$FIRST" | python3 -m json.tool
FIRST_HASH="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["content_hash"])' <<<"$FIRST")"

log "two edits produce commit versions 6 and 7, each with a new row version"
LV="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"ttl_ms\":5000}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])')"
post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV,\"base_row_version\":1,\"content\":\"Plan v2\"}" >/dev/null
LV="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"ttl_ms\":5000}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])')"
COMMIT2="$(post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"alice\",\"lease_version\":$LV,\"base_row_version\":2,\"content\":\"Plan v3\"}")"
echo "$COMMIT2" | python3 -m json.tool

log "old snapshot 1 stays stable while latest snapshot sees Plan v3"
AGAIN="$(post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":1}")"
test "$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["content_hash"])' <<<"$AGAIN")" = "$FIRST_HASH"
CURRENT_VER="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["current_commit_ver"])' <<<"$AGAIN")"
post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":$CURRENT_VER}" | python3 -m json.tool

log "active snapshot 1 protects row version 1 from recycling"
RECYCLE_SAFE="$(post /recycle "{\"row_id\":\"$ROW\",\"min_retain_snapshots\":1,\"min_retain_versions\":1}")"
echo "$RECYCLE_SAFE" | python3 -m json.tool
test "$(python3 -c 'import json,sys;d=json.load(sys.stdin)["data"];print(1 in d["kept_row_versions"])' <<<"$RECYCLE_SAFE")" = "True"

log "release the reader; recycling retires version 1, old snapshot now gets E_VERSION_RETIRED"
post /snapshots "{\"snapshot_ver\":1,\"holder\":\"alice\",\"action\":\"release\"}" >/dev/null
RECYCLE="$(post /recycle "{\"row_id\":\"$ROW\",\"min_retain_snapshots\":99,\"min_retain_versions\":1}")"
echo "$RECYCLE" | python3 -m json.tool
RETIRED="$(post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":1}")"
echo "$RETIRED" | python3 -m json.tool
test "$(code_of "$RETIRED")" = "E_VERSION_RETIRED"

log "repeating the same snapshot never substitutes a newer row version"
RETIRED2="$(post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":1}")"
test "$(code_of "$RETIRED2")" = "E_VERSION_RETIRED"
