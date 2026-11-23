#!/usr/bin/env bash
# End-to-end happy path: panel open -> acquire -> precheck -> commit -> snapshot read.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "${HERE}/common.sh"

PID="$(start_server)"; trap 'kill "$PID" 2>/dev/null || true' EXIT
wait_health
ROW="doc-100/body"

log "open panel at latest snapshot"
OPEN="$(post /rows/open "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\"}")"
echo "$OPEN" | python3 -m json.tool
BASE_ROW_VER="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["row"]["row_version"])' <<<"$OPEN")"
BASE_COMMIT_VER="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["row"]["commit_ver"])' <<<"$OPEN")"

log "acquire write lease"
GRANT="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"ttl_ms\":5000,\"request_id\":\"req-acquire-1\"}")"
echo "$GRANT" | python3 -m json.tool
LEASE_VER="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])' <<<"$GRANT")"

log "second immediate acquire must be E_WRITE_LEASE_HELD"
HELD="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-bob\",\"ttl_ms\":2000}")"
echo "$HELD" | python3 -m json.tool
test "$(code_of "$HELD")" = "E_WRITE_LEASE_HELD"

log "fast precheck (cache primed by panel open)"
PRE="$(post /edits/precheck "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"lease_version\":$LEASE_VER,\"budget_ms\":200,\"allow_lease_refresh\":true}")"
echo "$PRE" | python3 -m json.tool

log "commit edit with lease and base row version in one server transaction"
COMMIT="$(post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"lease_version\":$LEASE_VER,\"base_row_version\":$BASE_ROW_VER,\"content\":\"Updated body paragraph.\",\"request_id\":\"req-commit-1\"}")"
echo "$COMMIT" | python3 -m json.tool

log "duplicate request id with identical payload is served once and reused"
DUP="$(post /edits/commit "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"lease_version\":$LEASE_VER,\"base_row_version\":$BASE_ROW_VER,\"content\":\"Updated body paragraph.\",\"request_id\":\"req-commit-1\"}")"
echo "$DUP" | python3 -m json.tool

log "read old snapshot still returns original content; latest returns new content"
OLD_SNAP="$BASE_COMMIT_VER"
post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":$OLD_SNAP}" | python3 -m json.tool
post /rows "{\"row_id\":\"$ROW\",\"snapshot_ver\":999}" | python3 -m json.tool
