#!/usr/bin/env bash
# Deterministic lease conflict, real interruptible wait, renew and stale renew.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "${HERE}/common.sh"

PID="$(start_server)"; trap 'kill "$PID" 2>/dev/null || true' EXIT
wait_health
ROW="doc-100/body"

log "alice acquires the lease"
ALICE="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"ttl_ms\":1200}")"
echo "$ALICE" | python3 -m json.tool
ALICE_LV="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])' <<<"$ALICE")"

log "bob with wait_ms=0 gets E_WRITE_LEASE_HELD including holder and expiry"
BOB_FAST="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-bob\",\"ttl_ms\":1000,\"wait_ms\":0}")"
echo "$BOB_FAST" | python3 -m json.tool
test "$(code_of "$BOB_FAST")" = "E_WRITE_LEASE_HELD"

log "bob enters a bounded interruptible wait in the background"
post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-bob\",\"ttl_ms\":1000,\"wait_ms\":4000}" >/tmp/bob-wait.json &
BOB_JOB=$!
sleep 0.3

log "alice renews before TTL, then releases; bob's parked wait must be woken (no spin)"
post /leases/renew "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"lease_version\":$ALICE_LV,\"ttl_ms\":1200}" | python3 -m json.tool
post /leases/release "{\"row_id\":\"$ROW\",\"holder\":\"editor-alice\",\"lease_version\":$ALICE_LV}" | python3 -m json.tool
wait "$BOB_JOB"
cat /tmp/bob-wait.json | python3 -m json.tool
test "$(code_of "$(cat /tmp/bob-wait.json)")" = "OK"
BOB_LV="$(python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["lease_version"])' </tmp/bob-wait.json)"
test "$BOB_LV" = "$((ALICE_LV + 1))"

log "renew with an outdated lease version returns E_LEASE_STALE"
STALE="$(post /leases/renew "{\"row_id\":\"$ROW\",\"holder\":\"editor-carol\",\"lease_version\":$ALICE_LV,\"ttl_ms\":1000}")"
echo "$STALE" | python3 -m json.tool
test "$(code_of "$STALE")" = "E_LEASE_STALE"

log "wait timeout path: carol waits only 300ms while bob keeps the lease"
TIMEOUT="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-carol\",\"ttl_ms\":5000,\"wait_ms\":300}")"
echo "$TIMEOUT" | python3 -m json.tool
test "$(code_of "$TIMEOUT")" = "E_LEASE_TIMEOUT"

log "let bob's lease expire without renewal; carol then gets a fresh grant"
sleep 1.4
CAROL="$(post /leases/acquire "{\"row_id\":\"$ROW\",\"holder\":\"editor-carol\",\"ttl_ms\":800}")"
echo "$CAROL" | python3 -m json.tool
test "$(code_of "$CAROL")" = "OK"
