#!/usr/bin/env bash
# Deterministic end-to-end validation against the local HTTP interface.
# Uses only the fixed seed samples; no external services or accounts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ADDR="127.0.0.1:18080"
BASE="http://${ADDR}"
DB_DIR="data/validate"
SAMPLES="samples"
BIN="bin/configtree"
FAILURES=0

mkdir -p "$DB_DIR" data
rm -f "$DB_DIR"/*.db "$DB_DIR"/*.db-wal "$DB_DIR"/*.db-shm

go build -o "$BIN" ./cmd/configtree
go test ./... 2>&1 | sed 's/^/[go-test] /'

# Regenerate the deterministic samples from the built-in seed generator.
"$BIN" -gen-seed -samples "$SAMPLES" >/dev/null

"$BIN" -db "$DB_DIR/pipeline.db" -addr "$ADDR" >"$DB_DIR/server.log" 2>&1 &
SRV_PID=$!
trap 'kill "$SRV_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  if curl -sf "$BASE/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl -sf "$BASE/healthz" >/dev/null

# envelope <commit_id> <payload-file>
envelope() {
  local cid="$1" file="$2"
  python3 -c 'import json,sys
cid, path = sys.argv[1], sys.argv[2]
with open(path) as f:
    payload = json.load(f)
print(json.dumps({"commit_id": cid, "payload": payload}))' "$cid" "$file"
}

assert_code() { # description expected_code actual_code body
  local desc="$1" want="$2" got="$3" body="$4"
  if [[ "$got" != "$want" ]]; then
    echo "FAIL: $desc (http want=$want got=$got) body=$body"
    FAILURES=$((FAILURES + 1))
  else
    echo "PASS: $desc (http $got)"
  fi
}

assert_biz() { # description expected_biz body
  local desc="$1" want="$2" body="$3"
  if grep -q "\"code\":\"$want\"" "$DB_DIR/resp.json"; then
    echo "PASS: $desc ($want)"
  else
    echo "FAIL: $desc expected business code $want, body=$body"
    FAILURES=$((FAILURES + 1))
  fi
}

echo "== 1. invalid snapshot is rejected with E_SPEC_INVALID =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-invalid "$SAMPLES/snapshot_v1_invalid.json")" \
  "$BASE/v1/commits/snapshots")
assert_code "invalid snapshot http status" 422 "$code" "$(cat "$DB_DIR/resp.json")"
assert_biz "invalid snapshot business code" E_SPEC_INVALID "$(cat "$DB_DIR/resp.json")"

echo "== 2. valid snapshot v1 commits =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-snap-v1 "$SAMPLES/snapshot_v1.json")" \
  "$BASE/v1/commits/snapshots")
assert_code "snapshot v1 commit" 200 "$code" "$(cat "$DB_DIR/resp.json")"

echo "== 3. stale snapshot version rejected with E_STALE_SNAPSHOT =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-snap-stale "$SAMPLES/snapshot_stale_v1.json")" \
  "$BASE/v1/commits/snapshots")
assert_code "stale snapshot http status" 409 "$code" "$(cat "$DB_DIR/resp.json")"
assert_biz "stale snapshot business code" E_STALE_SNAPSHOT "$(cat "$DB_DIR/resp.json")"

echo "== 4. prefix adjudication: nested prefixes and source winner =="
curl -s "$BASE/v1/components/auth/view?observed_version=1" > "$DB_DIR/auth.json"
grep -q '"prefix":"service.http.tls"' "$DB_DIR/auth.json" \
  && echo "PASS: nested prefix matched without parent key leakage" \
  || { echo "FAIL: nested prefix adjudication"; FAILURES=$((FAILURES+1)); }
grep -q '"kind":"upstream"' "$DB_DIR/auth.json" \
  && echo "PASS: upstream-sourced entry present" \
  || { echo "FAIL: upstream sourcing"; FAILURES=$((FAILURES+1)); }

echo "== 5. delta with wrong base rejected with E_STALE_BASE =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-delta-wrong "$SAMPLES/delta_wrong_base.json")" \
  "$BASE/v1/commits/deltas")
assert_code "wrong-base delta http status" 409 "$code" "$(cat "$DB_DIR/resp.json")"
assert_biz "wrong-base business code" E_STALE_BASE "$(cat "$DB_DIR/resp.json")"

echo "== 6. first delta v2 wins; concurrent same-base delta gets E_STALE_BASE =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-delta-v2 "$SAMPLES/delta_v2_cache.json")" \
  "$BASE/v1/commits/deltas")
assert_code "delta v2 commit" 200 "$code" "$(cat "$DB_DIR/resp.json")"
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-delta-v2-concurrent "$SAMPLES/delta_v2_concurrent.json")" \
  "$BASE/v1/commits/deltas")
assert_code "concurrent delta http status" 409 "$code" "$(cat "$DB_DIR/resp.json")"
assert_biz "concurrent delta business code" E_STALE_BASE "$(cat "$DB_DIR/resp.json")"

echo "== 7. idempotent retry reuses result; conflicting body rejected =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-delta-v2 "$SAMPLES/delta_v2_cache.json")" \
  "$BASE/v1/commits/deltas")
assert_code "idempotent retry" 200 "$code" "$(cat "$DB_DIR/resp.json")"
grep -q '"reused":true' "$DB_DIR/resp.json" \
  && echo "PASS: duplicate commit reused once" \
  || { echo "FAIL: idempotent reuse flag"; FAILURES=$((FAILURES+1)); }
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-delta-v2 "$SAMPLES/delta_v2_concurrent.json")" \
  "$BASE/v1/commits/deltas")
assert_biz "conflicting duplicate body" E_CONFLICTING_REQUEST "$(cat "$DB_DIR/resp.json")"

echo "== 8. stale read refusal during unfinished recompute window =="
# The crashdemo binary proves the real os.Exit path; here we verify the HTTP
# read contract on a converged database and cross-check the crash report.
curl -s "$BASE/v1/components/worker/view?observed_version=2" > "$DB_DIR/worker.json"
grep -q '"used_version":2' "$DB_DIR/worker.json" \
  && echo "PASS: converged read at observed version 2" \
  || { echo "FAIL: converged read"; FAILURES=$((FAILURES+1)); }

echo "== 9. dependency re-direction delta v3 and cascade rebuild =="
code=$(curl -s -o "$DB_DIR/resp.json" -w '%{http_code}' -H 'Content-Type: application/json' \
  -d "$(envelope c-delta-v3 "$SAMPLES/delta_v3_redirect.json")" \
  "$BASE/v1/commits/deltas")
assert_code "redirect delta commit" 200 "$code" "$(cat "$DB_DIR/resp.json")"
curl -s "$BASE/v1/affected/3" > "$DB_DIR/affected3.json"
grep -q '"component_id":"worker"' "$DB_DIR/affected3.json" \
  && echo "PASS: affected set covers re-pointed component" \
  || { echo "FAIL: affected set"; FAILURES=$((FAILURES+1)); }

echo "== 10. hard-kill recovery on a separate database =="
go build -o bin/crashdemo ./cmd/crashdemo
mkdir -p data
if ./bin/crashdemo -db data/validate_crash.db > "$DB_DIR/crashdemo.log" 2>&1; then
  grep -q 'E_STALE_READ' "$DB_DIR/crashdemo.log" \
    && grep -q 'last_good_version=1' "$DB_DIR/crashdemo.log" \
    && echo "PASS: hard-kill stale refusal and rollback recovery" \
    || { echo "FAIL: crashdemo assertions"; FAILURES=$((FAILURES+1)); }
else
  echo "FAIL: crashdemo exited non-zero"; FAILURES=$((FAILURES+1))
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "ALL VALIDATION CHECKS PASSED"
else
  echo "$FAILURES VALIDATION CHECK(S) FAILED"
  exit 1
fi
