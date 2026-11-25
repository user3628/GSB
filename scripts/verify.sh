#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"

WORK_DIR="${WORK_DIR:-$(mktemp -d /tmp/multirecall-verify-XXXXXX)}"
ADDR="${ADDR:-127.0.0.1:18080}"
mkdir -p "$WORK_DIR/bin"

echo "[1/8] go test -count=1 ./..."
go test -count=1 ./...

echo "[2/8] build CLI"
go build -o "$WORK_DIR/bin/multirecall" .
BIN="$WORK_DIR/bin/multirecall"

echo "[3/8] deterministic sample generation"
"$BIN" init --data "$WORK_DIR/data"

echo "[4/8] sequential vs parallel byte identity"
"$BIN" run --data "$WORK_DIR/data" --out "$WORK_DIR/sequential.json" --mode sequential
"$BIN" run --data "$WORK_DIR/data" --out "$WORK_DIR/parallel.json" --mode parallel --workers 4
cmp "$WORK_DIR/sequential.json" "$WORK_DIR/parallel.json"
echo "BYTE_MATCH_PARALLEL=yes"

echo "[5/8] SIGKILL and SQLite recovery byte identity"
set +e
"$BIN" run --data "$WORK_DIR/data" --db "$WORK_DIR/checkpoints.db" \
  --out "$WORK_DIR/killed.json" --mode sequential --kill-after-commits 4
KILL_RC=$?
set -e
if [[ "$KILL_RC" -ne 137 ]]; then
  echo "expected SIGKILL exit code 137, got $KILL_RC" >&2
  exit 1
fi
"$BIN" run --data "$WORK_DIR/data" --db "$WORK_DIR/checkpoints.db" \
  --out "$WORK_DIR/recovered.json" --mode sequential
cmp "$WORK_DIR/sequential.json" "$WORK_DIR/recovered.json"
echo "BYTE_MATCH_RECOVERY=yes"

echo "[6/8] sqlite3 checkpoint inspection"
command -v sqlite3 >/dev/null
sqlite3 "$WORK_DIR/checkpoints.db" <<'SQL'
.headers on
.mode column
SELECT count(*) AS valid_records FROM checkpoint WHERE status='valid';
SELECT query_id, stage, seq, config_version, substr(output_digest, 1, 12) AS digest
FROM checkpoint
WHERE status='valid'
ORDER BY seq
LIMIT 8;
SQL

echo "[7/8] loopback HTTP checks at $ADDR"
"$BIN" serve --data "$WORK_DIR/data" --addr "$ADDR" >"$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  if curl -fsS "http://$ADDR/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://$ADDR/healthz" | grep -q '"ok":true'
curl -fsS "http://$ADDR/v1/recall?query_id=Q001" >"$WORK_DIR/http-q001.json"
grep -q '"passed":true' "$WORK_DIR/http-q001.json"

EMPTY_CODE="$(curl -sS -o "$WORK_DIR/empty.json" -w '%{http_code}' "http://$ADDR/v1/recall?query_id=Q900")"
test "$EMPTY_CODE" = "422"
grep -q 'E_EMPTY_QUERY' "$WORK_DIR/empty.json"

STALE_CODE="$(curl -sS -o "$WORK_DIR/stale.json" -w '%{http_code}' \
  -X POST "http://$ADDR/v1/config/fragment" \
  -H 'Content-Type: application/json' \
  -d '{"base_version":999,"candidate_depth":{"lexical":3,"category":3,"vector":3}}')"
test "$STALE_CODE" = "409"
grep -q 'E_STALE_BASE' "$WORK_DIR/stale.json"

echo "[8/8] HTTP batch parallel report"
curl -fsS -X POST "http://$ADDR/v1/recall/batch" \
  -H 'Content-Type: application/json' \
  -d '{"workers":4,"use_checkpoint":true}' >"$WORK_DIR/http-batch.json"
grep -q '"query_count":10' "$WORK_DIR/http-batch.json"
cleanup
trap - EXIT

echo "WORK_DIR=$WORK_DIR"
echo "ALL_VERIFICATION_CHECKS_PASSED"
