#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="${TM_VERIFY_WORK:-$(mktemp -d /tmp/tm-sync-verify-XXXXXX)}"
mkdir -p "$WORK"
BIN="$WORK/tm-sync"

echo "[1/6] go test ./..."
go test -count=1 ./...

echo "[2/6] build binary"
go build -o "$BIN" ./cmd/tm-sync

echo "[3/6] deterministic demo + incremental byte comparison"
"$BIN" demo --dir "$WORK/demo" --db "$WORK/demo/tm.db" >"$WORK/demo-stdout.json"
python3 - "$WORK/demo/demo-report.json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
assert report["source_import"]["segment_count"] == 12
failed = {x["segment_id"]: x["error"]["code"] for x in report["initial_sync"]["failed_segments"]}
assert failed == {
    "seg-003-empty-target": "E_SEGMENT_EMPTY",
    "seg-008-punct-only": "E_SEGMENT_EMPTY",
}, failed
assert len(report["steps"]) == 2
for step in report["steps"]:
    assert step["byte_identical"] is True, step
    assert step["import"]["invalid_units"], step["import"]
print("demo: 12 source segments, 2 expected E_SEGMENT_EMPTY, 2 increments byte-identical")
PY

echo "[4/6] SIGKILL recovery byte comparison"
CRASH_DB="$WORK/crash.db"
RECOVERED_EXPORT="$WORK/recovered.json"
"$BIN" import-source --db "$CRASH_DB" --version 1 --dir "$WORK/demo" >/dev/null
set +e
"$BIN" sync --db "$CRASH_DB" --crash-after 3 >"$WORK/crash.out" 2>"$WORK/crash.err"
CRASH_RC=$?
set -e
if [[ "$CRASH_RC" -ne 137 ]]; then
  echo "expected SIGKILL exit 137, got $CRASH_RC" >&2
  exit 1
fi
"$BIN" recover --db "$CRASH_DB" >"$WORK/recover.json"
"$BIN" export --db "$CRASH_DB" --out "$RECOVERED_EXPORT"
cmp "$RECOVERED_EXPORT" "$WORK/demo/export-v1.json"
echo "recovery: committed checkpoints only; export matches continuous v1 byte-for-byte"

INC_CRASH_DB="$WORK/inc-crash.db"
"$BIN" import-source --db "$INC_CRASH_DB" --version 1 --dir "$WORK/demo" >/dev/null
"$BIN" sync --db "$INC_CRASH_DB" >/dev/null
"$BIN" import-increment --db "$INC_CRASH_DB" "$WORK/demo/increments/increment-001.jsonl" >/dev/null
set +e
"$BIN" sync --db "$INC_CRASH_DB" --crash-after 2 >"$WORK/inc-crash.out" 2>"$WORK/inc-crash.err"
INC_CRASH_RC=$?
set -e
test "$INC_CRASH_RC" -eq 137
"$BIN" recover --db "$INC_CRASH_DB" >"$WORK/inc-recover.json"
"$BIN" export --db "$INC_CRASH_DB" --out "$WORK/inc-recovered-v2.json"
cmp "$WORK/inc-recovered-v2.json" "$WORK/demo/export-v2-incremental.json"
echo "recovery: mid-increment SIGKILL matches v2 byte-for-byte"

echo "[5/6] concurrent publication mutex"
PUB_DB="$WORK/publish.db"
for i in $(seq 1 16); do
  "$BIN" publish --db "$PUB_DB" --segment seg-lock --seq 1 \
    >"$WORK/pub-$i.out" 2>"$WORK/pub-$i.err" &
done
wait
ACCEPTED=0
STALE=0
OTHER=0
for i in $(seq 1 16); do
  if grep -q '"status": "accepted"' "$WORK/pub-$i.out" 2>/dev/null; then
    ACCEPTED=$((ACCEPTED + 1))
  elif grep -q E_STALE_CHECKPOINT "$WORK/pub-$i.err" 2>/dev/null; then
    STALE=$((STALE + 1))
  else
    OTHER=$((OTHER + 1))
    echo "unexpected publish output $i" >&2
    cat "$WORK/pub-$i.err" >&2
  fi
done
test "$ACCEPTED" -eq 1
test "$STALE" -eq 15
test "$OTHER" -eq 0
echo "publish: accepted=1 stale=15 other=0"

echo "[6/6] HTTP smoke + graceful stop"
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
ADDR="127.0.0.1:$PORT"
"$BIN" serve --db "$WORK/demo/tm.db" --addr "$ADDR" >"$WORK/http.log" 2>&1 &
HTTP_PID=$!
trap 'kill "$HTTP_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  if curl -fsS "http://$ADDR/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "http://$ADDR/healthz" >/dev/null
curl -fsS "http://$ADDR/api/v1/suggestions?q=%E6%8F%90%E4%BA%A4&invalid=1" >"$WORK/http-suggest.json"
python3 - "$WORK/http-suggest.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d["active"], d
assert d["invalid"] and d["invalid"][0]["reason_chain"], d
PY
curl -fsS "http://$ADDR/api/v1/stages" >"$WORK/http-status.json"
curl -sS -X POST "http://$ADDR/api/v1/admin/stop" >/dev/null
for _ in $(seq 1 50); do
  if ! kill -0 "$HTTP_PID" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$HTTP_PID" 2>/dev/null; then
  echo "server did not stop gracefully" >&2
  exit 1
fi
trap - EXIT
echo "http: suggestion/reason-chain/status ok; graceful stop ok"

echo "VERIFY_OK workdir=$WORK"
