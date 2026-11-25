#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ART_DIR="$REPO_ROOT/artifacts/verification"
mkdir -p "$ART_DIR" "$REPO_ROOT/bin"

WORK_ROOT="${IMR_WORK_ROOT:-$(mktemp -d /tmp/imretrace-acceptance.XXXXXX)}"
DEMO="$WORK_ROOT/demo"
CONT="$WORK_ROOT/continuous"
KILL_ROOT="$WORK_ROOT/kill"
BIN="$REPO_ROOT/bin/imretrace"

echo "== versions =="
go version
sqlite3 --version
pkg-config --modversion sqlite3 || true

echo "== offline-capable build =="
go build -o "$BIN" ./cmd/imretrace
GOPROXY=off GOFLAGS=-mod=mod go build ./...

echo "== go tests =="
GOPROXY=off GOFLAGS=-mod=mod go test ./...

mkdir -p "$DEMO" "$CONT" "$KILL_ROOT"

echo "== prepare/build deterministic demo at $DEMO =="
"$BIN" --root "$DEMO" prepare | tee "$ART_DIR/01-prepare.json"
"$BIN" --root "$DEMO" build | tee "$ART_DIR/02-build-v1.json"

TEXT_Q='red circle 红色圆形 blue gradient'
"$BIN" --root "$DEMO" search-text --q "$TEXT_Q" --k 5 > "$ART_DIR/03-search-text-v1.json"
"$BIN" --root "$DEMO" search-image --item item-shard-0001 --k 5 > "$ART_DIR/04-search-image-v1.json"
python3 - <<'PY'
import json, sys
p = 'artifacts/verification/03-search-text-v1.json'
r = json.load(open(p))['result']
top = r['candidates'][0]['item_id']
assert top == 'item-shard-0001', top
assert r['numeric_check']['passed']
print('text top1 =', top)
print('corpus points =', len(r['corpus_error_bound']['scale_estimate']))
PY

echo "== query invalid cases retain partial components =="
set +e
"$BIN" --root "$DEMO" search-text --q 'too many k' --k 99 > "$ART_DIR/05-k-invalid.json"
rc_k=$?
"$BIN" --root "$DEMO" search-text --q '   ' > "$ART_DIR/06-empty-query.json"
rc_empty=$?
set -e
test "$rc_k" -eq 3
test "$rc_empty" -eq 3
grep -q E_QUERY_INVALID "$ART_DIR/05-k-invalid.json"
grep -q E_QUERY_INVALID "$ART_DIR/06-empty-query.json"

echo "== incremental 4-bit patch =="
cat > "$WORK_ROOT/patch4.json" <<'JSON'
{"base_version":1,"quant_bits":4}
JSON
"$BIN" --root "$DEMO" policy patch --file "$WORK_ROOT/patch4.json" | tee "$ART_DIR/07-patch-4bit.json"
"$BIN" --root "$DEMO" search-text --q 'green triangle 绿色三角 yellow' --k 5 > "$ART_DIR/08-search-4bit.json"
grep -q '"quant_bits": 4' "$ART_DIR/07-patch-4bit.json"

echo "== zero fusion weights query invalid with components =="
cat > "$WORK_ROOT/patch-zero.json" <<'JSON'
{"base_version":2,"fusion_weights":{"cosine":0,"term":0}}
JSON
set +e
"$BIN" --root "$DEMO" policy patch --file "$WORK_ROOT/patch-zero.json" > "$ART_DIR/09-patch-zero.json"
"$BIN" --root "$DEMO" search-text --q red --k 3 > "$ART_DIR/10-zero-weights.json"
rc_zero=$?
set -e
test "$rc_zero" -eq 3
grep -q E_QUERY_INVALID "$ART_DIR/10-zero-weights.json"
grep -q partial_components "$ART_DIR/10-zero-weights.json"

echo "== full replacement rebuild =="
"$BIN" --root "$DEMO" policy replace --next-demo | tee "$ART_DIR/11-full-replacement.json"
"$BIN" --root "$DEMO" build | tee "$ART_DIR/12-build-full.json"
"$BIN" --root "$DEMO" search-text --q 'deep blue rectangle 深蓝' --k 5 > "$ART_DIR/13-search-full.json"
active_count=$(sqlite3 "$DEMO/data/imretrace.sqlite" "select count(*) from checkpoints where status='active';")
test "$active_count" -eq 36
echo "active checkpoints after replacement = $active_count"

echo "== rollback rejected =="
set +e
"$BIN" --root "$DEMO" policy replace --file "$DEMO/data/config/policy.v1.json" > "$ART_DIR/14-rollback.json"
rc_rb=$?
set -e
test "$rc_rb" -eq 4
grep -q E_STALE_VERSION "$ART_DIR/14-rollback.json"

echo "== sqlite integrity and uniqueness =="
test "$(sqlite3 "$DEMO/data/imretrace.sqlite" 'PRAGMA integrity_check;')" = ok
dup=$(sqlite3 "$DEMO/data/imretrace.sqlite" "select count(*) from (select shard_id,stage,count(*) c from checkpoints where status='active' group by shard_id,stage having c>1);")
test "$dup" -eq 0

echo "== multi-process publish race =="
RACE="$WORK_ROOT/race"
mkdir -p "$RACE"
"$BIN" --root "$RACE" prepare > "$ART_DIR/25-race-prepare.json"
pids=()
for i in $(seq 1 16); do
  "$BIN" --root "$RACE" publish-test --item item-shard-0001 --seq 1 > "$ART_DIR/26-race-$i.json" 2>&1 &
  pids+=("$!")
done
race_ok=0
race_stale=0
set +e
for pid in "${pids[@]}"; do
  wait "$pid"
  rc=$?
  if [ "$rc" -eq 0 ]; then race_ok=$((race_ok+1));
  elif [ "$rc" -eq 4 ]; then race_stale=$((race_stale+1));
  else echo "unexpected publish race exit=$rc"; exit 1; fi
done
set -e
test "$race_ok" -eq 1
test "$race_stale" -eq 15
race_count=$(sqlite3 "$RACE/data/imretrace.sqlite" "select count(*) from checkpoints where shard_id='shard-0001' and seq=1 and status='active';")
test "$race_count" -eq 1
echo "race winners=$race_ok stale=$race_stale active_seq_rows=$race_count"

echo "== SIGKILL recovery byte comparison =="
"$BIN" --root "$CONT" build > "$ART_DIR/15-continuous-build.json"
mkdir -p "$KILL_ROOT"
"$BIN" --root "$KILL_ROOT" prepare >/dev/null
set +e
"$BIN" --root "$KILL_ROOT" build --crash-after-stage 2 > "$ART_DIR/16-killed-build.json" 2>&1
kill_rc=$?
set -e
test "$kill_rc" -eq 137
committed=$(sqlite3 "$KILL_ROOT/data/imretrace.sqlite" "select count(*) from checkpoints where status='active';")
test "$committed" -eq 10
"$BIN" --root "$KILL_ROOT" build > "$ART_DIR/17-recovery-build.json"
h_cont=$("$BIN" --root "$CONT" state-hash | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["state_hash"])')
h_rec=$("$BIN" --root "$KILL_ROOT" state-hash | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["state_hash"])')
test "$h_cont" = "$h_rec"
"$BIN" --root "$CONT" search-text --q "$TEXT_Q" --k 5 > "$ART_DIR/18-cont-search.json"
"$BIN" --root "$KILL_ROOT" search-text --q "$TEXT_Q" --k 5 > "$ART_DIR/19-rec-search.json"
cmp "$ART_DIR/18-cont-search.json" "$ART_DIR/19-rec-search.json"
echo "recovery state hash = $h_rec"

echo "== export diagnostic report =="
"$BIN" --root "$DEMO" report --out "$ART_DIR/20-diagnostics-report.json" > "$ART_DIR/20-report-stdout.json"

echo "== HTTP workbench and graceful stop =="
FREE_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
ADDR="127.0.0.1:${FREE_PORT}"
"$BIN" --root "$DEMO" serve --addr "$ADDR" > "$ART_DIR/21-http.log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  if curl -fsS "http://$ADDR/api/status" > "$ART_DIR/22-http-status.json" 2>/dev/null; then break; fi
  sleep 0.1
done
grep -Eq '"ok":[[:space:]]*true' "$ART_DIR/22-http-status.json"
curl -fsS -X POST "http://$ADDR/api/search/text" -H 'Content-Type: application/json' -d '{"query":"rose red 玫红","k":3}' > "$ART_DIR/23-http-search.json"
curl -fsS -X POST "http://$ADDR/api/stop" > "$ART_DIR/24-http-stop.json"
for _ in $(seq 1 50); do
  if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
wait "$server_pid" || true
trap - EXIT

echo "ALL_ACCEPTANCE_CHECKS_PASSED"
echo "WORK_ROOT=$WORK_ROOT"
echo "ARTIFACTS=$ART_DIR"
