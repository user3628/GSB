#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-18081}"
WORK="${WORK_DIR:-$ROOT/.demo-run}"
DB="$WORK/columnar.db"
EXE="$WORK/columnar-server"
BASE="http://127.0.0.1:$PORT"
rm -rf "$WORK"
mkdir -p "$WORK"
cd "$ROOT"
go build -o "$EXE" ./cmd/server

PID=""
cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_server() {
  local suffix="$1"
  COLUMNAR_ALLOW_CRASH=1 "$EXE" -listen "127.0.0.1:$PORT" -db "$DB" -auto-seed=false -allow-crash \
    >"$WORK/server-$suffix.out.log" 2>"$WORK/server-$suffix.err.log" &
  PID=$!
  for _ in $(seq 1 100); do
    if curl -fsS "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "$PID" 2>/dev/null; then cat "$WORK/server-$suffix.err.log"; return 1; fi
    sleep 0.1
  done
  return 1
}

post_json() {
  curl -fsS -X POST -H 'Content-Type: application/json' --data "$2" "$BASE$1"
}

start_server first
post_json /api/v1/snapshots/generate '{"version":1,"rows":257,"seed":20240517}' >/dev/null
post_json /api/v1/chains '{
  "id":"demo-sales","version":1,"base_snapshot_version":1,
  "operators":[
    {"kind":"scan","scan":{"columns":["id","quantity","price","category"]}},
    {"kind":"filter","filter":{"predicate":{"column":"quantity","op":"ge","value_int":20}}},
    {"kind":"project","project":{"columns":["id","quantity","price","category"]}},
    {"kind":"aggregate","aggregate":{"specs":[
      {"name":"row_count","func":"count"},
      {"name":"quantity_count","func":"count_non_null","column":"quantity"},
      {"name":"quantity_sum","func":"sum","column":"quantity"},
      {"name":"quantity_avg","func":"avg","column":"quantity"},
      {"name":"price_sum","func":"sum","column":"price"},
      {"name":"price_avg","func":"avg","column":"price"},
      {"name":"category_distinct","func":"count_distinct","column":"category"}
    ]}}
  ]
}' >/dev/null
SMALL="$(post_json /api/v1/chains/demo-sales/execute '{"execution_id":"demo-small","chain_version":1,"bitmap_form":"all","batch_size":8,"large_vector_threshold":128}')"
LARGE="$(post_json /api/v1/chains/demo-sales/execute '{"execution_id":"demo-large","chain_version":1,"bitmap_form":"all","batch_size":256,"large_vector_threshold":128}')"
COMPARE="$(curl -fsS "$BASE/api/v1/executions/demo-small/compare?right=demo-large&tolerance_ppm=1")"
python3 - "$SMALL" "$LARGE" "$COMPARE" <<'PY'
import json,sys
small,large,cmp=map(json.loads,sys.argv[1:])
print(f"consistency: small_path={small['path']['path']} batches={small['path']['batch_count']}; large_path={large['path']['path']} batches={large['path']['batch_count']}; rows_equal={str(cmp['rows_consistent']).lower()}; aggregates_equal={str(cmp['aggregates_consistent']).lower()}")
PY

while read -r kind expected; do
  body="{\"execution_id\":\"fault-$kind\",\"chain_version\":1,\"bitmap_form\":\"all\",\"batch_size\":8,\"large_vector_threshold\":128,\"injections\":[{\"kind\":\"$kind\",\"batch\":2}]}"
  code_file="$WORK/fault-$kind.json"
  status="$(curl -sS -o "$code_file" -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data "$body" "$BASE/api/v1/chains/demo-sales/execute")"
  view="$(curl -fsS "$BASE/api/v1/executions/fault-$kind")"
  python3 - "$kind" "$expected" "$status" "$code_file" "$view" <<'PY'
import json,sys
kind,expected,status,path,view=sys.argv[1:]
err=json.load(open(path))["error"]
v=json.loads(view)
assert err["code"]==expected, (kind,err)
print(f"fault: kind={kind} code={err['code']} http={status} status={v['status']} published={str(v['published']).lower()} batches={len(v.get('batches') or [])} aggregates={len(v.get('aggregates') or [])}")
PY
done <<'FAULTS'
bitmap_mismatch E_BITMAP_MISMATCH
fixed_overflow E_NUMERIC_OVERFLOW
dict_index E_DICT_INDEX
divide_by_zero E_DIVIDE_BY_ZERO
exec_aborted E_EXEC_ABORTED
FAULTS

set +e
curl -sS -X POST -H 'Content-Type: application/json' --data '{"execution_id":"crash-demo","chain_version":1,"bitmap_form":"all","batch_size":8,"large_vector_threshold":128,"crash_after_batch":2}' "$BASE/api/v1/chains/demo-sales/execute" >/dev/null 2>&1
set +e
wait "$PID" 2>/dev/null
CRASH_EXIT=$?
set -e
echo "forced termination: process_exit_code=$CRASH_EXIT"
PID=""

start_server recovered
VIEW="$(curl -fsS "$BASE/api/v1/executions/crash-demo")"
REPORTS="$(curl -fsS "$BASE/api/v1/recovery/reports")"
python3 - "$VIEW" "$REPORTS" <<'PY'
import json,sys
view,reports=map(json.loads,sys.argv[1:])
report=reports["reports"][-1]
print(f"recovery: execution={view['execution_id']} status={view['status']} published={str(view['published']).lower()} rolled_back_batches={report['rolled_back_batches']} discarded_aggregates={report['discarded_aggregate_batches']} intents={len(report['intents'])}")
print(f"recovery artifact: report={report['id']}")
PY
