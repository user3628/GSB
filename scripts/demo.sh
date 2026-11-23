#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TMP=$(mktemp -d "${TMPDIR:-/tmp}/upool-demo.XXXXXX")
BIN="$TMP/poolctl"
HTTP="http://127.0.0.1:18700"
CRASH_HTTP="http://127.0.0.1:18710"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill -9 "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

wait_http() {
  local url=$1
  for _ in $(seq 1 200); do
    if curl -sf "$url/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  echo "service did not become healthy: $url" >&2
  return 1
}

echo "[1/6] build"
cd "$ROOT"
GOFLAGS="${GOFLAGS:-}" go build -o "$BIN" ./cmd/poolctl

echo "[2/6] start deterministic loopback upstreams and workbench"
"$BIN" upstream -listen 127.0.0.1:18701 >"$TMP/upstream-18701.log" 2>&1 &
PIDS+=("$!")
"$BIN" upstream -listen 127.0.0.1:18702 >"$TMP/upstream-18702.log" 2>&1 &
PIDS+=("$!")
"$BIN" serve -config samples/lab.json -db "$TMP/workbench.db" >"$TMP/workbench.log" 2>&1 &
MAIN_PID=$!
PIDS+=("$MAIN_PID")
wait_http "$HTTP"

echo "[3/6] submit whole track and segmented track"
curl -sS -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-full.json >"$TMP/full.json"
curl -sS -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-segment-1.json >"$TMP/seg1.json"
curl -sS -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-segment-2.json >"$TMP/seg2.json"
for at in 0 300 401 500 602; do
  curl -sS "$HTTP/v1/tracks/demo-full/snapshot?at_ms=$at" >"$TMP/full-$at.json"
  curl -sS "$HTTP/v1/tracks/demo-segmented/snapshot?at_ms=$at" >"$TMP/seg-$at.json"
done
python3 - "$TMP" <<'PY'
import json, sys
base=sys.argv[1]
def snap(path):
    d=json.load(open(path))
    d.pop('track_id',None); d.pop('requested_at_ms',None); d.pop('final',None)
    return d
def response_snapshot(path):
    d=json.load(open(path))
    s=d['snapshot']
    s.pop('track_id',None)
    return s
full=response_snapshot(base+'/full.json')
seg=response_snapshot(base+'/seg2.json')
assert full==seg, 'whole/segmented final snapshots differ'
for at in (0,300,401,500,602):
    assert snap(f'{base}/full-{at}.json')==snap(f'{base}/seg-{at}.json'), f'snapshot differs at {at}'
payload=json.load(open(base+'/full.json'))
by={r['request_id']:r for r in payload['requests']}
assert by['j2']['error']['code']=='E_POOL_EXHAUSTED'
assert by['r1']['connection_id']==by['r2']['connection_id']==by['r3']['connection_id']
print('PASS snapshot equality: whole == segmented, final + 5 midpoints')
PY

echo "[4/6] duplicate/conflict and pool exhaustion"
curl -sS -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-full.json >"$TMP/duplicate.json"
curl -sS -o "$TMP/conflict.json" -w '%{http_code}' -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' \
  --data-binary '{"track_id":"demo-full","batch_id":"full-batch","requests":[{"request_id":"changed","timestamp_ms":0,"client_id":"x","endpoint_id":"edge-a","request_bytes":1,"upload_rate_bps":1,"response_bytes":1}]}' >"$TMP/conflict.status"
curl -sS -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-exhaustion.json >"$TMP/exhaustion.json"
curl -sS -X POST "$HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-dial-failure.json >"$TMP/dial-failure.json"
python3 - "$TMP" <<'PY'
import json, sys
base=sys.argv[1]
dup=json.load(open(base+'/duplicate.json'))
assert dup['advanced'] is False
assert open(base+'/conflict.status').read().strip()=='409'
assert json.load(open(base+'/conflict.json'))['code']=='E_CONFLICTING_BATCH'
ex=json.load(open(base+'/exhaustion.json'))
by={r['request_id']:r for r in ex['requests']}
for key in ('wait-1','wait-2'):
    assert by[key]['error']['code']=='E_POOL_ACQUIRE_TIMEOUT'
    assert by[key]['error']['details']['waited_ms']==120
dial=json.load(open(base+'/dial-failure.json'))['requests'][0]
assert dial['error']['code']=='E_DIAL_FAILED'
print('PASS idempotency, E_CONFLICTING_BATCH, E_POOL_ACQUIRE_TIMEOUT(waited_ms=120), E_DIAL_FAILED')
PY

echo "[5/6] force-kill after durable intent"
DIAG_CRASH_AFTER_PLAN_INTENT_MS=30000 "$BIN" serve -config samples/lab.json -listen 127.0.0.1:18710 -db "$TMP/crash.db" >"$TMP/crash-first.log" 2>&1 &
CRASH_PID=$!
PIDS+=("$CRASH_PID")
wait_http "$CRASH_HTTP"
curl -sS -X POST "$CRASH_HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-crash-segment-1.json >"$TMP/crash-first-response.json" 2>"$TMP/crash-curl.err" &
CURL_PID=$!
for _ in $(seq 1 200); do grep -q DIAG_PLAN_INTENT_READY "$TMP/crash-first.log" && break; sleep 0.05; done
grep -q DIAG_PLAN_INTENT_READY "$TMP/crash-first.log"
kill -9 "$CRASH_PID"
wait "$CURL_PID" 2>/dev/null || true
sleep 0.2
if ps -p "$CRASH_PID" >/dev/null 2>&1; then echo 'forced termination failed' >&2; exit 1; fi

echo "[6/6] restart, recover and compare resumed result"
"$BIN" serve -config samples/lab.json -listen 127.0.0.1:18710 -db "$TMP/crash.db" >"$TMP/crash-second.log" 2>&1 &
CRASH2_PID=$!
PIDS+=("$CRASH2_PID")
wait_http "$CRASH_HTTP"
curl -sS "$CRASH_HTTP/v1/recovery" >"$TMP/recovery.json"
curl -sS -X POST "$CRASH_HTTP/v1/tracks" -H 'Content-Type: application/json' --data-binary @samples/track-crash-segment-2.json >"$TMP/crash-final.json"
python3 - "$TMP" <<'PY'
import json, sys
base=sys.argv[1]
rec=json.load(open(base+'/recovery.json'))['recovery']
assert rec['aborted_intents'], 'expected an aborted unfinished intent'
assert rec['resumed_batches'], 'expected a resumed batch'
c=json.load(open(base+'/crash-final.json'))['snapshot']
b=json.load(open(base+'/full.json'))['snapshot']
for d in (c,b):
    d.pop('track_id',None)
    for k in ('requested_at_ms','final','horizon_ms','final_seq'): d.pop(k,None)
assert c==b, 'resumed snapshot differs from uninterrupted baseline'
print('PASS recovery: aborted intent=%s resumed=%s; snapshot == uninterrupted baseline' % (len(rec['aborted_intents']), len(rec['resumed_batches'])))
PY

"$BIN" inspect -db "$TMP/workbench.db" >"$TMP/inspect.txt"
cat "$TMP/inspect.txt"
echo "demo artifacts: $TMP"

