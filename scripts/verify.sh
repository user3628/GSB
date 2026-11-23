#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PORT="${PORT:-$(python3 - <<'PY'
import socket
s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()
PY
)}"
BASE="http://127.0.0.1:${PORT}"
DB="$ROOT/data/verify-$$.db"
BIN="$(mktemp -d)/cdnlab"
LOG="$(mktemp)"
PID=""
cleanup() {
  if [[ -n "${PID}" ]] && kill -0 "$PID" 2>/dev/null; then kill "$PID" 2>/dev/null || true; fi
  rm -f "$DB" "$DB-wal" "$DB-shm" "$LOG"
}
trap cleanup EXIT
go build -o "$BIN" ./cmd/cdnlab
"$BIN" publish --config samples/config.json --path /static/app.js --body 'console.log("app-v1");' >/dev/null
start_server() {
  "$BIN" serve --config samples/config.json --listen "127.0.0.1:${PORT}" --db "$DB" >"$LOG" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    if curl -fsS "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  echo "server did not start" >&2
  cat "$LOG" >&2
  return 1
}
start_server
echo "== prime"
curl -sS -D /tmp/cdnlab-prime.h "$BASE/edge/edge-a/static/app.js" -o /tmp/cdnlab-prime.b
head -1 /tmp/cdnlab-prime.h
rg -i 'x-cache-status|x-content-version' /tmp/cdnlab-prime.h
echo "== persist E_ORIGIN_TIMEOUT fact"
curl -sS -H 'X-Request-ID: verify-origin-timeout' "$BASE/edge/edge-a/static/style.css?case=t&__fault=origin_timeout&__delay_ms=1000" -o /tmp/cdnlab-timeout.json -w 'HTTP %{http_code}\n'
cat /tmp/cdnlab-timeout.json; echo
echo "== persist E_STALE_SNAPSHOT fact"
curl -sS -X POST "$BASE/admin/policies" -H 'Content-Type: application/json' -H 'X-Request-ID: verify-stale-policy' --data-binary @samples/policies/policy-v1.json -o /tmp/cdnlab-stale.json -w 'HTTP %{http_code}\n'
cat /tmp/cdnlab-stale.json; echo
echo "== aborted purge must leave all nodes unchanged"
curl -sS -X POST "$BASE/admin/purge" -H 'Content-Type: application/json' -H 'X-Request-ID: verify-purge-incomplete' \
  --data '{"kind":"path","path":"/static/app.js","interrupt_nodes":["edge-b"]}' \
  -o /tmp/cdnlab-purge.json -w 'HTTP %{http_code}\n'
cat /tmp/cdnlab-purge.json; echo
echo "== force process termination with pending intent"
curl -sS -X POST -H 'X-Request-ID: verify-crash' "$BASE/admin/debug/crash?node=edge-a&path=/static/app.js" -o /tmp/cdnlab-crash.json || true
for _ in $(seq 1 100); do if ! kill -0 "$PID" 2>/dev/null; then break; fi; sleep 0.05; done
wait "$PID" 2>/dev/null || true
PID=""
echo "== restart and recover"
start_server
curl -sS "$BASE/admin/recovery" -o /tmp/cdnlab-recovery.json
curl -sS "$BASE/admin/entries?path=/static/app.js" -o /tmp/cdnlab-entries.json
python3 - <<'PY'
import json
report=json.load(open('/tmp/cdnlab-recovery.json'))
entries=json.load(open('/tmp/cdnlab-entries.json'))['entries']
codes={x['code'] for x in report.get('business_codes',[])}
assert report.get('facts_only') is True, report
assert report.get('scanned_intents',0) >= 1, report
assert report.get('rolled_back_rows',0) >= 1, report
for code in ('E_PURGE_INCOMPLETE','E_ORIGIN_TIMEOUT','E_STALE_SNAPSHOT'):
    assert code in codes, (code,codes)
assert len(entries)==4, entries
assert all(e['state']=='FRESH' for e in entries), entries
print(json.dumps({
  'recovery_report': report,
  'restored_entries': [{'node':e['node_id'],'state':e['state'],'etag':e['etag']} for e in entries]
}, indent=2, ensure_ascii=False))
PY
echo "VERIFY_OK"
