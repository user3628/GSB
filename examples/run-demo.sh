#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GO_BIN="${GO_BIN:-go}"
PORT="${RELEASEGATE_PORT:-18181}"
BASE="http://127.0.0.1:${PORT}"
OUT="$ROOT/artifacts/demo"
DB="$OUT/releasegate-demo.db"
BIN="$OUT/releasegate-demo"

mkdir -p "$OUT"
rm -f "$DB" "$DB-shm" "$DB-wal" "$OUT"/*.json "$OUT"/*.log "$OUT"/*.txt

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_health() {
  for _ in $(seq 1 100); do
    if curl -fsS "$BASE/healthz" 2>/dev/null | grep -q '"service":"releasegate"'; then
      return 0
    fi
    sleep 0.05
  done
  echo "server did not become healthy" >&2
  return 1
}

start_server() {
  local log="$1"
  RELEASEGATE_ENABLE_FAULT_INJECTION=1 "$BIN" -listen "127.0.0.1:${PORT}" -db "$DB" >"$log" 2>&1 &
  SERVER_PID=$!
  wait_health
}

request() {
  local method="$1" path="$2" body="${3:-}" output="$4"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" -H 'Content-Type: application/json' --data-binary "@$body" -o "$output" -w '%{http_code}' "$BASE$path"
  else
    curl -sS -X "$method" -o "$output" -w '%{http_code}' "$BASE$path"
  fi
}

echo '== fixture generation =='
"$GO_BIN" run ./cmd/fixturegen -out testdata -seed 20261124

echo '== build =='
"$GO_BIN" build -o "$BIN" ./cmd/releasegate

echo '== create and start release =='
start_server "$OUT/server-first.log"
CODE=$(request POST /v1/releases testdata/release.json "$OUT/release-created.json")
[[ "$CODE" == "201" ]] || { cat "$OUT/release-created.json"; exit 1; }
CODE=$(request POST /v1/releases/demo-progressive-release/start '' "$OUT/started.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/started.json"; exit 1; }
grep -q '"state":"CANARY"' "$OUT/started.json"

echo '== stage 1 ingest, stats, promote =='
CODE=$(request POST /v1/releases/demo-progressive-release/observations testdata/observations-stage-1.json "$OUT/stage1-ingest.json")
[[ "$CODE" == "201" ]] || { cat "$OUT/stage1-ingest.json"; exit 1; }
CODE=$(request GET '/v1/releases/demo-progressive-release/stats?version=1&scope=stage&stage=1' '' "$OUT/stage1-stats.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/stage1-stats.json"; exit 1; }
CODE=$(request GET '/v1/releases/demo-progressive-release/error-contract?version=1&scope=stage&stage=1' '' "$OUT/stage1-error-contract.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/stage1-error-contract.json"; exit 1; }
printf '{"stats_version":1}' > "$OUT/gate1-request.json"
CODE=$(request POST /v1/releases/demo-progressive-release/gate "$OUT/gate1-request.json" "$OUT/gate1.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/gate1.json"; exit 1; }
grep -q '"outcome":"PROMOTE"' "$OUT/gate1.json"
CODE=$(request GET /v1/releases/demo-progressive-release '' "$OUT/promoted-release.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/promoted-release.json"; exit 1; }
grep -q '"state":"PROMOTING"' "$OUT/promoted-release.json"
grep -q '"current_stage":2' "$OUT/promoted-release.json"
grep -q '"traffic_per_mille":300' "$OUT/promoted-release.json"

echo '== stage 2 ingest, threshold-triggered pause =='
CODE=$(request POST /v1/releases/demo-progressive-release/observations testdata/observations-stage-2.json "$OUT/stage2-ingest.json")
[[ "$CODE" == "201" ]] || { cat "$OUT/stage2-ingest.json"; exit 1; }
printf '{"stats_version":2}' > "$OUT/gate2-request.json"
CODE=$(request POST /v1/releases/demo-progressive-release/gate "$OUT/gate2-request.json" "$OUT/gate2-failure.json")
[[ "$CODE" == "409" ]] || { cat "$OUT/gate2-failure.json"; exit 1; }
grep -q '"code":"E_GATE_FAILED"' "$OUT/gate2-failure.json"
CODE=$(request GET /v1/releases/demo-progressive-release '' "$OUT/paused-release.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/paused-release.json"; exit 1; }
grep -q '"state":"OBSERVING"' "$OUT/paused-release.json"
grep -q '"traffic_per_mille":300' "$OUT/paused-release.json"
grep -q '"current_stats_version":2' "$OUT/paused-release.json"

echo '== forced termination after durable intent =='
printf '{"kind":"TRANSITION"}' > "$OUT/force-terminate-request.json"
set +e
curl -sS -X POST -H 'Content-Type: application/json' --data-binary @"$OUT/force-terminate-request.json" \
  -o /dev/null "$BASE/v1/releases/demo-progressive-release/faults/terminate-after-intent" 2>"$OUT/forced-curl-error.txt"
CURL_EXIT=$?
wait "$SERVER_PID"
SERVER_EXIT=$?
set -e
SERVER_PID=''
printf '%s\n' "$CURL_EXIT" > "$OUT/forced-curl-exit.txt"
printf '%s\n' "$SERVER_EXIT" > "$OUT/forced-server-exit.txt"
[[ "$SERVER_EXIT" == "70" ]] || { echo "server exit=$SERVER_EXIT, expected 70"; exit 1; }

echo '== restart and recovery scan =='
start_server "$OUT/server-restart.log"
CODE=$(request GET /v1/releases/demo-progressive-release/recovery-report '' "$OUT/recovery-report.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/recovery-report.json"; exit 1; }
grep -q '"pending_intents_found":1' "$OUT/recovery-report.json"
grep -q '"intents_recovered":1' "$OUT/recovery-report.json"
CODE=$(request GET /v1/releases/demo-progressive-release '' "$OUT/recovered-release.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/recovered-release.json"; exit 1; }
grep -q '"state":"OBSERVING"' "$OUT/recovered-release.json"

echo '== confirmed rollback after recovery =='
printf '{"to":"ROLLED_BACK","reason":"operator confirmed rollback after recovered pause"}' > "$OUT/rollback-request.json"
CODE=$(request POST /v1/releases/demo-progressive-release/transitions "$OUT/rollback-request.json" "$OUT/rolled-back.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/rolled-back.json"; exit 1; }
grep -q '"state":"ROLLED_BACK"' "$OUT/rolled-back.json"
grep -q '"traffic_per_mille":0' "$OUT/rolled-back.json"
CODE=$(request GET /v1/releases/demo-progressive-release/decisions '' "$OUT/decisions.json")
[[ "$CODE" == "200" ]] || { cat "$OUT/decisions.json"; exit 1; }

{
  printf '{'
  printf '"release_created":'; cat "$OUT/release-created.json"; printf ','
  printf '"started":'; cat "$OUT/started.json"; printf ','
  printf '"stage1_ingest":'; cat "$OUT/stage1-ingest.json"; printf ','
  printf '"stage1_stats":'; cat "$OUT/stage1-stats.json"; printf ','
  printf '"stage1_error_contract":'; cat "$OUT/stage1-error-contract.json"; printf ','
  printf '"gate1":'; cat "$OUT/gate1.json"; printf ','
  printf '"promoted_release":'; cat "$OUT/promoted-release.json"; printf ','
  printf '"stage2_ingest":'; cat "$OUT/stage2-ingest.json"; printf ','
  printf '"gate2_failure":'; cat "$OUT/gate2-failure.json"; printf ','
  printf '"paused_release":'; cat "$OUT/paused-release.json"; printf ','
  printf '"recovery_report":'; cat "$OUT/recovery-report.json"; printf ','
  printf '"recovered_release":'; cat "$OUT/recovered-release.json"; printf ','
  printf '"rolled_back":'; cat "$OUT/rolled-back.json"; printf ','
  printf '"decisions":'; cat "$OUT/decisions.json"
  printf '}\n'
} > "$OUT/demo-output.json"

echo 'demo complete:'
printf 'gate1=%s\n' "$(grep -o '"outcome":"[^"]*"' "$OUT/gate1.json" | head -1)"
printf 'gate2=%s\n' "$(grep -o '"code":"[^"]*"' "$OUT/gate2-failure.json" | head -1)"
printf 'recovered=%s\n' "$(grep -o '"state":"[^"]*"' "$OUT/recovered-release.json" | head -1)"
printf 'final=%s\n' "$(grep -o '"state":"[^"]*"' "$OUT/rolled-back.json" | head -1)"
printf 'output=%s\n' "$OUT/demo-output.json"
