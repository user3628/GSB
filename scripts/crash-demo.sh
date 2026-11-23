#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PORT:-18082}"
BASE="http://127.0.0.1:${PORT}"
mkdir -p artifacts
STAMP="$(date +%Y%m%d-%H%M%S)-$$"
DB="$ROOT/artifacts/crash-demo-${STAMP}.db"
LOG="$ROOT/artifacts/crash-demo-${STAMP}.server.log"
BIN="$ROOT/bin/workbench"
TMP="$ROOT/artifacts/crash-demo-${STAMP}.tmp"
mkdir -p "$TMP"

server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_health() {
  for _ in $(seq 1 100); do
    if curl -fsS --max-time 1 "$BASE/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  return 1
}

call() {
  local method="$1" path="$2" body="$3" out="$4"
  if [[ -n "$body" ]]; then
    curl -sS --max-time 20 -X "$method" -H 'Content-Type: application/json' --data-binary @"$body" -o "$out" -w '%{http_code}' "$BASE$path"
  else
    curl -sS --max-time 20 -X "$method" -o "$out" -w '%{http_code}' "$BASE$path"
  fi
}

printf '== build and start ==\n'
go build -o "$BIN" ./cmd/workbench
"$BIN" -addr "127.0.0.1:${PORT}" -db "$DB" -models ./samples/models.json -web ./web -allow-exit-fault >"$LOG" 2>&1 &
server_pid=$!
wait_health || { cat "$LOG"; exit 1; }
printf '  db=%s\n  log=%s\n' "$DB" "$LOG"

code="$(call POST "/api/v1/traces" "samples/traces/crash.json" "$TMP/submit.json")"
[[ "$code" == "201" ]] || { printf 'trace submit failed: %s\n' "$code"; exit 1; }

printf '== deliberate hard termination ==\n'
set +e
curl -sS --max-time 10 -X POST -H 'Content-Type: application/json' --data-binary @samples/organize-crash.json -o "$TMP/crash-response.txt" -w '%{http_code}' "$BASE/api/v1/traces/crash/organize?version=v1" > "$TMP/crash-code.txt"
curl_exit=$?
wait "$server_pid"
server_exit=$?
server_pid=""
set -e
if [[ "$curl_exit" -ne 0 && "$server_exit" -eq 87 ]]; then
  printf '  [PASS] server exited at durable batch intent boundary (code %s, curl %s)\n' "$server_exit" "$curl_exit"
else
  printf '  [FAIL] unexpected crash result: server=%s curl=%s\n' "$server_exit" "$curl_exit"
  cat "$LOG"
  exit 1
fi

printf '== restart and recover ==\n'
"$BIN" -addr "127.0.0.1:${PORT}" -db "$DB" -models ./samples/models.json -web ./web -allow-exit-fault >>"$LOG" 2>&1 &
server_pid=$!
wait_health || { cat "$LOG"; exit 1; }

code="$(call GET "/api/v1/recovery/report" "" "$TMP/recovery.json")"
[[ "$code" == "200" ]] || { printf 'recovery query failed: %s\n' "$code"; exit 1; }
grep -Fq '"clean":false' "$TMP/recovery.json"
grep -Fq '"completion_claim":"NO_COMPLETION_CLAIM_FOR_INTERRUPTED_RUN"' "$TMP/recovery.json"
grep -Fq '"preserved_complete_batches":["' "$TMP/recovery.json"
ABORTED_RUN="$(sed -n 's/.*"aborted_runs":\["\([^"]*\)".*/\1/p' "$TMP/recovery.json")"
printf '  [PASS] recovery scanned started intent and preserved complete boundary\n'
printf '  aborted_run=%s\n' "$ABORTED_RUN"

code="$(call GET "/api/v1/runs/${ABORTED_RUN}" "" "$TMP/aborted-run.json")"
[[ "$code" == "200" ]] || { printf 'aborted run query failed: %s\n' "$code"; exit 1; }
grep -Fq '"status":"aborted"' "$TMP/aborted-run.json"
printf '  [PASS] run is aborted; no completion conclusion was invented\n'

printf '== reorganize after recovery ==\n'
printf '{"mode":"full","node_vram_bytes":"128Gi","latency_decimals":2}' > "$TMP/retry-options.json"
code="$(call POST "/api/v1/traces/crash/organize?version=v1" "$TMP/retry-options.json" "$TMP/retry-run.json")"
[[ "$code" == "200" ]] || { printf 'retry failed: %s\n' "$code"; cat "$TMP/retry-run.json"; exit 1; }
RETRY_RUN="$(sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' "$TMP/retry-run.json" | head -1)"
grep -Fq '"status":"complete"' "$TMP/retry-run.json"
[[ "$RETRY_RUN" == *-a2 ]] || { printf 'expected deterministic attempt suffix, got %s\n' "$RETRY_RUN"; exit 1; }
printf '  [PASS] new deterministic attempt %s completed; aborted facts retained\n' "$RETRY_RUN"

printf '\n== result ==\n  crash/recovery demo: PASS\n  artifacts: %s\n  database: %s\n' "$TMP" "$DB"

