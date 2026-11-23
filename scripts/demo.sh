#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${DEMO_OUT:-$ROOT/artifacts/demo}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/h2diag-demo.XXXXXX")"
BIN="$WORK/h2diag"
SERVER_PID=""
BASE_URL=""

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

start_server() {
  local db="$1"
  local log="$2"
  shift 2
  rm -f "$WORK/ready"
  "$BIN" -addr 127.0.0.1:0 -db "$db" -ready-file "$WORK/ready" "$@" >"$log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if [[ -s "$WORK/ready" ]]; then
      BASE_URL="$(tr -d '\n' < "$WORK/ready")"
      return 0
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      cat "$log" >&2 || true
      return 1
    fi
    sleep 0.1
  done
  cat "$log" >&2 || true
  return 1
}

stop_server() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
  BASE_URL=""
}

post_sample() {
  local sample="$1"
  local output="$2"
  curl --noproxy '*' -sS -o "$output" -w '%{http_code}' \
    -H 'Content-Type: application/json' --data-binary @"$sample" "$BASE_URL/v1/samples"
}

echo "== build =="
mkdir -p "$OUT"
cd "$ROOT"
go build -o "$BIN" ./cmd/h2diag
echo "binary=$BIN"

echo "== 1. complete weighted multiplexing schedule =="
start_server "$WORK/demo.sqlite" "$WORK/server.log"
status="$(post_sample "$ROOT/samples/baseline.json" "$OUT/baseline-response.json")"
[[ "$status" == "201" ]] || { echo "baseline HTTP $status"; cat "$OUT/baseline-response.json"; exit 1; }
curl --noproxy '*' -sS "$BASE_URL/v1/samples/baseline-v1" -o "$OUT/baseline-snapshot.json"
curl --noproxy '*' -sS "$BASE_URL/v1/samples/baseline-v1/rounds?limit=5000" -o "$OUT/baseline-rounds.json"
grep -q '"status":"DONE"' "$OUT/baseline-response.json"
grep -q '"scheduling_rounds":60' "$OUT/baseline-response.json"
grep -q '"sampling_rounds":0' "$OUT/baseline-response.json"
echo "HTTP $status; snapshot=$OUT/baseline-snapshot.json; rounds=$OUT/baseline-rounds.json"
grep -o '"summary":{[^}]*}' "$OUT/baseline-response.json" | head -n 1
grep -o '"cumulative_rounding_error_ppm":[0-9]*' "$OUT/baseline-response.json" | tail -n 1

echo "== 2. flow-control blocking =="
blocked_status="$(post_sample "$ROOT/samples/blocked.json" "$OUT/blocked-response.json")"
[[ "$blocked_status" == "409" ]] || { echo "blocked HTTP $blocked_status"; cat "$OUT/blocked-response.json"; exit 1; }
grep -q '"code":"E_FLOW_CONTROL_BLOCKED"' "$OUT/blocked-response.json"
not_found_status="$(curl --noproxy '*' -sS -o "$OUT/blocked-not-persisted.json" -w '%{http_code}' "$BASE_URL/v1/samples/blocked-v1")"
[[ "$not_found_status" == "404" ]] || { echo "blocked sample unexpectedly persisted"; exit 1; }
echo "HTTP $blocked_status; failure was atomic and left no committed sample"
cat "$OUT/blocked-response.json"

echo "== 3. RST_STREAM removes pending DATA from scheduling =="
reset_status="$(post_sample "$ROOT/samples/reset.json" "$OUT/reset-response.json")"
[[ "$reset_status" == "201" ]] || { echo "reset HTTP $reset_status"; cat "$OUT/reset-response.json"; exit 1; }
grep -q '"state":"RESET"' "$OUT/reset-response.json"
grep -q '"data_bytes_sent":0' "$OUT/reset-response.json"
echo "HTTP $reset_status; RESET stream sent no DATA"
cat "$OUT/reset-response.json"

echo "== 4. business-error contract sweep =="
overflow_status="$(post_sample "$ROOT/samples/window-overflow.json" "$OUT/window-overflow-response.json")"
[[ "$overflow_status" == "409" ]] || { echo "window overflow HTTP $overflow_status"; exit 1; }
grep -q '"code":"E_WINDOW_OVERFLOW"' "$OUT/window-overflow-response.json"
transition_status="$(post_sample "$ROOT/samples/invalid-transition.json" "$OUT/invalid-transition-response.json")"
[[ "$transition_status" == "400" ]] || { echo "invalid transition HTTP $transition_status"; exit 1; }
grep -q '"code":"E_INVALID_TRANSITION"' "$OUT/invalid-transition-response.json"
invalid_frame_status="$(post_sample "$ROOT/samples/invalid-frame.json" "$OUT/invalid-frame-response.json")"
[[ "$invalid_frame_status" == "400" ]] || { echo "invalid frame HTTP $invalid_frame_status"; exit 1; }
grep -q '"code":"E_FRAME_INVALID"' "$OUT/invalid-frame-response.json"
conflict_status="$(post_sample "$ROOT/samples/conflict-original.json" "$OUT/conflict-first-response.json")"
[[ "$conflict_status" == "201" ]] || { echo "conflict seed HTTP $conflict_status"; exit 1; }
conflict_status="$(post_sample "$ROOT/samples/conflict-mutated.json" "$OUT/conflict-response.json")"
[[ "$conflict_status" == "409" ]] || { echo "conflict HTTP $conflict_status"; exit 1; }
grep -q '"code":"E_CONFLICTING_SAMPLE"' "$OUT/conflict-response.json"
echo "E_WINDOW_OVERFLOW=$overflow_status E_INVALID_TRANSITION=$transition_status E_FRAME_INVALID=$invalid_frame_status E_CONFLICTING_SAMPLE=$conflict_status"

echo "== 5. forced termination and restart recovery =="
stop_server
start_server "$WORK/crash.sqlite" "$WORK/crash-server.log" -crash-after-round 1
set +e
crash_http="$(post_sample "$ROOT/samples/baseline.json" "$OUT/crash-request.json")"
crash_curl_rc=$?
wait "$SERVER_PID" 2>/dev/null
crash_wait_rc=$?
set -e
SERVER_PID=""
BASE_URL=""
[[ "$crash_curl_rc" -ne 0 || "$crash_http" == "000" ]] || { echo "server did not terminate during request (HTTP $crash_http)"; exit 1; }
[[ "$crash_wait_rc" -eq 137 ]] || { echo "expected SIGKILL wait status 137, got $crash_wait_rc"; exit 1; }
echo "forced termination observed: curl_rc=$crash_curl_rc wait_rc=$crash_wait_rc"

start_server "$WORK/crash.sqlite" "$WORK/recovery-server.log"
curl --noproxy '*' -sS "$BASE_URL/v1/recovery?sample_id=baseline-v1" -o "$OUT/recovery-report.json"
curl --noproxy '*' -sS "$BASE_URL/v1/samples/baseline-v1" -o "$OUT/recovered-snapshot.json"
grep -q '"outcome":"ABORTED_AT_LAST_COMPLETE_BOUNDARY"' "$OUT/recovery-report.json"
grep -q '"status":"ABORTED"' "$OUT/recovered-snapshot.json"
grep -q '"recomputable_snapshot"' "$OUT/recovery-report.json"
echo "recovery report=$OUT/recovery-report.json snapshot=$OUT/recovered-snapshot.json"
grep -o '"intent_status":"[^"]*"' "$OUT/recovery-report.json" | head -n 1
grep -o '"outcome":"[^"]*"' "$OUT/recovery-report.json" | head -n 1
grep -o '"last_complete_version":[0-9]*' "$OUT/recovery-report.json" | head -n 1

echo "== done =="
echo "artifacts written to $OUT"
