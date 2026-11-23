#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PORT:-18081}"
BASE="http://127.0.0.1:${PORT}"
mkdir -p artifacts
STAMP="$(date +%Y%m%d-%H%M%S)-$$"
DB="$ROOT/artifacts/demo-${STAMP}.db"
LOG="$ROOT/artifacts/demo-${STAMP}.server.log"
BIN="$ROOT/bin/workbench"
TMP="$ROOT/artifacts/demo-${STAMP}.tmp"
mkdir -p "$TMP"

pass=0
failures=0

pass_msg() { pass=$((pass + 1)); printf '  [PASS] %s\n' "$1"; }
fail_msg() { failures=$((failures + 1)); printf '  [FAIL] %s\n' "$1"; }
expect_code() {
  local actual="$1" expected="$2" label="$3"
  if [[ "$actual" == "$expected" ]]; then pass_msg "$label (HTTP $actual)"; else fail_msg "$label (got $actual, want $expected)"; fi
}
expect_contains() {
  local file="$1" needle="$2" label="$3"
  if grep -Fq -- "$needle" "$file"; then pass_msg "$label"; else fail_msg "$label (missing: $needle)"; fi
}
run_id_of() { sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' "$1" | head -1; }

call() {
  local method="$1" path="$2" body="$3" out="$4"
  if [[ -n "$body" ]]; then
    curl -sS --max-time 20 -X "$method" -H 'Content-Type: application/json' --data-binary @"$body" -o "$out" -w '%{http_code}' "$BASE$path"
  else
    curl -sS --max-time 20 -X "$method" -o "$out" -w '%{http_code}' "$BASE$path"
  fi
}

server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

printf '== build ==\n'
go build -o "$BIN" ./cmd/workbench
pass_msg "go build ./cmd/workbench"

printf '== start local server ==\n'
"$BIN" -addr "127.0.0.1:${PORT}" -db "$DB" -models ./samples/models.json -web ./web >"$LOG" 2>&1 &
server_pid=$!
for _ in $(seq 1 100); do
  if curl -fsS --max-time 1 "$BASE/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.05
done
if curl -fsS --max-time 2 "$BASE/healthz" >/dev/null; then pass_msg "health endpoint on 127.0.0.1:${PORT}"; else fail_msg "server did not become healthy"; cat "$LOG"; exit 1; fi
printf '  db=%s\n  log=%s\n' "$DB" "$LOG"

printf '== submit deterministic traces ==\n'
for trace in demo mix-reject split capacity crash order; do
  code="$(call POST "/api/v1/traces" "samples/traces/${trace}.json" "$TMP/${trace}.submit.json")"
  expect_code "$code" "201" "submit ${trace}"
done
code="$(call POST "/api/v1/traces" "samples/traces/demo.json" "$TMP/demo.duplicate.json")"
expect_code "$code" "200" "idempotent duplicate trace"
expect_contains "$TMP/demo.duplicate.json" '"created":false' "duplicate returns created=false"

printf '== fast/full organization and comparison ==\n'
code="$(call POST "/api/v1/traces/demo/organize?version=v1" "samples/organize-full.json" "$TMP/full.json")"
expect_code "$code" "200" "full organization"
FULL_ID="$(run_id_of "$TMP/full.json")"
expect_contains "$TMP/full.json" '"batch_count":2' "full uses 2 batches"
expect_contains "$TMP/full.json" '"sample_conservation_ok":true' "full sample conservation"

code="$(call POST "/api/v1/traces/demo/organize?version=v1" "samples/organize-fast.json" "$TMP/fast.json")"
expect_code "$code" "200" "fast organization"
FAST_ID="$(run_id_of "$TMP/fast.json")"
expect_contains "$TMP/fast.json" '"deferred_request_count":3' "fast defers 3 requests"
expect_contains "$TMP/fast.json" '"mode_label":"low_latency"' "fast exposes low_latency mode marker"
expect_contains "$TMP/fast.json" '"reason":"ORGANIZATION_BUDGET_EXCEEDED"' "fast deferral has reason"

code="$(call GET "/api/v1/compare?left=${FAST_ID}&right=${FULL_ID}" "" "$TMP/compare.json")"
expect_code "$code" "200" "fast/full comparison"
expect_contains "$TMP/compare.json" '"same_composition_pairs":1' "comparison pairs common first batch"

printf '== quantile and precision contracts ==\n'
FIRST_BATCH="$(grep -o '"batch_id":"[^"]*"' "$TMP/full.json" | sed 's/.*:"//;s/"$//' | head -1)"
SECOND_BATCH="$(grep -o '"batch_id":"[^"]*"' "$TMP/full.json" | sed 's/.*:"//;s/"$//' | tail -1)"
code="$(call GET "/api/v1/batches/${FIRST_BATCH}/quantiles?p=99" "" "$TMP/p99-ok.json")"
expect_code "$code" "200" "P99 available for 120-sample batch"
expect_contains "$TMP/p99-ok.json" '"sample_count":120' "P99 reports sample size"
code="$(call GET "/api/v1/batches/${SECOND_BATCH}/quantiles?p=99" "" "$TMP/p99-insufficient.json")"
expect_code "$code" "422" "P99 insufficient on 29-sample batch"
expect_contains "$TMP/p99-insufficient.json" '"code":"E_INSUFFICIENT_SAMPLES"' "insufficient sample business code"

code="$(call POST "/api/v1/traces/demo/organize?version=v1" "samples/organize-precision-breach.json" "$TMP/precision-breach.json")"
expect_code "$code" "422" "precision probe breach"
expect_contains "$TMP/precision-breach.json" '"code":"E_PRECISION_BREACH"' "precision breach business code"
expect_contains "$TMP/precision-breach.json" '"status":"precision_failed"' "failed precision batch persisted"

printf '== organization order identity and packing ==\n'
code="$(call POST "/api/v1/traces/order/organize?version=v1" "samples/organize-order-arrival.json" "$TMP/order-arrival.json")"
expect_code "$code" "200" "arrival-order organization"
ORDER_ARRIVAL_ID="$(run_id_of "$TMP/order-arrival.json")"
expect_contains "$TMP/order-arrival.json" '"organization_order":"arrival"' "arrival order marker"
expect_contains "$TMP/order-arrival.json" '"sample_conservation_ok":true' "arrival order conserves samples"
code="$(call POST "/api/v1/traces/order/organize?version=v1" "samples/organize-order-priority.json" "$TMP/order-priority.json")"
expect_code "$code" "200" "priority-order organization"
ORDER_PRIORITY_ID="$(run_id_of "$TMP/order-priority.json")"
expect_contains "$TMP/order-priority.json" '"organization_order":"priority"' "priority order marker"
expect_contains "$TMP/order-priority.json" '"sample_conservation_ok":true' "priority order conserves samples"
if [[ "$ORDER_ARRIVAL_ID" != "$ORDER_PRIORITY_ID" ]]; then pass_msg "organization order changes deterministic run identity"; else fail_msg "organization order reused the same run identity"; fi
ORDER_ARRIVAL_FIRST="$(grep -o '"request_id":"[^"]*"' "$TMP/order-arrival.json" | sed 's/.*:"//;s/"$//' | head -1)"
ORDER_PRIORITY_FIRST="$(grep -o '"request_id":"[^"]*"' "$TMP/order-priority.json" | sed 's/.*:"//;s/"$//' | head -1)"
if [[ "$ORDER_ARRIVAL_FIRST" == "low" && "$ORDER_PRIORITY_FIRST" == "high-1" ]]; then pass_msg "priority order places high-priority request first"; else fail_msg "unexpected first requests: arrival=$ORDER_ARRIVAL_FIRST priority=$ORDER_PRIORITY_FIRST"; fi
code="$(call POST "/api/v1/traces/order/organize?version=v1" "samples/organize-order-priority.json" "$TMP/order-priority-repeat.json")"
expect_code "$code" "200" "priority-order deterministic replay"
if cmp -s "$TMP/order-priority.json" "$TMP/order-priority-repeat.json"; then pass_msg "priority-order replay is byte-identical"; else fail_msg "priority-order replay changed"; fi

printf '== late, mix, split and capacity faults ==\n'
code="$(call POST "/api/v1/traces/demo/organize?version=v1" "samples/organize-late-new-batch.json" "$TMP/late-new.json")"
expect_code "$code" "200" "late request isolated into a new batch"
expect_contains "$TMP/late-new.json" '"late_sealed":true' "late batch sealed marker"
expect_contains "$TMP/late-new.json" '"split_reason":"LATE_AFTER_SEAL_NEW_BATCH"' "late isolation split basis"

code="$(call POST "/api/v1/traces/demo/organize?version=v1" "samples/organize-late-reject.json" "$TMP/late-reject.json")"
expect_code "$code" "200" "late reject policy"
expect_contains "$TMP/late-reject.json" '"reason":"LATE_AFTER_SEAL"' "late reject reason"

code="$(call POST "/api/v1/traces/mix-reject/organize?version=v1" "samples/organize-mix-reject.json" "$TMP/mix.json")"
expect_code "$code" "200" "mixed-model latency gate"
expect_contains "$TMP/mix.json" '"reason":"MIXED_MODEL_LATENCY_LIMIT"' "mixed-model rejection reason"

code="$(call POST "/api/v1/traces/split/organize?version=v1" "samples/organize-split.json" "$TMP/split.json")"
expect_code "$code" "200" "sample-limit split"
expect_contains "$TMP/split.json" '"split_reason":"MODEL_MAX_SAMPLES"' "split child basis"
expect_contains "$TMP/split.json" '"sample_conservation_ok":true' "split conserves samples"
expect_contains "$TMP/split.json" '"total_samples":512' "split child-1 sample count"
expect_contains "$TMP/split.json" '"total_samples":188' "split child-2 sample count"

code="$(call POST "/api/v1/traces/capacity/organize?version=v1" "samples/organize-capacity.json" "$TMP/capacity.json")"
expect_code "$code" "409" "insufficient capacity"
expect_contains "$TMP/capacity.json" '"code":"E_INSUFFICIENT_CAPACITY"' "capacity business code"

printf '{"mode":"full","node_vram_bytes":"128Gi","faults":{"duplicate_request_id":"r1"}}' > "$TMP/duplicate-fault-options.json"
code="$(call POST "/api/v1/traces/demo/organize?version=v1" "$TMP/duplicate-fault-options.json" "$TMP/duplicate-fault.json")"
expect_code "$code" "409" "duplicate request fault"
expect_contains "$TMP/duplicate-fault.json" '"code":"E_CONFLICTING_REQUEST"' "duplicate request conflict code"

printf '{"mode":"full","node_vram_bytes":"128Gi","faults":{"vram_shortfall_bytes":"127Gi"}}' > "$TMP/vram-shortfall-options.json"
code="$(call POST "/api/v1/traces/demo/organize?version=v1" "$TMP/vram-shortfall-options.json" "$TMP/vram-shortfall.json")"
expect_code "$code" "409" "injected VRAM shortfall"
expect_contains "$TMP/vram-shortfall.json" '"code":"E_INSUFFICIENT_CAPACITY"' "injected shortfall capacity code"

printf '== trace/spec errors and conflict ==\n'
code="$(call POST "/api/v1/traces" "samples/traces/bad-monotonic.json" "$TMP/bad-monotonic.json")"
expect_code "$code" "400" "non-monotonic arrival rejected"
expect_contains "$TMP/bad-monotonic.json" '"code":"E_TRACE_INVALID"' "trace invalid business code"
code="$(call POST "/api/v1/traces" "samples/traces/bad-model.json" "$TMP/bad-model.json")"
expect_code "$code" "400" "unknown model rejected"
expect_contains "$TMP/bad-model.json" '"code":"E_TRACE_INVALID"' "unknown model trace code"

printf '{"version":"bad","models":[{"model_id":"x","vram_per_sample":"-1Mi","cost_per_sample":"3ms","max_batch_samples":1}]}' > "$TMP/bad-spec.json"
code="$(call POST "/api/v1/models" "$TMP/bad-spec.json" "$TMP/bad-spec-response.json")"
expect_code "$code" "400" "negative model quantity rejected"
expect_contains "$TMP/bad-spec-response.json" '"code":"E_SPEC_INVALID"' "spec invalid business code"

printf '{"trace_id":"demo","version":"v1","requests":[{"request_id":"r1","arrival_ms":0,"model_id":"llm-small","samples":1,"max_batch_latency":"1ms","priority":1}]}' > "$TMP/conflicting-trace.json"
code="$(call POST "/api/v1/traces" "$TMP/conflicting-trace.json" "$TMP/conflicting.json")"
expect_code "$code" "409" "same trace/version with different content"
expect_contains "$TMP/conflicting.json" '"code":"E_CONFLICTING_REQUEST"' "conflict business code"

printf '== persistence and recovery facts ==\n'
code="$(call GET "/api/v1/recovery/report" "" "$TMP/recovery.json")"
expect_code "$code" "200" "startup recovery report"
expect_contains "$TMP/recovery.json" '"clean":true' "clean startup recovery report"
code="$(call GET "/api/v1/intents?limit=20" "" "$TMP/intents.json")"
expect_code "$code" "200" "intent log query"
expect_contains "$TMP/intents.json" '"kind":"COMPLETE_RUN"' "intent log has committed run intent"

printf '\n== result ==\n'
printf '  checks passed: %d, failed: %d\n' "$pass" "$failures"
printf '  artifacts: %s\n' "$TMP"
printf '  database: %s\n' "$DB"
if [[ "$failures" -ne 0 ]]; then exit 1; fi
printf '  demo: PASS\n'

