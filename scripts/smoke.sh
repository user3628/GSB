#!/usr/bin/env bash
set -euo pipefail

base_url="${1:-http://127.0.0.1:18080}"
root="$(cd "$(dirname "$0")/.." && pwd)"

post_sample() {
  local path="$1" sample="$2"
  curl -fsS -X POST "${base_url}${path}" -H 'Content-Type: application/json' \
    --data-binary "@${root}/samples/${sample}"
}

policy="$(post_sample /api/v1/snapshots policy_v1.json)"
[[ "$policy" == *'"state_revision":1'* ]]

r1="$(post_sample /api/v1/checkpoints round1_increment.json)"
[[ "$r1" == *'"writes":['* ]]
[[ "$r1" == *'"deferred":['* ]]

r2="$(post_sample /api/v1/checkpoints round2_increment.json)"
[[ "$r2" == *'r1.attn.fp16'* && "$r2" == *'r1.mlp.fp16'* ]]

pools="$(curl -fsS "${base_url}/api/v1/pools")"
[[ "$pools" == *'"pool":"VRAM","used_bytes":2000'* ]]
[[ "$pools" == *'"pool":"PERSISTED","used_bytes":9376'* ]]

drift_file="$(mktemp)"
code="$(curl -sS -o "$drift_file" -w '%{http_code}' -X POST "${base_url}/api/v1/checkpoints" \
  -H 'Content-Type: application/json' --data-binary "@${root}/samples/round3_drift_increment.json")"
grep -q 'E_PRECISION_DRIFT' "$drift_file"
[[ "$code" == "422" ]]
rm -f "$drift_file"

r4="$(post_sample /api/v1/checkpoints round4_complete_increment.json)"
[[ "$r4" == *'"status":"WRITTEN"'* ]]
rounds="$(curl -fsS "${base_url}/api/v1/rounds?round=4")"
[[ "$rounds" == *'"status":"PASS"'* ]]

replay="$(post_sample /api/v1/checkpoints round1_increment.json)"
[[ "$replay" == *'"idempotent_replay":true'* ]]

policy2="$(post_sample /api/v1/snapshots policy_v2.json)"
[[ "$policy2" == *'"state_revision":5'* ]]

report="$(curl -fsS "${base_url}/api/v1/recovery-report")"
[[ "$report" == *'committed_sqlite_transactions_and_verified_blob_manifests'* ]]

echo "SMOKE PASS: write split, evictions, pool accounting, drift code, complete round, replay, policy version, recovery report"
