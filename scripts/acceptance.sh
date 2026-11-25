#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATA_DIR="${DATA_DIR:-$ROOT/data}"
ARTIFACT_DIR="$ROOT/artifacts"
BIN="$ARTIFACT_DIR/feedbackstream"
mkdir -p "$ARTIFACT_DIR"

echo "== go version =="
go version

echo "== go test =="
go test ./...

echo "== build CLI =="
go build -o "$BIN" ./cmd/feedbackstream

echo "== deterministic demo: $DATA_DIR =="
"$BIN" demo --data-dir "$DATA_DIR" --reset > "$ARTIFACT_DIR/demo-report.json"
"$BIN" query windows --data-dir "$DATA_DIR" > "$ARTIFACT_DIR/window-results.json"
"$BIN" query shed --data-dir "$DATA_DIR" > "$ARTIFACT_DIR/shed-decisions.json"
"$BIN" query bp --data-dir "$DATA_DIR" > "$ARTIFACT_DIR/backpressure-events.json"

python3 - <<'PY'
import json
from pathlib import Path
r=json.loads(Path('artifacts/demo-report.json').read_text())
assert r['completed_batch_count'] == 9, r['completed_batch_count']
assert r['backpressure_drill']['blocked_report']['blocked_upstream_batches'] == 1
assert r['stale_base_error_code'] == 'E_STALE_BASE'
assert r['window_results_v1'] > 0 and r['window_results_v2'] > 0
assert r['final_recovery']['scanned_pending_intents'] == 0
print(f"demo OK: completed=9 v1_rows={r['window_results_v1']} v2_rows={r['window_results_v2']}")
PY

for mode in batch window cascade policy; do
  CRASH_DIR="$(mktemp -d "/tmp/feedbackstream-${mode}-crash.XXXXXX")"
  set +e
  "$BIN" crash --mode "$mode" --data-dir "$CRASH_DIR" > "$ARTIFACT_DIR/crash-${mode}.out" 2>&1
  code=$?
  set -e
  echo "crash ${mode}: exit=${code} dir=${CRASH_DIR}"
  if [[ "$code" -ne 137 ]]; then
    echo "expected SIGKILL exit code 137, got $code" >&2
    exit 1
  fi
  "$BIN" recover --data-dir "$CRASH_DIR" > "$ARTIFACT_DIR/recovery-${mode}.json"
  python3 - "$mode" "$ARTIFACT_DIR/recovery-${mode}.json" <<'PY'
import json, sys
mode, path = sys.argv[1], sys.argv[2]
r=json.load(open(path))
if mode == 'batch':
    assert len(r['rolled_back_batches']) == 1, r
elif mode == 'window':
    assert len(r['discarded_window_intents']) == 1, r
else:
    assert len(r['aborted_policy_switches']) == 1 and r['active_policy_version'] == 1, r
print(f"recovery {mode} OK: pending={r['scanned_pending_intents']}")
PY
done

echo "ACCEPTANCE OK"
