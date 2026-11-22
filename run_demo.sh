#!/usr/bin/env bash
# Deterministic end-to-end demo: generate data, run, stop/recover, verify.
set -euo pipefail
cd "$(dirname "$0")"
PYTHON=${PYTHON:-python3}
DATA=${1:-data/demo}
RUN=${2:-runs/demo}

$PYTHON -c "import shutil; shutil.rmtree('$RUN', ignore_errors=True)"
echo "==> generating deterministic demo data into $DATA"
$PYTHON -m ragwb.cli generate --data-dir "$DATA"
echo "==> running the four-stage pipeline"
$PYTHON -m ragwb.cli run --data-dir "$DATA" --run-dir "$RUN"
echo "==> checkpoint/fingerprint status"
$PYTHON -m ragwb.cli status --data-dir "$DATA" --run-dir "$RUN" >/dev/null
echo "==> evidence chain (sess-s1-aurora turn 1)"
$PYTHON -m ragwb.cli evidence --data-dir "$DATA" --run-dir "$RUN" \
    --session-id sess-s1-aurora --turn 1 >/dev/null
echo "==> fixed run digest"
$PYTHON - "$RUN" <<'PY'
import json, sys
summary = json.load(open(f"{sys.argv[1]}/summary.json"))
print(summary["run_digest"])
PY
