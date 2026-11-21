#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-$ROOT/.venv/bin/python}"

"$PY" -m contract_elements.cli batch --output output/demo/full --runtime output/demo/full-runtime > /tmp/contract-full.json
"$PY" -m contract_elements.cli batch --output output/demo/resume --runtime output/demo/resume-runtime --stop-after-units 7 > /tmp/contract-stopped.json
"$PY" -m contract_elements.cli resume --output output/demo/resume --runtime output/demo/resume-runtime > /tmp/contract-resumed.json

for file in results.jsonl candidates.jsonl conflicts.jsonl errors.jsonl summary.json experiment_summary.json; do
  cmp "output/demo/full/$file" "output/demo/resume/$file"
done
printf 'stopped: '
cat /tmp/contract-stopped.json
printf 'resumed: '
cat /tmp/contract-resumed.json
echo 'all six final artifacts are byte-identical'

