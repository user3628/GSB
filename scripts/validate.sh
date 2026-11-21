#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m unittest discover -s tests -v
python3 -m cmir_alignment.cli demo \
  --workspace runtime/demo-workspace \
  --data-dir data/sample \
  --report artifacts/demo-report.json > artifacts/demo-console.json
python3 -m cmir_alignment.cli http-demo \
  --workspace runtime/http-demo-workspace \
  --data-dir data/sample \
  --port 0 > artifacts/http-demo-console.json
echo "validation complete: artifacts/demo-report.json, artifacts/http-demo-console.json"
