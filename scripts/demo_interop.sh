#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-$ROOT/.venv/bin/python}"

"$PY" -m contract_elements.cli import-v1 \
  --output output/demo/interop/annotations_v1.export.jsonl \
  --internal-output output/demo/interop/internal_v2.json > /tmp/contract-interop.json
"$PY" -m contract_elements.cli export-v1 \
  --internal-input output/demo/interop/internal_v2.json \
  --output output/demo/interop/annotations_v1.reexport.jsonl > /tmp/contract-reexport.json
cmp output/demo/interop/annotations_v1.export.jsonl output/demo/interop/annotations_v1.reexport.jsonl
cat /tmp/contract-interop.json
echo 'export -> import -> export is byte-identical'

