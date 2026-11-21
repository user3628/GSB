#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-$ROOT/.venv/bin/python}"

"$PY" scripts/build_samples.py
"$PY" -m unittest discover -s tests -t . -v
bash scripts/demo_stop_resume.sh
bash scripts/demo_errors.sh
bash scripts/demo_interop.sh
"$PY" scripts/verify_all.py > /tmp/contract-validation.json
echo 'validation artifact written to artifacts/validation.json'

