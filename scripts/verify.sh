#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-$ROOT/.venv311/bin/python}"

cd "$ROOT"
"$PY" -m pytest -q
"$PY" -m headworkbench --workspace "$ROOT" demo >/tmp/headworkbench-demo.json
"$PY" scripts/http_smoke.py
"$PY" scripts/write_verification.py

echo "Verification complete: artifacts/demo_report.json, artifacts/verification.json"

