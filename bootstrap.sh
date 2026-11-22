#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3.11}"
if [[ ! -d .venv ]]; then
  "$PYTHON_BIN" -m venv .venv
fi

if command -v uv >/dev/null 2>&1; then
  uv pip install --python .venv/bin/python -r requirements.txt
  uv pip install --python .venv/bin/python -e .
else
  .venv/bin/python -m ensurepip --upgrade
  .venv/bin/python -m pip install --upgrade pip
  .venv/bin/python -m pip install -r requirements.txt
  .venv/bin/python -m pip install -e .
fi
.venv/bin/python scripts/generate_samples.py
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/demo_interop.py
