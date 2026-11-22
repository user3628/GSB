#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if command -v uv >/dev/null 2>&1; then
  uv python install 3.11
  uv venv --python 3.11 --allow-existing .venv
  uv pip install --python .venv/bin/python -r requirements.txt
elif command -v python3.11 >/dev/null 2>&1; then
  if [[ ! -x .venv/bin/python ]]; then
    python3.11 -m venv .venv
  fi
  .venv/bin/python -m pip install --upgrade pip
  .venv/bin/python -m pip install -r requirements.txt
else
  echo "Python 3.11 or uv is required" >&2
  exit 1
fi

.venv/bin/python -m svc_registry.cli generate-samples
echo "Environment ready. Run .venv/bin/pytest -q and .venv/bin/python scripts/e2e.py"
