#!/usr/bin/env bash
set -euo pipefail

if command -v uv >/dev/null 2>&1; then
  uv python install 3.11
  uv venv .venv --python 3.11
  uv pip install --python .venv/bin/python -e '.[dev]'
else
  python3.11 -m venv .venv
  .venv/bin/python -m pip install --upgrade pip setuptools wheel
  .venv/bin/python -m pip install -e '.[dev]'
fi

mkdir -p artifacts
.venv/bin/python -m prep_pipeline.cli init
.venv/bin/python -m prep_pipeline.cli bootstrap
echo "Setup complete. Run: .venv/bin/python -m prep_pipeline.cli serve"
