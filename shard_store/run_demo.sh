#!/usr/bin/env bash
# One-command local verification: init DB -> full test suite -> e2e demo.
set -euo pipefail
cd "$(dirname "$0")"

if [ -d "../.venv" ]; then
  # shellcheck disable=SC1091
  source ../.venv/bin/activate
fi

python -m scripts.init_db --reset
python -m pytest
python scripts/demo_e2e.py
