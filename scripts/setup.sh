#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 --version
if [[ ! -d .venv ]]; then python3 -m venv .venv; fi
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest
echo "Setup complete. Run .venv/bin/python scripts/demo.py"

