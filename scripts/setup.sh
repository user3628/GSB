#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install pytest
.venv/bin/pip install -e .
.venv/bin/python scripts/generate_demo.py --data-dir data

