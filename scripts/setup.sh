#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-python3.11}"
"$PYTHON" -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install dnslib==0.9.26 pytest==9.2.0
.venv/bin/python -c "import sys, pytest; print('python', sys.version); print('pytest', pytest.__version__)"
