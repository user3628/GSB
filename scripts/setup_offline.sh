#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

"${PYTHON_BIN}" - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("Python 3.10 or newer is required")
print(f"Using Python {sys.version.split()[0]}")
PY

if [[ ! -d "${ROOT}/.venv" ]]; then
  "${PYTHON_BIN}" -m venv "${ROOT}/.venv"
fi

"${ROOT}/.venv/bin/python" -m pip install \
  --no-index --find-links "${ROOT}/wheelhouse" \
  -e "${ROOT}" pytest

echo "Offline environment ready: ${ROOT}/.venv"

