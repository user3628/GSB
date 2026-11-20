#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -z "${PYTHON:-}" ]; then
  if [ -x .venv/bin/python ]; then PYTHON=.venv/bin/python;
  elif [ -x .venv/Scripts/python.exe ]; then PYTHON=.venv/Scripts/python.exe;
  else PYTHON=python3; fi
fi
"$PYTHON" -m unittest discover -s tests -v
