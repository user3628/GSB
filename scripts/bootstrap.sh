#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -x .venv/bin/python ]] || ! .venv/bin/python -c 'import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)' >/dev/null 2>&1; then
  rm -rf .venv
  if command -v python3.11 >/dev/null 2>&1; then
    python3.11 -m venv .venv
  elif command -v uv >/dev/null 2>&1; then
    uv python install 3.11
    uv venv --python 3.11 --seed .venv
  else
    echo "Python 3.11 or uv is required to create the isolated runtime." >&2
    echo "Install Python 3.11 or uv, then rerun this script." >&2
    exit 1
  fi
fi

.venv/bin/python -m pip install --disable-pip-version-check -r requirements.txt

echo "Python: $(.venv/bin/python --version)"
echo "numpy:  $(.venv/bin/python -c 'import numpy; print(numpy.__version__)')"
echo "Pillow: $(.venv/bin/python -c 'import PIL; print(PIL.__version__)')"

