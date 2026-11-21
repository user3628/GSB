#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-python3.14}"

if ! compgen -G "vendor/numpy-*.whl" > /dev/null; then
  "$PYTHON" scripts/build_numpy_wheel.py
fi

"$PYTHON" -m venv .venv
"$ROOT/.venv/bin/python" -m pip install --no-index --find-links "$ROOT/vendor" "numpy>=2.0,<3"
"$ROOT/.venv/bin/python" -m pip install --no-index --no-build-isolation --force-reinstall --no-deps "$ROOT"
"$ROOT/.venv/bin/python" -c 'import numpy; print("NumPy", numpy.__version__, "ready")'

echo "Activate with: source .venv/bin/activate"

