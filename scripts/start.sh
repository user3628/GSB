#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ ! -x .venv/bin/python ]]; then
  echo "Virtual environment missing. Run scripts/setup.sh first." >&2
  exit 1
fi
export DIAGLAB_DB="${DIAGLAB_DB:-$ROOT/data/diaglab.sqlite3}"
exec .venv/bin/python -m uvicorn diaglab.app:app --host 127.0.0.1 --port "${DIAGLAB_PORT:-8000}"
