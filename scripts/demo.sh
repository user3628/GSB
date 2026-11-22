#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ ! -x .venv/bin/python ]]; then
  echo "Virtual environment missing. Run scripts/setup.sh first." >&2
  exit 1
fi
exec .venv/bin/python scripts/e2e_demo.py "$@"
