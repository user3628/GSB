#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DB_PATH="${1:-var/h2workbench.db}"
PORT="${2:-8765}"

if [[ ! -x .venv/bin/python ]]; then
  "$ROOT/scripts/bootstrap.sh"
fi

exec .venv/bin/python -m h2workbench.server --db "$DB_PATH" --host 127.0.0.1 --port "$PORT"
