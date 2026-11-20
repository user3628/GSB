#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -x .venv/bin/detdiff ]]; then
  echo "Missing .venv; run scripts/setup.sh first" >&2
  exit 1
fi

exec .venv/bin/detdiff demo --workspace demo_workspace --reset "$@"

