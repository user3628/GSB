#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

exec python3 -m rag_orchestrator.cli serve --host "${HOST:-127.0.0.1}" --port "${PORT:-8765}"
