#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_ID="${1:-main}"
cd "$ROOT"
shift || true
exec .venv/bin/python -m stream_tts.cli --run-id "$RUN_ID" run --resume "$@"
