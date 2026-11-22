#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PORT="${PORT:-8765}"
if [[ ! -f demo/workspace/workspace.json ]]; then
  python -m telemetry_align --root demo/workspace init-demo
fi
python -m telemetry_align --root demo/workspace serve --host 127.0.0.1 --port "$PORT"

