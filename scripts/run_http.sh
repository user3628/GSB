#!/usr/bin/env bash
# Start the local-only HTTP API (default 127.0.0.1:18765).
set -euo pipefail
cd "$(dirname "$0")/.."
HOST="${WB_HOST:-127.0.0.1}"
PORT="${WB_PORT:-18765}"
WORKSPACE="${WB_WORKSPACE:-workspaces/http_default}"
exec .venv/bin/python -m depworkbench.http_app --host "$HOST" --port "$PORT" \
    --workspace "$WORKSPACE"
