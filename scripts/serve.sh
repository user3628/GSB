#!/usr/bin/env bash
# Start the local HTTP workbench (Linux/macOS). No GPU, no external services.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
cd "$ROOT"
exec .venv/bin/python -m uvicorn lora_bench.http_app:app --host "$HOST" --port "$PORT" --app-dir src
