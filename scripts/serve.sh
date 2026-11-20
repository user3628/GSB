#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec .venv/bin/python -m stream_tts.cli serve --host 127.0.0.1 --port "${PORT:-18765}"
