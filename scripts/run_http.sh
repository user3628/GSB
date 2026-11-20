#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
exec "$ROOT/.venv/bin/python" -m transcript_workbench --root "$ROOT" --workspace "${WORKBENCH_WORKSPACE:-artifacts/workspace}" serve --host "${WORKBENCH_HOST:-127.0.0.1}" --port "${WORKBENCH_PORT:-8765}" --exit-on-stop "$@"
