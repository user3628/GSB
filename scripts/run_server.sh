#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
args=()
if [[ -n "${TONELAB_HOST:-}" ]]; then args+=(--host "$TONELAB_HOST"); fi
if [[ -n "${TONELAB_PORT:-}" ]]; then args+=(--port "$TONELAB_PORT"); fi
if [[ -n "${TONELAB_WORKSPACE:-}" ]]; then args+=(--workspace "$TONELAB_WORKSPACE"); fi
exec .venv/bin/python -m tonelab.cli serve "${args[@]}" "${@}"

