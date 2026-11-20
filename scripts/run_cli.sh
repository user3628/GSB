#!/usr/bin/env bash
# Convenience wrapper: run the workbench CLI inside the local virtualenv.
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python -m depworkbench.cli "$@"
