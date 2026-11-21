#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python -m tonelab.cli demo --data-dir data --workspace workspace/demo --output artifacts/baseline_summary.json "${@}"

