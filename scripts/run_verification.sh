#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
.venv/bin/python scripts/run_experiment.py --output artifacts/verification_report.json
.venv/bin/python -m pytest -q
