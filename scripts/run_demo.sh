#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -x .venv/bin/python ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi
python scripts/check_ports.py --http-port 18765
python -m vgw demo --output artifacts/demo/current

