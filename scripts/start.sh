#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)'
if [[ ! -x .venv/bin/python ]]; then
  python -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --disable-pip-version-check -r requirements.txt
python scripts/check_ports.py --http-port 18765
exec python -m vgw serve --host 127.0.0.1 --port 18765 --db artifacts/vgw.db

