#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m venv .venv-bootstrap
./.venv-bootstrap/bin/python -m pip install --quiet uv
./.venv-bootstrap/bin/uv python install 3.11
./.venv-bootstrap/bin/uv venv --python 3.11 .venv311
./.venv-bootstrap/bin/uv pip install --python .venv311/bin/python -r requirements.txt
echo "Environment ready: $ROOT/.venv311/bin/python"
