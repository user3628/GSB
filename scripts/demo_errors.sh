#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-$ROOT/.venv/bin/python}"

sha256sum config/default.json > /tmp/config-before.sha
"$PY" -m contract_elements.cli batch \
  --output output/demo/injected \
  --runtime output/demo/injected-runtime \
  --injections data/injections.json > /tmp/contract-injected.json
sha256sum config/default.json > /tmp/config-after.sha
cmp /tmp/config-before.sha /tmp/config-after.sha
"$PY" - <<'PY'
import json
from pathlib import Path
for line in Path("output/demo/injected/errors.jsonl").read_text(encoding="utf-8").splitlines():
    error = json.loads(line)
    print(error["code"], error["stage"], error["contract_id"], error["details"].get("element", ""))
PY

