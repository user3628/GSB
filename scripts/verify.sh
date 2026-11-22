#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

cd "$ROOT"
python3 -m pytest -q

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 -m multiscale_fusion demo \
  --dir "$TMP_DIR/demo" \
  --force \
  --verify-full > "$TMP_DIR/demo_run.json"

python3 - "$TMP_DIR/demo_run.json" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
run = payload["run"]
assert run["status"] == "completed"
assert run["verification"]["result"] == "incremental_matches_full_rebuild"
assert run["sensitivity"]["additivity_ok"] is True
print(json.dumps({
    "status": run["status"],
    "head_sequence": run["head_sequence"],
    "final_sha256": run["final_sha256"],
    "sensitivity_ranking": run["sensitivity"]["replay_ranking"],
    "verification": run["verification"]["result"],
}, indent=2, sort_keys=True))
PY
