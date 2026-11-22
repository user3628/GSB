#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONFIG="${1:-config/default.json}"
mkdir -p var/logs

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

mapfile -t NODE_IDS < <(.venv/bin/python - "$CONFIG" <<'PY'
import sys
from grad_fault_lab.config import LabConfig
for node in LabConfig.load(sys.argv[1]).nodes:
    print(node.node_id)
PY
)

for node_id in "${NODE_IDS[@]}"; do
  .venv/bin/python -m grad_fault_lab.node_app \
    --config "$CONFIG" --node-id "$node_id" \
    >"var/logs/${node_id}.log" 2>&1 &
  pids+=("$!")
done

.venv/bin/python -m grad_fault_lab.injector_app --config "$CONFIG" \
  >"var/logs/injector.log" 2>&1 &
pids+=("$!")

.venv/bin/python -m grad_fault_lab.validator_app --config "$CONFIG" \
  >"var/logs/validator.log" 2>&1 &
pids+=("$!")

echo "Services started from $CONFIG (Ctrl-C to stop)."
wait

