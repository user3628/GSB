#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m cmir_alignment.cli generate \
  --output data/sample \
  --seed 20261122 \
  --shards 3 \
  --items-per-shard 6 \
  --write-configs configs

python3 -m cmir_alignment.cli demo \
  --workspace runtime/demo-workspace \
  --data-dir data/sample \
  --report artifacts/demo-report.json
