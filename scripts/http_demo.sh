#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m cmir_alignment.cli http-demo \
  --workspace runtime/http-demo-workspace \
  --data-dir data/sample \
  --port 0
