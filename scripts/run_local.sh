#!/usr/bin/env bash
# Local runner: generate data, run CLI pipeline, run tests, (optionally) serve.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA_DIR=${DATA_DIR:-data}
WORKSPACE=${WORKSPACE:-workspaces/local}
CONFIG=${CONFIG:-$DATA_DIR/configs/config_v2.json}

case "${1:-all}" in
  data)
    python3 scripts/gen_data.py --out "$DATA_DIR"
    ;;
  run)
    python3 -m evalbench run --data "$DATA_DIR/shards" --config "$CONFIG" --workspace "$WORKSPACE"
    ;;
  status)
    python3 -m evalbench status --data "$DATA_DIR/shards" --config "$CONFIG" --workspace "$WORKSPACE"
    ;;
  test)
    python3 -m pytest tests/ -q
    ;;
  serve)
    exec python3 -m evalbench serve --host 127.0.0.1 --port "${PORT:-8077}" \
      --data "$DATA_DIR/shards" --config "$CONFIG"
    ;;
  all)
    python3 scripts/gen_data.py --out "$DATA_DIR"
    python3 -m evalbench run --data "$DATA_DIR/shards" --config "$CONFIG" --workspace "$WORKSPACE"
    python3 -m pytest tests/ -q
    ;;
  *)
    echo "usage: $0 {data|run|status|test|serve|all}" >&2
    exit 64
    ;;
esac
