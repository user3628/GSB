#!/usr/bin/env bash
# 资源编排依赖图诊断工作台 - POSIX 本地启动脚本（仅回环）。
set -euo pipefail
cd "$(dirname "$0")/.."
export GRAPH_BENCH_DB="${GRAPH_BENCH_DB:-$PWD/data/graph_bench.db}"
HOST="${GRAPH_BENCH_HOST:-127.0.0.1}"
PORT="${GRAPH_BENCH_PORT:-8080}"
exec python3 -m uvicorn graph_bench.api:app --host "$HOST" --port "$PORT"
