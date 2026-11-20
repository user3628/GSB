#!/usr/bin/env bash
# 演示：启动本地 HTTP 服务，完成导入/分析/查询/停止/恢复/导出
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
WS="workspace/demo_http"
PORT="${1:-8099}"
BASE="http://127.0.0.1:${PORT}"

python -m layout_order.cli --workspace "$WS" generate >/dev/null
python -m layout_order.http_app --host 127.0.0.1 --port "$PORT" \
  --workspace "$WS" >/tmp/layout_order_http.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT
sleep 1.5

call() { echo "-- $*"; curl -sS "$@"; echo; }

call "$BASE/api/health"
call -X POST -H 'Content-Type: application/json' -d '{}' "$BASE/api/samples/import"
call -X POST -H 'Content-Type: application/json' -d '{}' "$BASE/api/analyze"
call "$BASE/api/status?doc_id=doc_double_mixed"
call "$BASE/api/documents/doc_double_mixed/spanning"
call "$BASE/api/documents/doc_single_zh/result"
call -X POST "$BASE/api/stop"
call "$BASE/api/invalidations"
echo "HTTP 演示完成，日志：/tmp/layout_order_http.log"
