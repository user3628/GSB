#!/usr/bin/env bash
# 可重复运行的演示脚本：导入、导出、回放、冲突回滚、快照切换与恢复演练。
set -euo pipefail
cd "$(dirname "$0")/.."

GW=http://127.0.0.1:8080
export GATEWAY_PORT=8080 DATA_DIR=data GATEWAY_ENABLE_DEBUG_CRASH=1
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

step() { echo; echo "=== $1 ==="; }

step "构建"
go build -o bin/gateway ./cmd/gateway
go build -o bin/backend ./cmd/backend
go build -o bin/gensamples ./cmd/gensamples

step "生成确定性样本"
./bin/gensamples samples

step "启动本地后端测试服务 (9001/9002)"
BACKEND_PORT=9001 BACKEND_NAME=backend-9001 ./bin/backend & PIDS+=($!)
BACKEND_PORT=9002 BACKEND_NAME=backend-9002 ./bin/backend & PIDS+=($!)

step "启动网关"
rm -rf data && mkdir -p data
./bin/gateway & PIDS+=($!)
sleep 1

step "1. 导入 JSONL 路由表 (v2, 声明截断规则 truncate)"
curl -s -X POST "$GW/api/import?trunc=truncate" --data-binary @samples/routes_v2.jsonl

step "2. 导入二进制路由表 (v2, 内容一致幂等跳过)"
curl -s -X POST "$GW/api/import" --data-binary @samples/routes_v2.bin

step "3. 导出两种格式"
curl -s "$GW/api/export?format=bin" -o /tmp/exp.bin
curl -s "$GW/api/export?format=jsonl" -o /tmp/exp.jsonl
echo "导出: $(wc -c < /tmp/exp.bin) bytes bin, $(wc -c < /tmp/exp.jsonl) bytes jsonl"

step "4. 旧版 v1 降级导入 (缺工具链 + 低精度显存 + 微秒时间戳舍入)"
curl -s -X POST "$GW/api/import" --data-binary @samples/legacy_v1.jsonl

step "5. 高版本 v3 拒绝裁决 (E_FORMAT_UNSUPPORTED)"
curl -s -X POST "$GW/api/import" --data-binary @samples/future_v3.jsonl

step "6. 损坏记录 (E_RECORD_CORRUPT 含偏移)"
curl -s -X POST "$GW/api/import" --data-binary @samples/corrupt_crc.bin
curl -s -X POST "$GW/api/import" --data-binary @samples/corrupt_len.bin

step "7. 显存溢出 (E_UNIT_OVERFLOW)"
curl -s -X POST "$GW/api/import" --data-binary @samples/overflow.jsonl

step "8. 冲突导入：高工具链覆盖，平局拒绝整批回滚 (E_CONFLICTING_IMPORT)"
curl -s -X POST "$GW/api/import" --data-binary @samples/override.jsonl
curl -s -X POST "$GW/api/import" --data-binary @samples/conflict.jsonl
echo "回滚后 llm-omega 不应存在:"
curl -s "$GW/api/route/llm-omega"

step "9. 确定性回放 (seed=42, 两次逐条一致)"
curl -s -X POST "$GW/api/replay" -d '{"seed":42,"count":8}' > /tmp/replay1.json
curl -s -X POST "$GW/api/replay" -d '{"seed":42,"count":8}' > /tmp/replay2.json
if diff -q /tmp/replay1.json /tmp/replay2.json >/dev/null; then echo "回放确定性校验: PASS"; else echo "回放确定性校验: FAIL"; exit 1; fi
cat /tmp/replay1.json

step "10. 命中统计"
curl -s "$GW/api/stats"

step "11. 快照校验：非法序号与不兼容版本被拒绝 (E_SNAPSHOT_INVALID)"
curl -s -X POST "$GW/api/snapshot" -d '{"path":"samples/snapshot_stale.json"}'
curl -s -X POST "$GW/api/snapshot" -d '{"path":"samples/snapshot_badver.json"}'
echo "合法快照切换:"
curl -s -X POST "$GW/api/snapshot" -d '{"path":"samples/snapshot_1.json"}'

step "12. 切换后路由与命中统计"
curl -s "$GW/api/stats"
curl -s -X POST "$GW/api/replay" -d '{"seed":42,"count":4}'

step "13. 后端不可达 (E_BACKEND_UNAVAILABLE 附端点标识)"
kill "${PIDS[0]}" 2>/dev/null || true
sleep 0.3
curl -s -X POST "$GW/api/forward" -d '{"model_id":"llm-sigma","payload":"x"}'

step "14. 强制终止恢复演练"
curl -s -X POST "$GW/api/debug/crash" || true
sleep 1
./bin/gateway & PIDS+=($!)
sleep 1
curl -s "$GW/api/recovery"
curl -s "$GW/api/route/llm-alpha"

step "演示完成"
