#!/usr/bin/env bash
# 可重复执行的 CLI 演示：完整运行 / 优雅停止恢复 / 状态查询 / 验证。
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.venv/bin:$PATH"
OUT=demo_output/shell_demo
WS="$OUT/ws"
REF="$OUT/reference"

echo "== 0. 生成确定性样本 =="
.venv/bin/python -m mmpipe.samplegen_cli 2>/dev/null || true
.venv/bin/python -c "from mmpipe.samplegen import generate; generate('samples')"

for dirname in "$WS" "$REF"; do
  [ -d "$dirname" ] && find "$dirname" -mindepth 1 -delete
  mmpipe -w "$dirname" init >/dev/null
  mmpipe -w "$dirname" import >/dev/null
done

echo "== 1. 参考工作区完整运行 =="
mmpipe -w "$REF" run >/dev/null

echo "== 2. 演示工作区：完成 2 个单元后优雅停止 =="
mmpipe -w "$WS" run --unit-delay 0.1 --stop-after 2
mmpipe -w "$WS" status | python3 -c "import sys,json;s=json.load(sys.stdin);print('head_seq =',s['head_seq'])"

echo "== 3. 恢复并确认序号连续 =="
mmpipe -w "$WS" resume >/dev/null
mmpipe -w "$WS" status | python3 -c "import sys,json;s=json.load(sys.stdin);print('head_seq =',s['head_seq'])"

echo "== 4. 与完整运行逐字节比对 =="
for stage in text_encoding visual_alignment fusion_scoring aggregation; do
  mmpipe-verify compare "$REF/current/$stage.json" "$WS/current/$stage.json" \
    | python3 -c "import sys,json;print('$stage =', json.load(sys.stdin)['byte_identical'])"
done

echo "== 5. 验证工具整体检查 =="
mmpipe-verify -w "$WS" check | python3 -c "import sys,json;r=json.load(sys.stdin);print('verify ok =',r['ok'],'| checks =',len(r['checks']))"

echo "== 6. 查看聚合结论（冲突样本保留）=="
mmpipe -w "$WS" aggregate | python3 -c "import sys,json;a=json.load(sys.stdin);print('conflicts =',a['summary']['conflict_sample_ids']);print('counts =',a['summary']['conclusion_counts'])"
