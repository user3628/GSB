#!/usr/bin/env bash
# 演示：阶段间优雅停止 -> 恢复 -> 与从零全量运行逐字节一致
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="python -m layout_order.cli"
WS_RESUME="workspace/demo_resume"
WS_FULL="workspace/demo_full"

echo "== 1. 生成确定性样本 =="
$PY --workspace "$WS_RESUME" generate >/dev/null
$PY --workspace "$WS_FULL" generate >/dev/null

echo "== 2. 在 column 阶段后优雅停止（只跑一个文档） =="
$PY --workspace "$WS_RESUME" analyze --stop-after column \
  --doc-id doc_double_mixed

echo "== 3. 查询中断点状态（summary 应缺失） =="
$PY --workspace "$WS_RESUME" status --doc-id doc_double_mixed

echo "== 4. 恢复并完成全部文档 =="
$PY --workspace "$WS_RESUME" resume >/dev/null

echo "== 5. 从零全量构建对照工作区 =="
$PY --workspace "$WS_FULL" analyze >/dev/null

echo "== 6. 逐字节比对四个阶段产物与最终结果摘要 =="
python - "$WS_RESUME" "$WS_FULL" <<'PY'
import json, os, sys
from layout_order.hashing import canonical_json, digest_file
ws_a, ws_b = sys.argv[1], sys.argv[2]
stages = ("normalize", "column", "order", "summary")
ok = True
for doc_id in sorted(os.listdir(os.path.join(ws_a, "checkpoints"))):
    for idx, stage in enumerate(stages, start=1):
        pa = os.path.join(ws_a, "checkpoints", doc_id, f"stage-{idx}-{stage}.json")
        pb = os.path.join(ws_b, "checkpoints", doc_id, f"stage-{idx}-{stage}.json")
        same = digest_file(pa) == digest_file(pb)
        ok &= same
        print(f"{doc_id:32s} {stage:9s} byte-identical={same}")
print("RESULT:", "BYTE_IDENTICAL" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
PY
echo "== 演示完成 =="
