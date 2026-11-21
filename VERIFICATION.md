# 实际验证记录

验证日期：2026-11-22  
工作目录：`/home/zsl/feedback/uid37fecb20-target-mwnkf3ge-88`  
运行环境：Python 3.14.4、Flask 3.1.3、numpy 2.5.5、PyYAML 6.0.3、pytest 9.2.0  
约束：全程使用本地样本，无 Docker、无外部 HTTP 访问、无 LLM/向量库/GPU。

## 1. 自动化测试

```bash
python3 -m pytest -q
```

实际结果：

```text
...........................................                              [100%]
43 passed
```

覆盖内容：

- 固定种子样本逐字节重建、分片/重复文档/异常请求结构。
- NFKC、全半角、空白折叠、中文单字/拉丁连续串切分。
- 空查询、超子查询上限、无词项重叠错误及“不写半成品检查点”。
- 检索分对数域分量、重复 `doc_id` 去重与来源分片记录。
- 注入超大权重后的 `E_NUMERIC`。
- 固定点全局/通道配额、预算降级与全拒错误。
- 越界配额配置的稳定 `E_CONFIG_INVALID`。
- 证据为零、最小片段无法放入字符预算、引用全覆盖。
- MMR 重叠与跨子查询重复文档的舍弃原因记录。
- 并行/串行稳定投影逐字节一致，缓存复用后仍一致。
- 停止后恢复的最终摘要与从零运行一致，检查点序号 `1,2,3,4` 连续。
- 同义词、分解规则、文档、检索权重、预算、重排权重、证据条数、答案模板的增量失效。
- 延迟预算、工作线程异常保留已完成结果、阶段不推进。
- 原子写失败、孤立记录恢复、文件锁并发发布、阶段跳转/重复/跨请求错位。
- 多进程同时发布时真实 `flock` 只允许一个连续序号获胜。
- Flask 工作台/API、CLI 稳定输出。

## 2. 固定样本摘要

生成命令：

```bash
python3 -m rag_orchestrator.cli generate --out data
```

样本计数：

```text
seed             20261122
document records 9
unique doc_id    8
shards           shard-01, shard-02, shard-03
requests         7
```

关键文件 SHA-256：

```text
config.yaml                ce86c17fda56ee470d8a5a51c8c0f56580fbd4c443c5d835b378eb2b0cca3da1
docs.jsonl                 02075f6ee6fbbf27e465a6e3b18d8a0d2f3b8e5f38f0dde9458152d12e5fc128
requests.jsonl             4aef365702e87253bbb1d58e0ed9cde84b1bc3113ba3c3ea24a803e2e6952aa4
shards/shard-01.jsonl      b3f73d190aebe9886242981a76ba7930b9398c87fc025ccf055ccd32e191ac0f
shards/shard-02.jsonl      815f661f72c9ecd1154daa2129d5fc6a97abfed382fcce424f68bb570fe8f1b0
shards/shard-03.jsonl      f898ca35a891588a271c13b6af3e1117e9bd705ec748a35aa0e862e2a297ae65
```

`doc-003` 在 `shard-02` 与 `shard-03` 重复。`q-002` 的实际候选与引用中只保留一个 `doc-003`，并记录：

```json
{"doc_id": "doc-003", "source_shards": ["shard-02", "shard-03"], "duplicate_record_count": 2}
```

## 3. 并行与串行逐字节一致

命令（分别使用独立临时工作区）：

```bash
python3 -m rag_orchestrator.cli --workspace /tmp/rag-allp run-all --mode parallel --stable
python3 -m rag_orchestrator.cli --workspace /tmp/rag-allser run-all --mode serial --stable
```

实际共同摘要：

```text
run-all stable summary_sha256 = 3d148e3a760f41c9ce14df08af1bbe7a479090176aaf5d408326ddfc9650b26b
```

7 条请求的固定结果：

```text
q-001 ok    answer c04998ccfae4edeab3aa6bc2cbaf8148620f408e181cf6e1d8a3054adfafa771 expected_doc_hit=True
q-002 ok    answer 1b06596a392c8419f228e492c2188926e3b50f4a18b1a9a5e5704598a2afa5f3 expected_doc_hit=True
q-003 ok    answer 607bf02b8589036d46fd431247b8d72b9d013ca0c8e62c9f4d1e455bca842d4f expected_doc_hit=True
q-004 ok    answer 49625bd02de0cd50b485a70ddd28fb4a46968780c8d094607c78899d3b560653 expected_doc_hit=True
q-005 ok    answer 9337eebdb16a076c328e5f5a3b2a460b379dd0399ddc05d52681c3f54e975c52 expected_doc_hit=True
q-006 error E_QUERY_INVALID
q-007 error E_QUERY_INVALID
```

`run-all.status=partial` 仅因为 `q-006`/`q-007` 是故意注入的异常样本；5 条正常请求均完成。

## 4. 停止与恢复

在独立工作区先于 `dispatch` 完成后停止，再恢复：

```bash
python3 -m rag_orchestrator.cli --workspace /tmp/resume run q-001 --stop-after-stage dispatch
python3 -m rag_orchestrator.cli --workspace /tmp/resume run q-001 --stable
```

实际结果：

```text
停止后完成阶段         normalize, dispatch
停止后检查点序号       1, 2
恢复后新增检查点       3 (rerank), 4 (assemble)
恢复记录 (stage, seq, stage_seq)
  normalize 1 1
  dispatch  2 1
  rerank    3 1
  assemble  4 1
从零运行 stable SHA    018083d0b77a1757bd0c3f5d21149de0bd29e35ea2d2ab23eee021b621bf2728
恢复运行 stable SHA    018083d0b77a1757bd0c3f5d21149de0bd29e35ea2d2ab23eee021b621bf2728
逐字节一致             true
```

`q-001` 固定答案摘要：`c04998ccfae4edeab3aa6bc2cbaf8148620f408e181cf6e1d8a3054adfafa771`。

## 5. 版本失效与增量重建

在已有完整 `q-001` 检查点的工作区修改 `rerank.length_fit_weight` 后重跑：

```text
invalidation.earliest_stage = rerank
cache_usage                 normalize=reused, dispatch=reused, rerank=rebuilt, assemble=rebuilt
新增检查点                  rerank seq=5 stage_seq=2, assemble seq=6 stage_seq=2
原检查点                    normalize seq=1, dispatch seq=2, rerank seq=3, assemble seq=4
```

这验证了：上游未受影响产物继续复用；最早受影响阶段及其全部下游被强制失效；请求级检查点序号跨 epoch 连续推进。

自动化测试还分别覆盖：

```text
同义词表 / 分解规则变化      -> normalize 最早失效
文档内容 / 检索权重变化      -> dispatch  最早失效
预算配置变化                -> dispatch  最早失效
重排权重 / 证据条数变化      -> rerank    最早失效
答案模板变化                -> assemble  最早失效
```

每阶段 `stage_fingerprint`、分段配置指纹和失效原因均通过查询结果、`/api/diagnostics/<query_id>` 与 `runtime/fingerprints/<query_id>.json` 可观测。

## 6. 预算裁决与延迟

注入较小的全局配额（临时配置，不改动样本默认值）：

```text
global_retrieval_quota = 130
status                 = degraded
accepted_count         = 1
rejected_count         = 3
global_used / quota    = 120 / 130
rejected reasons       = global_quota, global_quota, global_quota
```

全部子查询被拒：

```text
global_retrieval_quota = 1
status                 = error
error.code             = E_BUDGET_EXHAUSTED
accepted_subquery_count= 0
已落盘阶段             = normalize
```

实际延迟注入通过单调时钟推进 60 ms/次、预算 100 ms 完成，不使用 sleep 或线程超时：

```json
{
  "code": "E_LATENCY_BUDGET",
  "details": {
    "stage": "dispatch",
    "elapsed_ms": 120.0,
    "latency_budget_ms": 100.0,
    "accepted_subquery_count": 4
  }
}
```

dispatch 阶段未落盘，仅保留已完成的 normalize 检查点。

## 7. 数值与证据错误

检索权重注入 `1.7e308` 后计算溢出：

```text
status        = error
error.code    = E_NUMERIC
query_id      = q-001
doc_id        = doc-001
已落盘阶段    = normalize
```

证据条数设为 0：

```text
status      = error
error.code  = E_EVIDENCE_EMPTY
query_id    = q-001
已落盘阶段  = normalize, dispatch
```

字符预算设为 20 且最小片段为 24 时，所有较长候选均无法放入，同样返回 `E_EVIDENCE_EMPTY`，错误细节含 `reason=no_selectable_evidence`。

## 8. 查询异常

```text
q-006 空查询      E_QUERY_INVALID  subquery_index=0  reason=empty_query
q-007 12 个分句   E_QUERY_INVALID  subquery_index=8  reason=too_many_subqueries
```

`q-007` 的 `max_subqueries=8`，第 9 个位置（零基 `subquery_index=8`）触发错误。两个异常请求均未创建任何阶段检查点。

## 9. HTTP 服务实测

验证时端口 `47017` 可用，使用同一 CLI 启动：

```bash
python3 -m rag_orchestrator.cli serve --host 127.0.0.1 --port 47017 --workspace /tmp/rag-http-demo
```

实际请求与结果：

```text
GET  /api/health
{"engine_version":"1.0.0","offline":true,"status":"ok"}

GET  /
HTTP 200, 4247 bytes, contains "并行编排与预算裁决工作台"

POST /api/run {"query_id":"q-001","mode":"parallel"}
status       = ok
answer SHA   = c04998ccfae4edeab3aa6bc2cbaf8148620f408e181cf6e1d8a3054adfafa771
stable SHA   = 018083d0b77a1757bd0c3f5d21149de0bd29e35ea2d2ab23eee021b621bf2728
evidence     = doc-001, doc-003, doc-005, doc-002
budget       = accepted 4, used 484 / 900

GET /api/checkpoints/q-001
records      = normalize#1, dispatch#2, rerank#3, assemble#4
```

## 10. 复现命令

```bash
python3 -m rag_orchestrator.cli generate --out data
python3 -m pytest -q
scripts/run_server.sh
```

完整测试与演示均只读取本地 `data/`、写入本地 `runtime/` 或临时目录。
