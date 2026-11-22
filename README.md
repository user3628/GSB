# Halu Pipeline

一个完全本地、确定性、可增量执行的 LLM 评测与幻觉检测管线。它围绕两个核心问题构建：**交互查询延迟预算**与**顺序/并行/增量/恢复结果逐字节一致**。管线不调用真实模型、API 或 GPU，只使用确定性规则、字符特征和 SHA-256 哈希特征。

## 能力概览

- 四阶段处理：规范化与切分 → 逐样本一致性/幻觉判定 → 按 `source`/`lang` 分层汇总 → 标注一致性验证。
- NFKC、Unicode 空白折叠、全半角统一、小写化；中文单字与拉丁连续串分词，保留规范化文本字符偏移。
- 词项覆盖、字符 n-gram 覆盖、稳定哈希特征、来源先验在对数域合成；数字与否定规则参与确定性仲裁。
- 分层汇总使用按 `sample_id` 顺序的 `np.logaddexp` 累加，输出六位定点量化值；声明过的空分层不会被丢弃。
- 人工标注混淆统计、一致率、误判/漏判清单、标注覆盖率和源级复核建议。
- 阈值、证据权重、一致性下限编辑后的阶段级增量重算，并用从零顺序全量运行做字节一致性审计。
- 原子阶段产物、真实 `fcntl.flock` 文件锁、单调检查点序号、优雅停止与恢复。
- 样本依据与汇总查询的三元指纹缓存；线程硬超时返回 `E_LATENCY_BUDGET`。
- CLI、交互式 REPL、本地 HTTP API、固定种子演示数据与验收测试。

## 环境与安装

要求 Python 3.11+、NumPy、jsonlines。无需 Docker、GPU 或网络。

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
```

本工作区已实际安装 `numpy 2.2.1`、`jsonlines 4.0.0` 与可编辑包 `halu-pipeline 0.1.0`。

## 快速开始

生成固定种子的 3 个演示分片（20 条样本），然后执行完整验收演示：

```bash
python3 scripts/generate_samples.py --out-dir data/demo --seed 20261123 --shards 3
python3 scripts/demo.py
```

演示会在 `artifacts/demo-run` 留下可继续查询的运行目录，并写出：

- `artifacts/demo_report.json`：验收检查、增量结果、汇总、错误码和阶段哈希。
- `artifacts/demo_report_full.json`：包含四阶段完整产物的报告。

直接查询演示运行目录：

```bash
halu-pipeline status --run-dir artifacts/demo-run
halu-pipeline sample --run-dir artifacts/demo-run --sample-id mix-002 --budget-ms 1000
halu-pipeline summary --run-dir artifacts/demo-run
halu-pipeline validation --run-dir artifacts/demo-run
halu-pipeline export --run-dir artifacts/demo-run --output artifacts/export.json
```

## 数据格式

每个 JSONL 分片一行一个对象：

```json
{
  "sample_id": "mix-001",
  "prompt": "巴黎和卢浮宫在哪里？",
  "model_output": "巴黎是法国的首都，the louvre is located in paris.",
  "reference_answer": "法国巴黎",
  "retrieved_evidence": ["巴黎是法国的首都。", "The Louvre is located in Paris."],
  "lang": "mixed",
  "source": "wiki_mixed",
  "annotation": {"is_hallucination": false, "needs_review": false}
}
```

`annotation` 可省略；`is_hallucination`/`needs_review` 可为 `true`、`false` 或 `null`。演示数据覆盖：

- 中文、英文、中英混排；
- 空证据列表、空证据字符串、空 `model_output`；
- 人工标注缺失和 `needs_review=true`；
- 数字矛盾、否定矛盾、语义转述误判；
- 标注覆盖不足的 source；
- 两个声明但没有样本的空分层。

`data/demo/invalid_cases.json` 还固定记录了两个错误码演示输入：非法句支撑阈值与零延迟预算。

## 配置

默认配置位于 `src/halu_pipeline/config.py`。常用配置编辑：

```bash
halu-pipeline config --run-dir artifacts/demo-run \
  --set sentence_support_threshold=0.84 \
  --set hallucination_ratio_threshold=0.40 \
  --set evidence_weights.wiki_mixed=0.75 \
  --set consistency_min=0.82 \
  --set min_annotation_coverage=0.60 \
  --set parallel_workers=4
```

配置会在写入前完整校验。非法阈值返回 `E_INVALID_CONFIG`，不会修改旧配置。

## 分阶段与增量执行

```bash
halu-pipeline run --run-dir artifacts/demo-run --until-stage 1
halu-pipeline stop --run-dir artifacts/demo-run
halu-pipeline resume --run-dir artifacts/demo-run
halu-pipeline run --run-dir artifacts/demo-run
```

配置变化与最早失效阶段：

| 变化 | 最早失效阶段 |
| --- | --- |
| 输入内容变化 | 1 |
| `sentence_support_threshold`、`hallucination_ratio_threshold`、评分/证据权重、n-gram 参数 | 2 |
| `declared_strata` | 3 |
| `consistency_min`、`min_annotation_coverage` | 4 |
| 仅延迟预算/缓存容量/并行度 | 0（查询配置变化，无阶段失效） |

每次增量运行默认执行“从零顺序全量”审计。任一阶段与审计结果不是逐字节一致时，返回 `E_INCREMENTAL_MISMATCH`，并附 `different_stages` 与 `different_sample_ids`。

## 本地 HTTP API

```bash
halu-pipeline serve --run-dir artifacts/demo-run --host 127.0.0.1 --port 8765
```

主要端点：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/status` | 阶段状态、配置版本、阶段配置指纹、失效原因 |
| GET | `/summary` | 分层汇总（可选 `source`、`lang`、`budget_ms`） |
| GET | `/sample/{sample_id}` | 逐样本规范化、判定依据、复核与失效原因 |
| GET | `/validation` | 标注一致性验证与源级复核建议 |
| GET | `/checkpoints` | 单调检查点记录 |
| GET | `/report` | 完整报告 |
| POST | `/import` | 导入 JSONL：`{"shards":["..."]}` |
| POST | `/config` | 编辑配置：`{"assignments":["key=value"]}` |
| POST | `/run` | 执行：`{"until_stage":4,"verify_incremental":true}` |
| POST | `/stop` / `/resume` | 请求停止 / 恢复 |
| POST | `/query/sample` | 带 `sample_id`、`budget_ms`、`query_id` 的样本查询 |
| POST | `/query/summary` | 带 `budget_ms`、`query_id` 的汇总查询 |
| POST | `/cache/clear` | 清空查询缓存 |
| POST | `/checkpoint/publish` | 显式测试并发检查点发布 |

服务只允许绑定 loopback 地址。查询预算为 0 或超时返回 HTTP 408 与：

```json
{"code":"E_LATENCY_BUDGET","query_id":"...","budget_ms":0,"elapsed_ms":0.005,"elapsed_us":5}
```

## 实际验收结果

2026-11-23 在本工作区实际运行：

```text
python3 -m unittest discover -s tests -v
Ran 23 tests in 1.585s
OK

python3 scripts/demo.py
all_checks_pass: true  (25/25)
verification: PASS
incremental_matches_fresh_full_run: true
parallel_matches_sequential: true
stop_resume_matches_fresh: true
sample_cache_hit_byte_equal: true
sample_cache_recompute_byte_equal: true
summary_cache_recompute_byte_equal: true
evidence_weight_changes_score: true
latency_budget_error: true
invalid_threshold_error: true
```

最终演示汇总：

```text
样本 20，自动可判定 17，错误 3，幻觉 7，幻觉率 0.411765
日志域平均一致性 0.802946，待复核 7
空分层保留 2：kb_zh/ja、shadow_source/en
人工一致率 0.857143，TP 5 / TN 7 / FP 1 / FN 1
```

最终阶段产物 SHA-256：

```text
stage1 cddb47f27324f5af7c1d65159aac7ad48b09e5cabb85ad2b4c2b6c77488a33ae
stage2 6a66806a320b511be49abce6376e0d10d50e6a19f4bcc819ee333256c5a589e1
stage3 9eca9aed1d93a3f070ad5655231e411d21883f304ce62b740fc663c831821d57
stage4 be0257aafde4b306c817523c543a1a1ae1a4481f0c70ef3e90868f9a364899d9
```

详细算法、确定性不变量、缓存键和检查点协议见 `docs/DESIGN.md`。

逐项终端记录见 `artifacts/validation.log`；机器可读验收结果见 `artifacts/demo_report.json` 与 `artifacts/demo_report_full.json`。

## 错误码

| 错误码 | 含义 |
| --- | --- |
| `E_EMPTY_FIELD` | 输出或证据为空；作为逐样本记录错误返回 |
| `E_INVALID_CONFIG` | 阈值、权重、配置路径或类型非法 |
| `E_INVALID_RECORD` / `E_INVALID_JSONL` | 输入记录/分片格式非法 |
| `E_DUPLICATE_SAMPLE_ID` | 分片间 `sample_id` 重复 |
| `E_LATENCY_BUDGET` | 查询超时或预算为零 |
| `E_STAGE_NOT_READY` | 目标阶段尚未完成 |
| `E_SAMPLE_NOT_FOUND` | 样本不存在 |
| `E_STOPPED` | 停止请求已生效，可恢复 |
| `E_RUN_BUSY` | 另一进程持有运行锁 |
| `E_STALE_CHECKPOINT` | 检查点序号落后或重复 |
| `E_INCREMENTAL_MISMATCH` | 增量与从零全量结果不一致 |

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

测试覆盖规范化偏移与空字段、判定规则、分层复算与定点量化、空层保留、标注混淆、误判/漏判、增量阶段复用与字节一致性、并行等价、停止恢复、双进程检查点竞争、缓存重算字节一致、HTTP 非法阈值和延迟预算错误码。
