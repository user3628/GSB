# 本地 RAG 并行编排与资源预算裁决工作台

这是一个完全离线、确定性的检索增强生成（RAG）编排实现。它不调用真实 LLM、不依赖向量库、GPU 或任何线上资源；答案由本地模板与证据片段拼接生成。项目同时提供命令行入口、Flask HTTP API、阶段指纹/预算/检查点诊断接口和一个无外部前端依赖的可视化工作台。

核心验收目标：

- 四阶段执行：规范化与子查询分解 → 预算裁决与并行检索 → 重排与证据拼接 → 答案组装与引用归属。
- 全局固定点配额、单通道配额、单请求延迟预算同时生效；超预算显式降级或返回稳定错误码。
- 并行线程池结果与串行结果逐字节一致。
- 每阶段原子检查点、真实文件锁、停止后恢复、连续序号与版本增量失效。
- 使用固定种子生成中英混排、跨分片重复文档和异常请求样本。

## 快速开始

环境要求：Python 3.10+，本地可安装 `Flask`、`numpy`、`PyYAML` 与 `pytest`。项目不使用 Docker，也不会访问外部服务。

```bash
python3 --version
python3 -m pip install -r requirements.txt

# 可选：安装本地 CLI 入口（不访问网络）
python3 -m pip install -e . --no-deps

# 用固定种子 20261122 重建样本
python3 -m rag_orchestrator.cli generate --out data

# 运行单条请求，输出完整结果
python3 -m rag_orchestrator.cli --workspace runtime run q-001

# 只输出与缓存/耗时无关的稳定投影，可用于逐字节比较
python3 -m rag_orchestrator.cli --workspace runtime run q-001 --stable

# 全量运行
python3 -m rag_orchestrator.cli --workspace runtime run-all --mode parallel
python3 -m rag_orchestrator.cli --workspace runtime run-all --mode serial
```

启动本地 HTTP 工作台：

```bash
scripts/run_server.sh
# 浏览器打开 http://127.0.0.1:8765/

# 也可指定端口/工作区
python3 -m rag_orchestrator.cli serve --host 127.0.0.1 --port 8765 --workspace runtime
```

请求体示例：

```bash
curl -sS -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' \
  -d '{"query_id":"q-001","mode":"parallel"}'
```

## 样本与配置

`python3 -m rag_orchestrator.cli generate --out data` 会生成：

- `data/docs.jsonl`：合并后的文档分片，共 9 条记录、8 个唯一 `doc_id`。
- `data/shards/shard-01.jsonl`、`shard-02.jsonl`、`shard-03.jsonl`：3 个来源分片。
- `data/requests.jsonl`：7 条请求，其中 `q-001` 至 `q-005` 为正常请求，`q-006` 为空查询，`q-007` 为超长查询。
- `data/config.yaml`：规范化、检索、预算、重排、证据和组装配置。
- `data/manifest.json`：固定种子、文件 SHA-256 与样本计数。

`doc-003` 在 `shard-02` 和 `shard-03` 中重复出现；检索阶段按 `doc_id` 去重，并把两个来源分片记录在候选与引用中。样本同时覆盖中英查询、全半角输入和跨阶段版本失效。

固定样本的可重建摘要记录在 [`VERIFICATION.md`](VERIFICATION.md)。

## 四阶段引擎

### 1. 查询规范化与分解

实现位于 `rag_orchestrator/normalize.py`：

- 先做 Unicode NFKC、全半角统一与空白折叠。
- 中文按单字切分，拉丁字母/数字连续串作为一个词项。
- 基于配置的 `decomposition_rules` 与本地 `synonyms` 生成带优先级的子查询。
- 空查询、词项为空、子查询数超过 `max_subqueries`、子查询与原文/同义词闭包无词项交集时返回 `E_QUERY_INVALID`，并携带 `query_id`、`subquery_index`。
- 规范化在写入任何检查点之前完成，因此非法查询不会留下半成品阶段记录。

### 2. 预算裁决与并行检索

实现位于 `rag_orchestrator/budget.py` 与 `rag_orchestrator/retrieval.py`：

- 每个子查询先按固定点 `credits` 预估消耗：`cost_per_subquery + cost_per_doc_shard * shard_count + cost_per_term * term_count`。
- 全局配额、每通道配额和通道数三者同时约束接纳；被拒绝的子查询进入 `budget.rejected` 并带原因，不会被阻塞等待。
- 通道分配按确定性的最小负载优先进行；线程池按通道并发，每个通道内部按优先级执行。
- 检索分全部在对数域累加：`lexical_log + title_log + source_log + length_log`，输出每个分量。
- 重复 `doc_id` 按最佳记录合并，`source_shards` 保留所有来源分片。
- 任一分数为 `NaN`/`Inf` 时返回 `E_NUMERIC`，携带 `query_id` 与 `doc_id`，不会静默丢弃候选。

### 3. 重排与证据拼接

实现位于 `rag_orchestrator/evidence.py`：

- 重排分由检索分、查询词项覆盖率和片段长度贴合度组成。
- 按子查询优先级与片段排序位进行确定性全局归约。
- 使用最大边际相关（MMR）相似度阈值去除重复片段，并记录 `discarded_evidence` 及原因（跨子查询重复文档、MMR 重叠、字符预算、最小片段不足、证据条数上限）。
- 证据条数为零、字符预算不足、无可用最小片段时返回 `E_EVIDENCE_EMPTY`。

### 4. 答案组装与引用

实现位于 `rag_orchestrator/assembly.py`：

- 使用本地中/英模板拼接证据片段，不使用 LLM。
- 每条证据获得稳定引用编号 `[1]`、`[2]`……并校验引用 `doc_id` 覆盖全部证据。
- 输出 `summary_sha256`、`answer_sha256`、引用覆盖检查与期望引用命中结果。

## 预算与确定性语义

`budget` 配置默认值：

```yaml
budget:
  global_retrieval_quota: 900
  per_channel_quota: 320
  max_concurrent_channels: 3
  latency_budget_ms: 5000.0
  cost_per_subquery: 20
  cost_per_doc_shard: 8
  cost_per_term: 2
```

要点：

- 配额是“每请求”的全局配额；同一请求内任何时刻累计消耗不超过 `global_retrieval_quota`。
- 单通道累计消耗不超过 `per_channel_quota`，并发通道数不超过 `max_concurrent_channels`。
- 延迟预算发生在完成一个不可拆分阶段后，用真实单调时钟测量；没有睡眠、线程超时或伪造终止。
- 超延迟返回 `E_LATENCY_BUDGET`，包含 `elapsed_ms` 与已接纳子查询数。
- 全部子查询被拒时返回 `E_BUDGET_EXHAUSTED`；部分被拒时请求继续完成，结果为 `degraded`。
- `mode=parallel` 与 `mode=serial` 的检索/重排/组装产物完全一致；`stable_projection` 排除了实际耗时与缓存命中状态，因此可做逐字节比较。

## 检查点、锁与恢复

工作区默认在 `runtime/`，结构如下：

```text
runtime/
  checkpoints/<query_id>/
    ledger.json
    normalize/00000001.json
    dispatch/00000002.json
    rerank/00000003.json
    assemble/00000004.json
  fingerprints/<query_id>.json
  outputs/<query_id>.json
  locks/checkpoints.lock
```

每条记录包含：

- `query_id`、`stage`、`stage_index`
- `seq`（请求级单调连续序号）与 `stage_seq`（阶段内序号）
- `input_fingerprint`、`component_fingerprints`、`stage_fingerprint`
- `budget_config_version`
- `product_summary`、`product_sha256`、完整 `product`

一致性机制：

- 检查点先写入同目录临时文件，`flush + fsync` 后用 `os.replace` 原子发布，再 `fsync` 目录；不存在半写目标文件。
- 发布使用真实文件句柄上的 `fcntl.flock`；多进程/多线程竞争同一工作区时只允许一个连续序号推进。
- 落后、重复、跨请求错位发布返回 `E_STALE_CHECKPOINT`。
- 缺失前置阶段或非法阶段跳转返回 `E_INVALID_STAGE`。
- 如果进程在“记录已落盘、ledger 尚未落盘”之间中断，下一次读取会在锁内识别并折回该孤立记录，恢复连续序号。
- `stop_after_stage`/`/api/stop` 只会在当前不可拆分阶段完成后生效；恢复时复用指纹一致的上游检查点。

停止与恢复演示：

```bash
python3 -m rag_orchestrator.cli --workspace runtime run q-001 --stop-after-stage dispatch
python3 -m rag_orchestrator.cli --workspace runtime run q-001
python3 -m rag_orchestrator.cli --workspace runtime checkpoints q-001
```

## 版本指纹与增量失效

每个阶段指纹由上游阶段指纹、输入内容摘要与相关配置段共同构成。当前映射：

| 变化来源 | 最早失效阶段 |
| --- | --- |
| query、同义词表、分解规则 | `normalize` |
| 文档分片、检索权重/来源先验、预算配置 | `dispatch` |
| 重排权重、MMR 阈值、证据条数/字符预算 | `rerank` |
| 答案模板配置 | `assemble` |

任何阶段失效时，其全部下游阶段强制重建，即使重建后的局部产物字节恰好相同；上游指纹匹配的阶段继续复用。查询结果的 `version.stage_fingerprints` 与 `invalidation.reasons` 会显示版本指纹、变化分量、最早受影响阶段和重建/复用阶段。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 可视化诊断工作台 |
| GET | `/api/health` | 健康检查 |
| GET | `/api/workspace` | 样本计数、配置指纹、预算版本、最近结果 |
| GET | `/api/config` | 完整配置与分段指纹 |
| GET | `/api/requests` | 本地请求样本 |
| GET | `/api/documents` | 本地文档样本 |
| POST | `/api/run` | `{"query_id","mode","stop_after_stage"}` |
| POST | `/api/run-all` | 运行全部请求，返回固定 `summary_sha256` |
| POST | `/api/stop` | 请求在当前阶段完成后停止 |
| POST | `/api/reload` | 重载 YAML/JSONL，便于验证版本失效 |
| GET | `/api/results` | 内存中的最近结果 |
| GET | `/api/results/<query_id>` | 单请求结果 |
| GET | `/api/checkpoints/<query_id>` | ledger 与阶段记录摘要 |
| GET | `/api/diagnostics/<query_id>` | 请求、结果、检查点、工作区诊断 |

业务错误统一放在 JSON 的 `error` 对象中；不会通过线程超时或静默丢弃表达失败。

## 稳定错误码

| 错误码 | 场景 |
| --- | --- |
| `E_QUERY_INVALID` | 空查询、超子查询上限、无词项重叠等 |
| `E_NUMERIC` | 检索/重排分数出现 `NaN`/`Inf` |
| `E_LATENCY_BUDGET` | 实际阶段耗时超过延迟预算 |
| `E_EVIDENCE_EMPTY` | 证据为空、引用无法覆盖、字符预算无法放入最小片段 |
| `E_BUDGET_EXHAUSTED` | 没有任何子查询通过预算准入 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/跨请求错位 |
| `E_INVALID_STAGE` | 缺失前置阶段或非法阶段跳转 |
| `E_WORKER` | 工作线程非预期异常（保留已完成子查询） |
| `E_CONFIG_INVALID` | YAML 配置非法或越界 |
| `E_REQUEST_NOT_FOUND` | 请求样本中不存在该 `query_id` |

## CLI

```text
python3 -m rag_orchestrator.cli [全局选项] <command> [命令选项]

全局选项：
  --config PATH       默认 data/config.yaml
  --docs PATH         默认 data/docs.jsonl
  --requests PATH     默认 data/requests.jsonl
  --workspace PATH    默认 runtime
  --compact           单行 canonical JSON

命令：
  generate            固定种子生成 JSONL/YAML 样本
  run <query_id>      单请求运行；--mode parallel|serial；--stable；--stop-after-stage
  run-all             全量运行；--mode；--stable
  checkpoints <id>    打印 ledger 与阶段记录
  serve               启动本地 HTTP 工作台
```

## 验证

```bash
scripts/verify.sh
```

该脚本重建样本并执行完整测试。当前开发环境结果：**43 passed**。并行/串行全量运行固定摘要、停止恢复、版本失效入口、预算降级、HTTP 启动和错误码演示见 [`VERIFICATION.md`](VERIFICATION.md)。

## 主要文件

```text
rag_orchestrator/
  config.py        YAML 加载、校验、配置分段哈希
  samplegen.py     固定种子样本生成器
  normalize.py     NFKC/分词/子查询分解
  budget.py        固定点预算裁决
  retrieval.py     多分片检索、去重、对数域打分
  evidence.py      重排、MMR 去重、字符预算
  assembly.py      模板答案与引用归属
  checkpoints.py   原子检查点、ledger、文件锁、序号
  fingerprint.py   阶段版本指纹与失效链
  engine.py        四阶段编排、线程池、停止/恢复
  web.py           Flask API 与工作台服务
  cli.py           命令行入口
tests/             43 项单元/集成验收测试
data/              固定种子样本
```
