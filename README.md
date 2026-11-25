# 多路召回融合离线实验组件

这是一个纯本地、可重复的多路召回融合实验组件，用于在固定评估预算下比较：

- `lexical`：查询词与物品标题的词项重叠度；
- `category`：历史点击类目频次与类目物品频次；
- `vector`：查询哈希特征与物品隐因子的余弦相似度。

组件内置固定种子样本生成器，提供 CLI、本地 HTTP API、纯 Go SQLite 检查点、真实子进程工作池，以及串行/并行逐字节一致的确定性归约。不依赖 Docker、真实账号、线上服务或外部数据集。

## 环境与构建

本机验证环境为 Go 1.27.4；`go.mod` 已锁定模块版本，SQLite 使用 `database/sql` + `modernc.org/sqlite` 纯 Go 驱动，不依赖 CGO。构建脚本显式使用本地模块缓存：

```powershell
$env:GOPROXY = "off"
$env:GOSUMDB = "off"
go build -o .\bin\recallab.exe .\cmd\recallab
```

Linux/macOS：

```bash
export GOPROXY=off GOSUMDB=off
go build -o bin/recallab ./cmd/recallab
```

当前锁定的纯 Go SQLite 依赖要求 Go 1.26+；在 Go 1.27.4 上已完成构建、测试和 Windows 交叉构建。

## 快速开始

Windows PowerShell：

```powershell
.\scripts\run.ps1 -Items 800 -Queries 120 -Workers 4 -Concurrency 4
.\scripts\test.ps1
.\scripts\serve.ps1 -Addr 127.0.0.1:18761
```

Linux：

```bash
./scripts/run.sh
./scripts/test.sh
./scripts/serve.sh
./scripts/verify.sh
```

直接使用 CLI：

```bash
go run ./cmd/recallab generate --out data --seed 20261125 --items 800 --queries 120 --vector-dim 24 --strategy weighted
go run ./cmd/recallab run --data data --db data/experiment.db --out data/results.json --workers 4 --mode process
go run ./cmd/recallab summary --db data/experiment.db
go run ./cmd/recallab checkpoints --db data/experiment.db --query-id q-000001
```

`generate` 支持数十至数千查询。推荐的验收规模为 40 至 2000 查询；样本越大，JSONL 与结果文件也越大。

## 样本格式

生成目录包含：

- `items.jsonl`：`item_id`、`category`、`vector`、`title`；
- `queries.jsonl`：`query_id`、`query`、`locale`、`clicked_categories`；
- `qrels.jsonl`：`query_id`、`item_id`、`relevance`、`reference_rank`；
- `config.json`：带版本号的召回、融合、预算和执行配置；
- `manifest.json`：种子、规模和内容摘要。

配置示例：

```json
{
  "version": "fusion-v1",
  "seed": 20261125,
  "vector_dim": 24,
  "routes": {
    "lexical":  { "candidate_depth": 24, "evaluation_quota": 24000 },
    "category": { "candidate_depth": 24, "evaluation_quota": 24000 },
    "vector":   { "candidate_depth": 24, "evaluation_quota": 24000 }
  },
  "fusion": {
    "strategy": "weighted",
    "weights": { "lexical": 0.45, "category": 0.25, "vector": 0.30 },
    "rrf_k": 60,
    "top_k": 10,
    "dedupe_by_item_id": true
  },
  "budget": {
    "global_candidate_evaluations": 60000,
    "per_round_time_budget_ms": 500,
    "estimated_candidate_micros": 25
  },
  "evaluation": { "fused_top_k": 10 },
  "execution": { "max_concurrent_queries": 4, "worker_processes": 4 }
}
```

融合策略支持 `weighted` 和 `rrf`。负权重在运行前返回 `E_INVALID_WEIGHT`；非法配置返回 `E_INVALID_CONFIG`。

## 算法口径

### 召回

- 词法路：唯一词项集合的 Dice 系数 `2*overlap/(|Q|+|T|)`；
- 类目路：`Σ click_frequency(c) * log1p(category_item_count(c))`，其中点击频率按历史类目归一化；
- 向量路：查询词、locale 和历史类目通过 FNV-1a 稳定哈希为查询特征，再计算余弦相似度；
- 每路只保留正分候选，按 `score DESC, item_id ASC` 排序并截断；
- 空查询文本或空物品库返回带 `query_id` 的 `E_EMPTY_INPUT`。

### 预算裁决

预算器以查询为单元、以并发批次为轮次。每轮同时考虑：

- 全局候选评估次数预算；
- 每路累计评估配额；
- 配置候选深度与物品库大小；
- 剩余查询数对应的公平份额；
- 每轮时间预算换算出的确定性候选容量。

每路决定均记录 `enabled`、配置/实际深度、预测消耗、剩余配额、选择理由和被淘汰原因。无法分配任何候选的查询进入 `unprocessed` 并记录原因，绝不静默丢弃。

为保证串行与并行逐字节一致，时间预算不直接读取实际 wall-clock 来改变结果，而是使用 `estimated_candidate_micros` 转换成每轮候选容量；这样同一配置在慢速 Windows 机器和快速 Linux 机器上会得到同一实验产物。实际并行执行仍受 `max_concurrent_queries` 和真实子进程数量约束。

### 融合与评估

- `weighted`：`Σ weight(route) * route_score`；
- `rrf`：`Σ weight(route) / (rrf_k + rank)`；
- 同一 `item_id` 合并分数，记录每路排名、原始分数和贡献；最终无重复项；
- `route_details` 中的 `item_ids` 按 `item_id` 升序，便于与逐路明细自洽校验；
- `top_k` 大于候选数时返回全部候选，并记录 `top_k_exceeds_candidates` 和有效截断；
- 指标：每路命中数、融合前十重合度、候选覆盖率、平均排名偏差；
- 分母为零时 `applicable=false` 并写明原因，摘要中同时记录指标级和原因级不适用计数。

具体口径：

- 融合前十重合度：`|fused_top10 ∩ relevant| / min(10, |relevant|)`；
- 候选覆盖率：三路候选并集中命中的参考物品数 / 参考物品数；
- 平均排名偏差：对同时出现在融合结果和任一路候选中的参考物品，计算 `|fused_rank - best_route_rank|` 的均值；
- 任一公式分母为零时显式标记不适用，而不是写入 0。

## 并行与确定性

`--mode process` 会启动真实本地子进程池，通过 stdin/stdout JSONL 协议逐任务通信；`--mode inline` 使用相同计算核心在进程内执行，便于对照。所有查询结果、预算决定、未处理列表和摘要最终按 `query_id` 归约排序，浮点值不经过随机化处理。

工作进程崩溃、协议损坏或 panic 会产生结构化 `E_WORKER_FAILURE`，包含 `shard`、`query_id` 和消息，并写入 SQLite 的 `failures` 表。

## 指纹与增量失效

各阶段输入指纹如下：

| 阶段 | 输入指纹组成 |
| --- | --- |
| `recall` | 物品摘要、查询内容摘要、召回/深度/预算配置摘要 |
| `fusion` | 召回产物摘要、融合策略/权重/截断配置摘要 |
| `evaluation` | 融合产物摘要、qrels 摘要、评估配置摘要 |

因此：

- 物品或查询变化时，从 `recall` 及其下游重建；
- 权重、策略、融合截断变化时，只重建 `fusion` 和 `evaluation`；
- qrels 变化时，只重建 `evaluation`；
- 不受影响的上游检查点继续复用。

复用后默认强制执行一次从零内存重算校验；结果或摘要不一致时返回 `E_INCREMENTAL_MISMATCH`，并附首个差异 `query_id`。仅在明确性能调优时可使用 `--skip-incremental-verification` 跳过，默认不建议。

## SQLite 检查点与并发发布

每个查询每阶段在一个 `BEGIN IMMEDIATE` 事务中发布：

- `stage`、`query_id`、查询内阶段序号 `sequence`；
- 单调递增的 `revision`；
- 输入指纹、配置版本、产物摘要、完整产物；
- 分片、运行标识和失效原因。

同一查询同一阶段的 `valid=1` 记录通过部分唯一索引保证最多一条。发布时先获取真实跨进程文件锁（Unix `flock`，Windows `LockFileEx`），再在 SQLite 事务中比较修订号、上游产物摘要和发布头。

分类规则：

- 正常推进：目标修订号等于当前最新修订号 + 1；
- `duplicate`：同修订、同指纹、同摘要；
- `tampered`：同修订但指纹/摘要变化，或行与发布头不一致，或 payload 摘要不匹配；
- `rollback`：修订号回退、跳号或跨查询上游错位。

后三类均返回稳定的 `E_STALE_CHECKPOINT`，并通过 `details.classification` 区分。阶段跳转返回 `E_ILLEGAL_STAGE_TRANSITION`，缺少前置阶段返回 `E_MISSING_PREREQUISITE`。

收到首个 `SIGINT`/`SIGTERM` 或 HTTP stop 请求后，完成当前轮已在执行的查询单元，再写入已完成检查点并退出。后续运行复用检查点恢复。第二个信号会强制退出，可用于模拟崩溃恢复。

## HTTP API

默认只允许绑定 loopback：

```powershell
.\bin\recallab.exe serve --addr 127.0.0.1:18761
```

端点：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| POST | `/v1/generate` | 生成确定性样本 |
| POST | `/v1/runs` | 异步启动实验，返回 `job_id` |
| GET | `/v1/jobs/{job_id}` | 查询任务状态与摘要 |
| GET | `/v1/jobs/{job_id}/result` | 获取完整运行结果 |
| POST | `/v1/jobs/{job_id}/stop` | 请求优雅停止 |
| GET | `/v1/runs/{run_id}/summary` | 查询已落库摘要 |
| GET | `/v1/runs/{run_id}/checkpoints?query_id=...` | 查询检查点元数据 |

示例：

```powershell
curl.exe -X POST http://127.0.0.1:18761/v1/generate `
  -H "Content-Type: application/json" `
  -d '{"out_dir":"data-http","seed":7,"items":200,"queries":40,"vector_dim":16,"strategy":"weighted"}'

curl.exe -X POST http://127.0.0.1:18761/v1/runs `
  -H "Content-Type: application/json" `
  -d '{"data_dir":"data-http","db_path":"data-http/experiment.db","workers":2,"concurrency":2,"mode":"process"}'
```

业务错误统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_STALE_CHECKPOINT",
    "message": "...",
    "query_id": "q-000001",
    "details": { "classification": "rollback" }
  }
}
```

主要稳定错误码：

`E_EMPTY_INPUT`、`E_INVALID_CONFIG`、`E_INVALID_WEIGHT`、`E_ILLEGAL_STAGE_TRANSITION`、`E_MISSING_PREREQUISITE`、`E_STALE_CHECKPOINT`、`E_INCREMENTAL_MISMATCH`、`E_WORKER_FAILURE`。

## 测试与验证结果

本工作区实际执行：

```bash
export GOPROXY=off GOSUMDB=off
go test ./... -count=1
go test -race ./internal/recall ./internal/budget ./internal/store ./internal/engine ./internal/api ./internal/sample ./internal/worker -count=1
go vet ./...
GOOS=windows go build ./...
./scripts/verify.sh
```

验证覆盖：

- 三路打分、同分 `item_id` 升序、空输入错误；
- 加权融合、RRF、去重、来源贡献、截断标志和明细自洽；
- 预算/配额/每轮容量裁决及未处理原因；
- 评估指标、N/A 计数和摘要指纹；
- 子进程池与内联执行的逐字节一致；
- 查询、qrels、融合权重变化的最早阶段失效与全量重建一致；
- 优雅停止后恢复与一次从零运行逐字节一致；
- SQLite 并发发布、重复/回退/篡改分类、单有效记录约束；
- HTTP 生成、运行、结果、摘要和检查点接口；
- 增量产物语义损坏时返回 `E_INCREMENTAL_MISMATCH` 及首个差异查询。

本地 200 物品/40 查询端到端对照中，`process` 与 `inline` 结果文件通过 `cmp` 和 SHA-256 校验，当前回归摘要为 `6f054abace7d2718994909b7fce99ee417d1dc2c933f1bb1bd0fc902af79670b`。仅修改融合权重后，召回检查点保留 `revision=1`，融合和评估检查点推进到 `revision=2`，增量结果与全新数据库结果逐字节一致。

另以 500 物品/1000 查询、单并发真实子进程执行外部 `SIGTERM` 测试：停止后保留 2 个完整查询和 6 条有效检查点，未完成查询均带原因进入 `unprocessed`；恢复运行后 1000 个查询全部完成，结果文件与全新数据库从零运行逐字节一致。HTTP 服务在回环地址 `127.0.0.1:18761` 的健康检查返回：

```json
{"ok":true,"result":{"service":"multirecall","version":"0.1.0"}}
```

## 目录结构

```text
cmd/recallab/           CLI 入口与隐藏 worker 模式
internal/api/           本地 HTTP API
internal/budget/        自适应预算裁决
internal/engine/        三阶段编排、检查点复用、摘要与增量校验
internal/model/         数据类型、配置、指纹、错误码
internal/pool/          真实子进程池与内联执行器
internal/recall/        三路召回、融合、评估
internal/sample/        固定种子 JSONL 生成与读取
internal/store/         SQLite 事务检查点、发布头、跨进程文件锁
internal/worker/        JSONL worker 协议与安全执行
scripts/                Windows PowerShell 与 Unix 脚本
```
