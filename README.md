# CSRouter：意图与优先级联判服务

首个可运行版本：固定种子生成本地 JSONL 工单与策略快照，完成规范化、意图打分、优先级裁决、验证汇总、SQLite 检查点、增量失效、停止恢复和回环 HTTP/CLI 演示。

原始任务书保留在 `docs/REQUIREMENTS.md`；实际验收记录见 `docs/VALIDATION.md`，环境与驱动版本见 `docs/ENVIRONMENT.md`。

## 快速开始

```bash
go version                 # 需要 Go 1.22+；实测 go1.27.4
make verify                # 单测 + 两档样本 + 稳定性/中断恢复验收
make demo                  # 生成 data/demo 并运行 small/medium
make serve-demo            # http://127.0.0.1:18099，服务 data/demo
```

也可以直接使用 CLI：

```bash
go build -o bin/csrouter ./cmd/csrouter
./bin/csrouter generate --root data
./bin/csrouter validate --root data
./bin/csrouter run --root data --tier small
./bin/csrouter run --root data --tier medium
./bin/csrouter report --root data --tier medium
./bin/csrouter serve --root data --addr 127.0.0.1:18099
```

依赖已经 vendored 到 `vendor/`，运行时不需要访问网络或外部数据集。SQLite 驱动为 cgo 版 `github.com/mattn/go-sqlite3 v1.14.24`，具体环境版本见 `docs/ENVIRONMENT.md`。

## 生成数据

固定种子为 `20261126`，生成 6 个 JSONL 分片和两份策略：

- `small`：400 条、8 个意图、2 个分片，每片 200 行。
- `medium`：4000 条、24 个意图、4 个分片，每片 1000 行。
- 每行固定 7 列：`ticket_id`、`text`、`channel`、`language`、`intent`、`priority`、`confidence_pct`。
- 确定性覆盖：中英混排、同渠道重复文本、空文本/空白/纯标点、优先级 `9/-1/7` 越界、`confidence_pct: null`，以及一条“越界优先级 + 缺失一致度”的 `E_INVALID_LABEL` 样本。

产物布局：

```text
<root>/samples/manifest.json
<root>/samples/small/shard-000.jsonl
<root>/samples/small/policy.json
<root>/samples/medium/shard-000.jsonl
<root>/samples/medium/policy.json
<root>/csrouter.db
<root>/reports/report-<tier>.json
<root>/reports/reference-<tier>.json
```

## 四阶段引擎

1. `normalize`
   - `golang.org/x/text/unicode/norm` 执行真实 Unicode NFKC，兼容全角/半角宽度。
   - 小写化、Unicode 空白折叠。
   - 中文输出单字与相邻二字 bigram；拉丁字母数字输出连续串。
   - 每个词项带规范化文本中的字符偏移、字节偏移、kind 和 channel。
   - 空文本或无词项的纯标点返回 `E_EMPTY_TEXT`，错误仍带 `ticket_id` 和生效版本。

2. `intent`
   - 从全部已标注工单统计类别先验、词项-类别计数和类别词项总量。
   - 可配置平滑强度，所有概率转 log 后累加。
   - 未见词项回退到显式 `__UNKNOWN__`。
   - 候选按分数降序、同分按意图名升序；Top 3 候选保留逐项对数贡献。
   - `smoothing_alpha=0` 返回 `E_INVALID_SMOOTHING`；NaN/Inf 返回 `E_NON_FINITE_SCORE`，错误含 `ticket_id`、`intent`、`config_version`。

3. `priority`
   - 合成意图后验、意图严重度和标注一致度，按 `p1/p2/p3` 阈值映射到 0–3。
   - 一致度缺失时使用策略中的 fallback，并在 `degradation_sources` 标注。
   - 优先级越界但一致度可用：仍给预测，明细为 `UNCOMPARABLE_PRIORITY`。
   - 越界/缺失优先级且一致度缺失：返回 `E_INVALID_LABEL`，不丢弃样本。

4. `verify`
   - 优先级比对分为 `HIT`、`MISS`（标注更高，漏判）、`FALSE_POSITIVE`（预测更高，误判）。
   - 汇总意图标签准确率、优先级准确率、降级和无效标签数量。
   - 与本地高精度 `exact_full` 参考器比对并写入 `reference` 字段。

## 规模策略

规划器读取策略中的显式观测：`declared_tickets`、`intent_count`、`observed_unique_terms`、`observed_total_terms`、`hash_dimensions`、`round_budget`。

- 小档选择 `exact_full`：估算 121,344 个 term-intent 累加，低于预算 180,000；不启用为大规模设计的 hash/collision 机制。
- 中档默认选择 `hash_bucket_budget`：精确估算 3,625,392 超过预算 3,500,000；hash 估算 3,469,920 符合预算，且 `hash_dimensions=4096` 相对 781 个观察词项足够稀疏。
- `vocab_priority_truncated` 也会计算估算成本和入选/淘汰理由；单元测试覆盖预算变化时三种策略的选择边界。

报告中的 `strategy.candidates` 保存每种策略的估算 ops、是否符合预算、选择状态和淘汰原因。

## SQLite 检查点与恢复

数据库表：

- `artifacts`：当前每工单每阶段产物，含 `stage_seq`、`revision`、`input_fingerprint`、`config_version`、`config_fingerprint`、`digest` 和完整 JSON。
- `artifact_ledger`：append-only 单调全局序号和 `(stage, stage_seq)` 唯一序号。
- `stage_cursors`：每阶段最后连续序号。
- `policies`：单调策略版本、指纹、变更码、回退来源。
- `tickets`、`shards`、`reports`、`stop_state`。

发布协议：

1. `flock(csrouter.db.lock, LOCK_EX)` 获取真实进程间文件锁。
2. `BEGIN IMMEDIATE` 开启 SQLite 事务。
3. 检查前置阶段产物；缺失返回 `E_ILLEGAL_STAGE`。
4. 条件更新 `stage_cursors ... WHERE last_seq=?`；落后、重复、非连续或竞争失败返回 `E_STALE_CHECKPOINT`。
5. 插入 artifact 和 ledger，提交事务。进程在任意 SQL 写入位置被杀掉时，未提交事务由 SQLite 回滚，不会留下半写 artifact。

收到停止请求后，循环只在下一工单前检查停止位；当前工单的四个阶段会全部完成。恢复命令复用未失效检查点。Go 测试和 `scripts/verify.sh` 均验证连续运行与 41 票中断后恢复出的 `report-small.json` 逐字节相同。

## 版本变化与增量失效

版本指纹由输入内容、各阶段 digest 和配置 section hash 构成：

- 工单文本变化：最早失效该工单 `normalize` 及其下游。
- 因 NB 模型从全部标注工单训练，分片替换还会使全 tier 的 `intent/priority/verify` 失效；其他工单的 `normalize` 继续复用。
- `intent_vocab_weights`、`smoothing_alpha`、`hash_dimensions`、`round_budget`、`vocab_top_k`：最早失效 `intent`，复用 `normalize`。
- `priority_thresholds`、`missing_confidence_fallback_pct`、`intent_severity`：最早失效 `priority`，复用 `normalize/intent`。

稳定业务码：

- 正常推进：`PIPELINE_ADVANCED` / `RUN_COMPLETED` / `STAGE_COMPLETED`
- 单分片替换：`SHARD_REPLACED`
- 整体快照/策略替换：`SNAPSHOT_REPLACED`
- 单配置变更：`CONFIG_UPDATED`
- 回退：`CONFIG_ROLLED_BACK`
- 增量重建：`INCREMENTAL_REBUILT`
- 停止：`STOP_REQUESTED`
- 错误：`E_EMPTY_TEXT`、`E_INVALID_LABEL`、`E_INVALID_SMOOTHING`、`E_NON_FINITE_SCORE`、`E_STALE_CHECKPOINT`、`E_ILLEGAL_STAGE`、`E_SHARD_DECLARATION_MISMATCH`

## CLI 示例

```bash
./bin/csrouter generate --root data
./bin/csrouter validate --root data
./bin/csrouter run --root data --tier small --stage normalize
./bin/csrouter run --root data --tier small --stage intent
./bin/csrouter run --root data --tier small
./bin/csrouter stop --root data
./bin/csrouter resume --root data --tier small
./bin/csrouter status --root data --tier small
./bin/csrouter report --root data --tier medium --comparison MISS
./bin/csrouter replace-shard --root data --tier small --demo
./bin/csrouter change-config --root data --tier small \
  --key priority_thresholds.p3 --value 0.90
./bin/csrouter rollback --root data --tier small --version 1
```

`run --stop-after N` 是确定性测试钩子：完成 N 个不可拆分工单后设置优雅停止位。

## HTTP API

服务强制只允许 `127.0.0.1`、`localhost` 或 `[::1]`：

```bash
./bin/csrouter serve --root data --addr 127.0.0.1:18099
```

浏览器打开 `http://127.0.0.1:18099/` 有确定性演示台。主要 JSON API：

- `POST /api/generate?seed=20261126`：生成、校验并导入。
- `GET /api/manifest`、`GET /api/validate`
- `POST /api/run?tier=small[&stage=intent]`：执行到指定阶段或完整验证。
- `POST /api/resume?tier=small`、`POST /api/stop`
- `GET /api/status?tier=small`、`GET /api/fingerprints?tier=small`
- `GET /api/ticket?id=S-0001`：返回四阶段产物和检查点记录。
- `GET /api/report?tier=medium[&comparison=MISS]`
- `GET /api/priority-details?tier=medium&comparison=FALSE_POSITIVE`
- `GET /api/strategy?tier=medium`、`GET /api/reference?tier=medium`
- `POST /api/shards/replace`：body `{"tier":"small","demo":true}` 或指定 `path/source`。
- `POST /api/config/change`：body `{"tier":"small","key":"smoothing_alpha","value":0.5}`。
- `POST /api/config/rollback`：body `{"tier":"small","version":1}`。
- `POST /api/policy/replace`：整体策略替换。
- `POST /api/checkpoints/publish`：可复现 `E_STALE_CHECKPOINT` / `E_ILLEGAL_STAGE` 的条件发布入口。

错误响应体是结构化 `BizError`，包含 `code`、`message`、`ticket_id`、`intent`、`config_version` 和可用时的 `version_fingerprint`；HTTP 状态分别使用 404、409、422、500。

## 代码结构

```text
cmd/csrouter/          CLI 与 demo 入口
internal/domain/       稳定 JSON 模型、业务码、指纹
internal/generator/    固定种子生成器
internal/nlp/          NFKC、空白/宽度、偏移、分词
internal/engine/       NB log 打分、策略规划、优先级/验证
internal/reference/    高精度 exact_full 参考器与比对
internal/store/        SQLite schema、事务、flock、检查点
internal/app/          导入、增量、恢复、报告编排
internal/api/          回环 HTTP API 与演示页
scripts/verify.sh      一键验收
```
