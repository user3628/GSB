# GradSync：单机多进程梯度交换与聚合调度器

GradSync 在单机上模拟参数服务器与多个工作节点之间的 AllReduce 式梯度聚合。所有输入由固定随机种子生成；服务、测试节点和演示均只访问 `127.0.0.1`，不依赖 GPU、Docker、真实集群或外部网络。

## 快速开始（Windows PowerShell）

要求 Python 3.11，并在项目根目录执行：

```powershell
py -3.11 --version
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python scripts\generate_samples.py
python scripts\generate_samples.py --check
python -m pytest -q
python scripts\run_demo.py
```

`run_demo.py` 会自动启动 3 个本地 HTTP 工作节点和调度服务，执行完整聚合、严格迟到、降级、幂等冲突、零权重与强行终止恢复场景。最终摘要写入 `artifacts/demo_report.json`，验证输出写入 `artifacts/verification.log`。

也可手动启动服务：

```powershell
python -m gradsync serve --host 127.0.0.1 --port 8000 --db artifacts\scheduler.db
python -m gradsync worker --host 127.0.0.1 --port 8101 --name worker-a
python -m gradsync worker --host 127.0.0.1 --port 8102 --name worker-b
python -m gradsync worker --host 127.0.0.1 --port 8103 --name worker-c
```

FastAPI 接口文档在 `http://127.0.0.1:8000/docs`。

## 确定性样本与聚合定义

- 样本文件：`data/samples.json`，由 `scripts/generate_samples.py` 生成。
- 主种子：`20261120`；每个演示轮次另有固定种子。
- 每轮包含若干参数分片，每个分片包含与工作节点数相同的梯度块。
- 聚合公式：对每个分片、每个元素执行
  `sum(gradient[node][element] * sample_count[node]) / sum(sample_count[node])`。
- 若任一分片可用权重和为 0，返回 `E_ZERO_WEIGHT`，不会触发除零异常。
- 聚合前按 `(shard_id, node_id)` 规范排序，并以显式 float64 顺序累加，因此 HTTP 到达顺序不影响数值。
- 输出同时提供 8 位小数预览、逐元素和、float64 小端字节的 Base64，以及 SHA-256 摘要。摘要输入为规范张量形状和原始 IEEE-754 字节，不依赖浮点近似相等。

轮次创建时会把 `worker_ids`、`sample_counts`、分片数/维度、收集截止时间、最大等待截止时间、策略和节点结果端点统一写入 SQLite；提交时验证节点、分片、样本数和梯度维度，避免各模块使用不同元数据。

## 状态机

状态为 `COLLECTING`、`READY`、`AGGREGATED`、`ABORTED`。

| 当前状态 | 合法目标 | 触发条件 |
|---|---|---|
| `COLLECTING` | `READY` | 截止快照所需分片齐全，或 finalize 建立最终快照 |
| `COLLECTING` | `ABORTED` | 显式中止 |
| `READY` | `AGGREGATED` | 快照聚合成功并完成 SQLite 提交 |
| `READY` | `ABORTED` | 显式中止 |
| `AGGREGATED` / `ABORTED` | 无 | 终态 |

非法迁移返回 `E_INVALID_TRANSITION`，未知轮次返回 `E_ROUND_NOT_FOUND`；校验与状态检查发生在写入前，失败事务回滚，不增加事件或修改轮次状态。

## 截止时间、迟到与降级

每轮持久化两个时刻：

- `collect_deadline_at`：收集截止时间。
- `max_wait_deadline_at = collect_deadline_at + max_wait_seconds`：严格等待上限。

`received_at <= collect_deadline_at` 的分片为按时分片；之后到达的分片写入迟到队列，并记录 `SUBMISSION_LATE_QUEUED` 事件。

- `STRICT`：截止后在最大等待窗口内到达的分片仍参与聚合；窗口结束前若仍不完整，finalize 返回 `E_BUSY`。窗口结束后才到达的分片不会进入最终快照，缺失块按降级结果处理。
- `DEGRADED`：截止时刻后即可按“按时到达分片”锁定快照聚合；迟到分片保留在队列但不参与该快照，结果附 `missing_nodes`、`missing_blocks` 和业务码 `E_DEGRADED_ROUND`。
- 可在轮次创建时指定策略，也可在 finalize 请求中用 `mode` 覆盖；实际生效策略写入快照和聚合结果。

严格模式在窗口内迟到的情况可由 `demo-strict-late` 复现：第一次 finalize 返回 `E_BUSY`，迟到块到达后轮次进入 `READY`，最终摘要与所有节点完整参与时的预期摘要相同。

## 幂等、冲突与并发

- 主键 `(round_id, node_id, shard_id)` 保证同一块只落一条记录。
- 内容摘要一致的重试返回 `outcome=DUPLICATE`，不重复计数，也不改变快照。
- 同键但内容不同的提交返回 `E_CONFLICTING_REPORT`；首次提交及其摘要保持不变。
- 每个轮次有独立 `RLock`，实现单轮单写者；不同轮次之间没有同一把轮次锁，可并行收集。
- 同一轮并发 finalize 时，第一个执行聚合，其余等待锁后直接读取并复用已提交结果。测试验证 8 个并发 finalize 只产生一个 `ROUND_AGGREGATED` 事件。
- 聚合前先持久化包含每个提交内容摘要的快照。聚合期间新提交要么等待后被终态拒绝，要么在已有锁定快照的 `READY` 状态下进入迟到队列，不能改写快照。

## SQLite 持久化与崩溃恢复

数据库使用 WAL、`synchronous=FULL` 和 `BEGIN IMMEDIATE`。主要表：

- `rounds`：轮次元数据、状态、截止时间、最终快照与摘要。
- `submissions`：首次提交的梯度原始 float64 字节、内容摘要、到达时间和迟到标记。
- `intents`：聚合意图，状态包括 `PENDING`、`IN_PROGRESS`、`COMMITTED`、`FAILED`、`CANCELLED`。
- `aggregate_results`：聚合张量、校验摘要、缺失清单、策略和快照摘要。
- `events`：可核验状态轨迹。
- `recovery_runs`：启动扫描恢复报告。
- `deliveries`：向工作节点下发结果的结果。

聚合流程先在一个事务内写入 `PENDING` 意图、锁定快照并把轮次置为 `READY`，事务提交后才计算。结果事务再同时写入聚合张量、将意图改为 `COMMITTED`、轮次改为 `AGGREGATED`。

启动时扫描 `PENDING` / `IN_PROGRESS` 意图：若快照可重算则重新聚合；若已存在结果则只修复意图状态；若重算失败则意图标记 `FAILED`，轮次保留 `READY` 并记录“需重跑”。`/recovery/report` 只陈述 SQLite 中已存在的事实，并明确不承诺被强杀进程仍能生成完整退出报告。

演示通过环境变量在结果落库前执行真实 `os._exit(91)`：

```text
GRADSYNC_CRASH_AT=before_aggregated_commit
GRADSYNC_CRASH_MARKER=artifacts/crash_marker.json
```

重启后恢复报告显示崩溃前为 `READY + PENDING intent + 0 result`，恢复后为 `AGGREGATED`，且摘要与预期一致。

## HTTP 接口

调度服务：

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查与待处理意图数 |
| `POST` | `/rounds` | 创建并注册轮次 |
| `GET` | `/rounds` | 列出轮次 |
| `GET` | `/rounds/{id}` | 状态、快照、提交、迟到队列、缺失块、结果和事件 |
| `POST` | `/rounds/{id}/submissions` | 提交节点/分片梯度块 |
| `POST` | `/rounds/{id}/finalize` | `{"mode":"STRICT|DEGRADED","deliver":true}` |
| `GET` | `/rounds/{id}/result` | 聚合结果 |
| `POST` | `/rounds/{id}/deliver` | 下发结果到已注册回环端点 |
| `POST` | `/rounds/{id}/abort` | 中止非终态轮次 |
| `GET` | `/rounds/{id}/events` | 状态轨迹 |
| `GET` | `/recovery/report` | 最近一次启动恢复报告 |
| `POST` | `/recovery/scan` | 手动再次扫描未完成意图 |

本地测试节点：

- `POST /run`：按每项的 `delay_seconds` 延迟提交；只允许回环 `scheduler_url`。
- `POST /results`：接收调度器下发的聚合结果。
- `GET /results`：检查节点收到的结果。

错误响应统一为：

```json
{
  "ok": false,
  "code": "E_CONFLICTING_REPORT",
  "message": "同一节点、轮次、分片已被首次提交占用，冲突内容未写入",
  "round_id": "demo-idempotent-a",
  "details": {}
}
```

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_ZERO_WEIGHT` | 422 | 可用权重和为 0 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移或终态写入 |
| `E_ROUND_NOT_FOUND` | 404 | 未知轮次 |
| `E_CONFLICTING_REPORT` | 409 | 重复键内容冲突，保留首次值 |
| `E_DEGRADED_ROUND` | 200 | 降级成功结果，`ok=true` 且附缺失清单 |
| `E_BUSY` | 409 | 严格模式尚未到可 finalize 的时刻 |
| `E_INVALID_SUBMISSION` | 422 | 节点、分片、样本数或维度不合法 |
| `E_RESULT_NOT_FOUND` | 404 | 轮次尚无结果 |
| `E_VALIDATION_ERROR` | 422 | 请求体校验失败 |
| `E_INTERNAL` | 500 | 未预期内部错误 |

## 本次实际验证

验证环境：Ubuntu/WSL，Python `3.11.18`，CPU，本地回环；无 GPU、Docker 和外部训练服务。最终测试结果：`16 passed`。

| 场景 | 业务码 | 缺失节点 | 逐元素和（8 位） | 聚合 SHA-256 |
|---|---|---|---|---|
| 完整聚合 | `OK` | 无 | `0.37634829` | `141721e21bf3785f7f66fa41231e54ea6126105cc772e716be1e424e577dcb2b` |
| 严格等待 + 截止后迟到 | `OK` | 无 | `1.60075748` | `02b85bab60e43c89cc5ee2bfcb90c19d39efdad872821e2961843d9446a28a20` |
| 降级聚合 | `E_DEGRADED_ROUND` | `worker-c` | `-0.89559280` | `e77cfec0cc5ed6fc75a740de91a567e6f616267a25238fe78b4378e3192d2c08` |
| 乱序/幂等 A | `OK` | 无 | - | `e35617df729a550c3322cf4545decc34ff557a879fdaccfe60a6f9f8cb8e8afa` |
| 乱序/幂等 B | `OK` | 无 | - | `e35617df729a550c3322cf4545decc34ff557a879fdaccfe60a6f9f8cb8e8afa` |
| 强杀恢复 | `OK` | 无 | `0.97731967` | `3f11b6ec6571d6e91349a2a86f36604a5333b42a70cd9d44659b3d586cfea942` |

额外验证：

- 严格模式提前 finalize 返回 `E_BUSY`，迟到块标记为 `late=true` 后仍参与窗口内聚合。
- 降级结果只包含截止时刻前到达的块；`worker-c` 的三个迟到块保留在队列中。
- 重复提交返回 `DUPLICATE`；另造冲突内容返回 `E_CONFLICTING_REPORT`，提交总数仍为 9。
- 正序与逆序提交的两个轮次具有相同张量预览和相同 SHA-256。
- 零权重返回 `E_ZERO_WEIGHT`；对已聚合轮次执行 abort 返回 `E_INVALID_TRANSITION`；未知轮次返回 `E_ROUND_NOT_FOUND`。
- 强杀前 SQLite：轮次 `READY`、意图 `PENDING`、结果数 0；重启后：轮次 `AGGREGATED`、恢复报告 `outcome=RECOVERED`，三个节点均收到结果。

可核验文件：

- `data/samples.json`：确定性样本与分片元数据。
- `artifacts/demo_report.json`：每轮摘要、缺失节点、错误码、状态轨迹、恢复报告。
- `artifacts/verification.log`：测试与演示输出。
- `artifacts/demo_crash.db`：强杀演练的 SQLite 事实。

## 项目结构

```text
gradsync/
  aggregation.py   规范化排序、加权均值、摘要与预览
  api.py           FastAPI 调度接口与统一错误体
  domain.py        状态、策略与合法迁移
  errors.py        业务错误码
  samples.py       固定种子样本生成
  scheduler.py     单轮锁、finalize、下发、启动恢复
  store.py         SQLite 表、事务、意图与状态轨迹
  worker_node.py   本地 HTTP 测试工作节点
scripts/
  generate_samples.py
  run_demo.py
tests/
  test_scheduler.py
  test_api.py
```

## 边界说明

- 调度器是单个协调进程；工作节点和 HTTP 测试端点均为本机独立进程/回环端口，不实现跨主机共识或真实参数服务器集群。
- SQLite 负责单机崩溃恢复；它不替代分布式数据库。同一数据库不应暴露给多个调度写者。
- 结果下发是尽力交付，失败会持久化在 `deliveries`，可再次调用 `/deliver`。
- 仅验证本地确定性样本；未调用外部网络、云服务、GPU 或容器。
