# 本地块存储冷热分层与迁移组件

这是一个面向本地块存储场景的冷热分层、批次迁移与崩溃恢复组件。服务使用 FastAPI + SQLite，提供可直接集成的本地 HTTP API；所有样本均由固定随机种子生成，不依赖外部数据或网络服务。

## 交付内容

- `app/`：策略快照、候选选择、状态机、迁移执行、占用重算、恢复与 HTTP API。
- `samples/`：固定种子生成的三层 12 分片样本、策略快照与热度分布。
- `scripts/generate_samples.py`：确定性样本生成器。
- `scripts/demo_e2e.py`：真实子进程强杀、重启恢复、全量迁移和容量级联演示。
- `tests/`：24 项自动化测试，包含真实 `os._exit(73)` 子进程恢复测试。
- `docs/DESIGN.md`：事务边界、恢复规则、并发与不变量说明。

## Windows 本地启动

在 PowerShell 中执行：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

python scripts\generate_samples.py
python -m app.cli init --db data\tiering.db --samples samples --reset
python -m app.cli serve --db data\tiering.db --samples samples --host 127.0.0.1 --port 8765
```

打开 `http://127.0.0.1:8765/docs` 查看交互式 API。服务默认仅绑定回环地址，CLI 固定 `workers=1`；SQLite 写事务由单写者串行化，不应启动多个 uvicorn worker。

重新生成可重复环境：

```powershell
python scripts\generate_samples.py
python -m app.cli init --db data\tiering.db --samples samples --reset
```

`init --reset` 只删除工作区内的 SQLite、WAL 和 SHM 文件。`scripts/demo_e2e.py` 会自动清理并重建 `artifacts/demo-runtime/`。

## 测试与端到端演示

```powershell
pytest -q
python scripts\demo_e2e.py
```

演示结果写入 `artifacts/demo-runtime/demo-result.json`，包括：

- 策略级联调整前后的候选资格与分层占用；
- 非法 `COMMITTED -> PENDING` 迁移拒绝；
- 强制终止前的批次计划；
- 真实子进程退出码与重启后的恢复批次、逐分片状态、转移记录；
- 恢复前后分层占用差异；
- 全量迁移批次的层占用前后对照；
- 最终安全退出摘要。

## 本地样本

`scripts/generate_samples.py` 使用 `numpy.random.default_rng(20261123)` 生成：

- `samples/policy_snapshot.json`：版本 1，三层 `hot-nvme` / `warm-ssd` / `cold-hdd`，热度阈值 128，批上限 16，冷却 3600 秒。
- `samples/shards.json`：12 个确定性分片，包含字节数、热度灰度、所在层和冷却截止时刻。
- `samples/heat_distribution.json`：16 桶热度分布及每层冷热计数。

样本快照校验摘要为：

```text
d2e09c3199fa1fc6eb2cbca8acd9beebd4970e43d42493871876bc0127c6424e
```

快照摘要计算范围严格限定为 `version`、`generated_at`、`layers`、`heat_threshold`、`batch_limit` 与 `cooldown_seconds`，使用排序键、紧凑分隔符的 UTF-8 JSON 做 SHA-256。可直接调用：

```python
from app.utils import snapshot_checksum
checksum = snapshot_checksum(snapshot_without_checksum)
```

## HTTP API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康检查与当前提交版本 |
| GET | `/v1/policy/current` | 当前完整快照与派生运行摘要 |
| POST | `/v1/policy/snapshots` | 整体替换策略快照 |
| POST | `/v1/policy/adjustments` | 基于 `base_version` 的增量调整 |
| GET | `/v1/layers/occupancy` | 分层容量、占用、冷热分布与候选资格 |
| GET | `/v1/layers/distribution` | 热度直方图和冷热字节摘要 |
| GET | `/v1/shards` | 分片列表，可按层过滤 |
| GET | `/v1/shards/{shard_id}` | 单分片及最近状态迁移记录 |
| POST | `/v1/batches/preview` | 确定性构造候选批次，不落盘执行 |
| POST | `/v1/batches` | 按批次标识执行一次迁移批次 |
| GET | `/v1/batches/{batch_id}` | 批次整体状态、逐分片状态和全部状态迁移 |
| GET | `/v1/state-machine` | 状态与合法迁移表 |
| POST | `/v1/state-machine/validate` | 只校验、不修改持久化状态 |
| POST | `/v1/faults/injection` | 注入 `raise` 或真实 `os._exit` 中断 |
| GET | `/v1/recovery/report` | 启动恢复报告与安全退出报告 |
| GET | `/v1/recovery/reports` | 报告索引 |
| POST | `/v1/admin/safe-shutdown` | 写入安全退出摘要并停止接受写请求 |

### 快照提交

```json
{
  "version": 2,
  "generated_at": "2026-02-01T00:00:00Z",
  "layers": [
    {"id": "hot-nvme", "capacity_bytes": 4294967296},
    {"id": "warm-ssd", "capacity_bytes": 8589934592},
    {"id": "cold-hdd", "capacity_bytes": 21474836480}
  ],
  "heat_threshold": 200,
  "batch_limit": 16,
  "cooldown_seconds": 0,
  "checksum": "..."
}
```

完整快照提交始终整体替换，不合并旧层参数。版本不大于当前版本返回 `E_STALE_SNAPSHOT`；摘要不符返回 `E_SNAPSHOT_INVALID`；两者都不会改变当前可服务快照。任何层新容量小于当前占用时返回 `E_CAPACITY_OVERFLOW`，并在 `context.layers[*].overflow_bytes` 给出超出字节数，整个快照事务回滚。

增量调整示例：

```json
{
  "base_version": 1,
  "version": 2,
  "layer_capacity_updates": {"cold-hdd": 21474836480},
  "heat_threshold": 200,
  "cooldown_seconds": 0
}
```

`base_version` 不是当前版本时返回 `E_STALE_BASE`。相同基线的并发调整只有一个能成功。容量、阈值、候选资格、层占用和热度摘要均在同一事务内重算。

### 批次执行

```json
{
  "batch_id": "batch-20261123-001",
  "snapshot_version": 2
}
```

- 批次标识相同且请求内容一致时复用第一次结果；内容不同返回 `E_CONFLICTING_BATCH`。
- 候选为空时仍提交空批次，不返回错误。
- 候选规则：热度 `>= threshold` 向更热一层移动；低于阈值向更冷一层移动；每层边界的极值分片不再移动。
- 候选按 `heat DESC, shard_id ASC` 排序，并以目标层当前占用加已接受入站字节做容量预留。
- 因批上限或目标层容量被排除的候选全部进入 `truncated`，其中包含 `truncation_reason`、容量缺口等依据。
- 每个迁移分片记录 `NONE -> PENDING -> MIGRATING -> COMMITTED`；恢复批次记录 `PENDING/MIGRATING -> ABORTED`。

### 状态机

```text
PENDING    -> MIGRATING | ABORTED
MIGRATING  -> COMMITTED | FAILED | ABORTED
FAILED     -> PENDING
COMMITTED  -> 终态
ABORTED    -> 终态
```

非法状态路径返回 `E_INVALID_TRANSITION`，校验接口本身不写数据库。批次查询同时返回整体状态、逐分片最后状态/时刻、全部状态迁移事件和 `conclusion_matches_items`。

## 一致性与恢复设计

每次写操作都使用 SQLite `BEGIN IMMEDIATE`；读操作在单个 WAL 读事务中完成，因此一次查询只能看到某个完整提交版本。`state_epoch` 是单调提交序号，写入层占用、批次和快照响应。

迁移采用两阶段持久化：

1. 先提交批次计划、逐分片 `PENDING` 状态和 `PREPARED` 意图记录。
2. 再在单个事务中修改分片所在层、源/目标层占用、逐分片状态和意图状态，并重算全部派生摘要。

因此不会出现“分片已迁移但源层未释放”或“目标层已计入但分片仍指向源层”的可观察状态。强制终止只会留下可识别的 `MIGRATING` 批次；下次启动会：

- 将未提交批次标记为 `ABORTED`，逐分片标记为 `ABORTED`；
- 将 `PREPARED` 意图标记为 `ROLLED_BACK`；
- 从 `shards.layer_id` 重建每层占用、分片数、冷热字节、热度直方图和候选资格；
- 写入 `STARTUP_RECOVERY` 报告，并明确 `safe_exit_summary_available=false`。

安全退出写入独立的 `SAFE_SHUTDOWN` 报告，包含已完成批次摘要、退出时层占用和退出期间被中止的未完成批次。两类报告在 `/v1/recovery/report` 中分别返回，不会把强制恢复伪装成完整退出报告。

详细事务边界和故障矩阵见 `docs/DESIGN.md`。

## 统一错误体

所有业务失败均返回：

```json
{
  "error": {
    "code": "E_CAPACITY_OVERFLOW",
    "message": "new layer capacity is below current occupancy",
    "context": {"layers": [], "total_overflow_bytes": 1},
    "state_epoch": 12
  }
}
```

已覆盖的核心业务码：

- `E_STALE_SNAPSHOT`
- `E_SNAPSHOT_INVALID`
- `E_INVALID_TRANSITION`
- `E_CONFLICTING_BATCH`
- `E_STALE_BASE`
- `E_CAPACITY_OVERFLOW`

另有 `E_NO_SNAPSHOT`、`E_BATCH_NOT_FOUND`、`E_SHARD_NOT_FOUND`、`E_INVALID_REQUEST`、`E_SERVICE_STOPPING`、`E_SIMULATED_TERMINATION` 等上下文错误码。

## 实际验证结果

本次交付在当前工作区实际执行：

```text
pytest -q
24 passed

python scripts/demo_e2e.py
exit code 0
```

固定样本与演示中的关键核对值：

- 阈值 128 初始候选 9 个，全部唯一；策略调整为阈值 200、`cold-hdd` 20 GiB、冷却 0 后，候选资格从 9 个重算为 8 个，`cascade_generation` 从 1 增至 2。
- 真实强杀进程退出码为 `73`；重启报告 `forced_termination_detected=true`，未完成批次 1 个，批次及 8 个分片均为 `ABORTED`，状态轨迹为 `NONE->PENDING` 8 次、`PENDING->ABORTED` 8 次。
- 恢复前后三层占用差异全部为 0 字节，恢复后层占用严格等于各层分片字节累加。
- 恢复后全量迁移批次移动 8 个分片：`hot-nvme` 从 `1,805,647,872` 字节/4 分片变为 `956,301,312` 字节/2 分片；`warm-ssd` 从 `1,809,842,176`/4 变为 `1,908,408,320`/4；`cold-hdd` 从 `2,821,718,016`/4 变为 `3,572,498,432`/6，总量守恒为 `6,437,208,064` 字节。
- 批次整体 `COMMITTED`、逐分片均 `COMMITTED`、`conclusion_matches_items=true`，迁移轨迹为 `NONE->PENDING`、`PENDING->MIGRATING`、`MIGRATING->COMMITTED` 各 8 次。
- 最终安全退出报告为 `SAFE_SHUTDOWN`，`forced_termination_detected=false`，与之前的启动恢复报告明确分离。

测试中出现的 DeprecationWarning 来自当前 Python 3.14 下 FastAPI/Starlette 的依赖告警，不影响测试结果；代码本身兼容 Python 3.10+。

## 工程约束

- 仅使用本地回环 HTTP 和 SQLite，不访问线上服务或真实账号。
- 数据库与运行产物均位于工作区；默认 `data/tiering.db` 和 `artifacts/demo-runtime/`。
- `/v1/faults/injection` 的 `hard_exit` 会真实终止进程，仅供本地故障验收使用。
- 生产集成应保持单写者进程；如需多进程网关，应在外部令牌或主从选举层保证同一时刻只有一个迁移执行器。
