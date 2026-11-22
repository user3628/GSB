# GitOps 集群配置回滚与收敛裁决组件

这是一个完全本地运行、可集成的领域组件。它以不可变快照维护受管集群、应用和依赖图，
负责快照提交、增量比较交换、受影响集合计算、级联回滚、收敛批次裁决，以及进程强杀后的
SQLite 事实恢复。

项目不需要 Docker、外部服务、账号或网络。提供 3000 个应用、5742 条依赖边的确定性
样本，也提供真实回环 HTTP 端到端脚本。

## 快速开始（Windows / Linux）

要求 Python 3.11 或更高版本。

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pytest
python scripts\run_example.py
```

Linux/macOS 对应命令：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pytest
python scripts/run_example.py
```

`requirements.txt` 固定了直接依赖版本：

```text
fastapi==0.115.6
uvicorn==0.34.0
numpy==2.2.1
pytest==8.3.4
httpx==0.28.1
```

手动启动服务（启动前会尝试绑定端口，绑定失败直接退出，不自动连接任何远端）：

```powershell
python -m gitops_convergence.server --host 127.0.0.1 --port 8876 --db artifacts/service.db
```

健康检查：`GET http://127.0.0.1:8876/health`。OpenAPI 文档位于 `/docs`。

## 固定样本

`scripts/generate_samples.py` 可重复生成以下确定性文件：

| 文件 | 内容 |
|---|---|
| `samples/large_cluster_graph.json` | 3 个集群、3000 个应用、5742 条依赖边；版本 1 |
| `samples/increment_single_app.json` | 基于版本 1、只改 `app-2999` 的版本 2 增量 |
| `samples/rollback_cascade_snapshot.json` | 基于版本 2、回滚 `app-2885` 并触发级联的版本 3 增量 |
| `samples/increment_conflict_a.json` | 基于版本 3 的并发增量 A（版本 4） |
| `samples/increment_conflict_b.json` | 同一基础版本 3 的并发增量 B（版本 4） |

样本包含整体 SHA-256 摘要。摘要对版本、生成时间、排序后的集群和完整应用配置做规范化
JSON 编码；增量提交时会基于基础快照重建完整配置并重新计算摘要。

## 领域模型与持久化

主要代码：

- `gitops_convergence/validation.py`：输入、引用、预算、摘要与版本校验。
- `gitops_convergence/state_machine.py`：五态状态机。
- `gitops_convergence/graph.py`：下游闭包、Kosaraju 环检测、级联顺序与依赖层级。
- `gitops_convergence/planning.py`：全量/局部路径选择、拓扑批次与配额拆分。
- `gitops_convergence/engine.py`：SQLite 事务、增量 CAS、幂等收敛与恢复。
- `gitops_convergence/api.py`：FastAPI 业务接口与统一错误响应。
- `gitops_convergence/storage.py`：WAL、Schema、事务工具。

SQLite 存放快照头、集群、不可变应用配置、运行时应用状态、收敛运行、批次、回滚级联和
意图日志。服务 CLI 默认使用工作区 `data/gitops.db`，示例脚本使用
`artifacts/example_run.db`；均可通过 `--db` 显式指定。

### 不可变版本与运行时状态

每个快照版本分别保存配置和状态。配置包含：

- `app_id`、`cluster_id`
- `desired_version`
- 提交时的 `effective_version`
- `replica_budget`
- `dependencies`（当前应用依赖的其他应用）

运行时状态另存 `state`、实际 `effective_version`、`requires_reevaluation` 和
`cascade_id`。查询始终绑定一个完整快照版本。

### 状态机

```text
PENDING       -> ROLLING_BACK | CONVERGED | FAILED | ABORTED
ROLLING_BACK  -> PENDING | CONVERGED | FAILED | ABORTED
CONVERGED     -> PENDING | ROLLING_BACK
FAILED        -> PENDING | ROLLING_BACK | CONVERGED | ABORTED
ABORTED       -> (terminal)
```

提交新快照时，`desired_version == effective_version` 的应用直接进入 `CONVERGED`，否则
进入 `PENDING`。非法迁移抛出 `E_INVALID_TRANSITION`，应用状态和批次不会被修改；手工
迁移只使用已持久化状态，不依赖进程内存。

## 算法与规模取舍

### 受影响集合

图中边方向为 `app -> dependency`，反向边为 `dependency -> dependent`。从变更或回滚
应用出发做反向 BFS，得到直接和间接下游闭包。复杂度为 `O(V_affected + E_affected)`，
不会为局部增量遍历无关子图。

### 级联回滚顺序

1. 取触发应用的下游闭包。
2. 用迭代式 Kosaraju 检测闭包中的强连通分量。
3. 有环时抛出 `E_DEPENDENCY_CYCLE`，上下文包含 `cycle_app_ids`、
   `cycle_components`、`outside_cycle_app_ids` 和仍可计算的部分顺序。
4. 无环时按“距触发点的最长下游距离”降序输出，即最远下游应用先回滚、触发应用最后回滚。

级联集合的全部状态迁移与新快照写入发生在同一个 `BEGIN IMMEDIATE` 事务中，因此不会出现
部分下游已 `ROLLING_BACK` 而触发应用仍为 `CONVERGED` 的可见状态。

### 收敛批次

仅 `PENDING`、`ROLLING_BACK`、`FAILED` 是待处理状态。批次按待处理子图的依赖层级生成：

1. 依赖先于依赖者，使用确定性 Kahn 拓扑分层。
2. 同一层应用之间不可能存在依赖边，所以同批不存在未完成依赖。
3. 每层按 `(cluster_id, app_id)` 排序，再按各集群副本配额和单批总副本上限拆批。
4. 单个应用自身超过配额时保留在独立批次并设置 `quota_exceeded=true`，绝不丢弃应用。

级联报告中的 `ordered_app_ids` 是回滚动作次序（下游先、触发应用后）；收敛批次使用
“依赖先于依赖者”的重放次序。两者分别服务于回滚编排和回滚完成后的收敛重放。

### 全量与局部一致性

`auto` 模式在总应用数超过阈值且受影响集合小于全图时选择 `INCREMENTAL`，否则使用
`EXACT_FULL`。两条路径最终使用相同的“待处理状态谓词”和同一批次构造函数；输出包含：

```json
{
  "recompute": {
    "path": "INCREMENTAL",
    "affected_app_count": 3,
    "total_app_count": 3000,
    "switch_threshold": 1000,
    "reason": "total applications 3000 exceed threshold 1000; only 3 are affected"
  }
}
```

测试对同一版本分别强制 `full` 与 `incremental`，断言 `plan_hash`、批次序号、应用归属和
最终状态完全一致。

## 并发与幂等

- **增量 CAS**：增量在写事务中重新读取 `current_version`，只有等于请求 `base_version`
  才提交。相同基础版本的两个并发增量只有一个能成功，另一个得到 `E_STALE_BASE`，且不会
  留下部分快照或状态。
- **收敛单写者**：`convergence_runs.snapshot_version` 唯一。运行先以意图记录和
  `RUNNING` 状态占位；相同内容的后到请求等待并复用已完成结果，执行次数保持 1。
- **触发内容冲突**：`snapshot_version + 规范化请求内容` 生成请求摘要。同版本不同模式或
  阈值返回 `E_CONFLICTING_TRIGGER`。
- **读写隔离**：WAL 模式下多个读事务可并发读取；状态迁移、批次发布和运行完成记录在同一
  写事务中提交，因此查询不会看到半收敛状态或与该状态不匹配的已发布批次。

## 意图日志与强杀恢复

快照提交、增量、状态迁移和收敛批次重建都先在独立事务写入 `RUNNING` 意图，然后才执行
数据事务。数据事务成功时在同一次提交中把意图标为 `COMPLETED`。

收敛采用以下顺序：

1. 提交收敛意图与 `RUNNING` 运行占位。
2. 计算计划（此阶段无可见写）。
3. 把计划中的批次以不可查询的 `BUILDING` 草稿持久化。
4. 一个事务内推进全部应用状态、把新批次发布为 `ACTIVE`、把运行标为 `COMPLETED`。

进程在任意位置被强杀时：

- 数据事务未提交，SQLite 回滚到最后一个完整版本；
- 独立的 `RUNNING` 意图和运行记录仍可被扫描到；
- 重启时 `Engine.recover()` 把未完成意图和收敛运行标为 `ABORTED`，把 `BUILDING` 批次
  标为 `ABORTED`；
- 恢复报告只列出数据库中的事实：最后完整版本、被中止的意图/运行/批次和持久化状态计数。

`gitops_convergence/crash_worker.py` 默认在 `BUILDING` 批次草稿持久化后、任何状态或
`ACTIVE` 计划发布前调用 `os._exit(137)`，用于真实验证该路径；也可用
`--crash-after convergence_intent` 在只有意图和运行占位时强杀。

## HTTP API

| 方法与路径 | 用途 |
|---|---|
| `GET /health` | 健康、当前版本、启动恢复摘要 |
| `GET /v1/state-machine` | 状态及合法迁移 |
| `POST /v1/snapshots` | 提交完整快照 |
| `POST /v1/snapshots/{base_version}/increments` | 提交局部增量 |
| `GET /v1/snapshots/current` | 当前快照摘要 |
| `GET /v1/snapshots/{version}` | 指定版本摘要 |
| `GET /v1/snapshots/{version}/apps` | 分页查询应用，可按状态/集群过滤 |
| `GET /v1/snapshots/{version}/apps/{app_id}` | 单应用状态 |
| `POST /v1/snapshots/{version}/apps/{app_id}/transition` | 手工状态迁移 |
| `POST /v1/snapshots/{version}/plan-preview` | 只读计算 full/incremental/auto 计划 |
| `POST /v1/snapshots/{version}/convergence` | 幂等触发收敛 |
| `GET /v1/snapshots/{version}/plan` | 读取已发布批次及其应用状态 |
| `GET /v1/snapshots/{version}/cascades` | 读取已持久化回滚级联 |
| `GET /v1/snapshots/{version}/cascade?app_id=...` | 只读计算级联；可用重复参数传多触发应用 |
| `POST /v1/recovery` | 显式执行一次恢复扫描 |

最小完整快照请求：

```json
{
  "version": 1,
  "generated_at": "2026-01-01T00:00:00Z",
  "clusters": [{"cluster_id": "c1", "replica_quota": 100}],
  "applications": [
    {
      "app_id": "a",
      "cluster_id": "c1",
      "desired_version": "v2",
      "effective_version": "v1",
      "replica_budget": 2,
      "dependencies": []
    }
  ]
}
```

增量示例：

```json
{
  "version": 2,
  "base_version": 1,
  "generated_at": "2026-01-01T00:05:00Z",
  "changes": [
    {"app_id": "a", "desired_version": "v3"},
    {"app_id": "b", "desired_version": "v0", "rollback": true}
  ]
}
```

失败响应统一为：

```json
{
  "code": "E_STALE_BASE",
  "message": "increment base version is no longer current",
  "context": {"base_version": 1, "current_version": 2}
}
```

业务错误码至少覆盖：

| 错误码 | 场景 |
|---|---|
| `E_SPEC_INVALID` | 重复 ID、缺失依赖、非法预算/溢出、摘要不符、结构错误 |
| `E_INVALID_TRANSITION` | 非法状态迁移 |
| `E_DEPENDENCY_CYCLE` | 级联或批次计算遇到环，响应仍含环外受影响集合 |
| `E_STALE_BASE` | 增量基础版本或基础摘要不是当前版本 |
| `E_CONFLICTING_TRIGGER` | 同版本并发触发内容不一致 |

## 端到端示例

`python scripts/run_example.py` 会：

1. 检查并绑定本地回环端口；
2. 启动真实 uvicorn 服务；
3. 提交 3000 应用样本并完成全量收敛；
4. 提交单应用增量并完成局部收敛；
5. 提交回滚增量，输出级联集合、回滚次序和每个收敛批次的状态；
6. 用两个线程同时提交基础版本 3 的版本 4 增量，验证一个成功、一个
   `E_STALE_BASE`；
7. 用独立子进程在版本 4 收敛意图后强杀，调用恢复接口；
8. 对比恢复前后的应用状态与计划摘要，再重试收敛；
9. 输出汇总并把完整证据写入 `artifacts/example_report.json`。

本次实际运行结果节选（Python 3.14.4，Linux 验证环境；代码兼容 Python 3.11+，完整控制台
输出见 `artifacts/example_stdout.json`）：

```json
{
  "sample_counts": {
    "applications": 3000,
    "clusters": 3,
    "initial_pending": 6,
    "rollback_cascade_size": 3
  },
  "v1_path": "EXACT_FULL",
  "v2_path": "INCREMENTAL",
  "v3_path": "INCREMENTAL",
  "rollback_cascade_order": ["app-2926", "app-2998", "app-2885"],
  "concurrent_conflict_codes": ["E_STALE_BASE", "OK"],
  "recovery": {
    "last_complete_snapshot_version": 4,
    "aborted_convergence_versions": [4],
    "aborted_batch_ids": ["v4-g1-b0001"],
    "state_diff_app_ids": [],
    "plan_unchanged": true
  },
  "final_state_counts": {"CONVERGED": 3000}
}
```

完整报告还包含每个批次的 `apps` 数组（含最终状态和生效版本）、完整回滚级联集合、
恢复前后的应用对象和并发响应。服务日志写入 `artifacts/example_server.log`。

## 测试与验证

```bash
pytest
```

本次验证结果：`17 passed`。覆盖范围：

- 重复应用 ID、缺失依赖、负数/溢出预算、摘要篡改及整份快照不生效；
- 初始状态、非法迁移不改变已持久化状态、重启后从 SQLite 重算；
- 同基础版本并发增量恰好一个成功；
- 增量后下游 `requires_reevaluation` 标记；
- 级联闭包、下游优先顺序、同事务全量推进；
- 依赖环、环上应用与环外受影响集合；
- full/incremental 路径的批次划分和最终状态完全一致；
- 配额拆批、超大单应用保留、阈值自动切换；
- 收敛触发幂等、同版本内容冲突、并发触发只执行一次；
- 真实 HTTP 业务错误与端到端流程；
- 独立进程强杀后的恢复、持久化状态与计划不变、恢复后重试。

原始记录保存在 `artifacts/pytest_output.txt`、`artifacts/example_stdout.json`、
`artifacts/example_report.json` 和 `artifacts/validation_summary.json`。

说明：收敛裁决负责推进领域状态与计划，不直接执行真实集群发布；外部 GitOps 执行器可按
批次结果执行，并把执行结果通过状态迁移接口回写。
