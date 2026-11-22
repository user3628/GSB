# 本地 GPU 配额调度与推理批预算裁决引擎

这是一个不依赖容器和外部服务的本地单机引擎，用于离线评估推理请求能否被放入一组逻辑 GPU 节点。系统使用确定性 JSON 样本、SQLite、两阶段意图日志和 Flask/CLI 接口，覆盖配额准入、状态机、推理批组织、延迟拆分、轮次调度、抢占回收、并发幂等和强杀恢复。

## 快速开始

目标环境是 Python 3.11 或更新版本。Windows PowerShell 示例：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# 一次跑完确定性场景（会重建指定数据库）
python -m quota_scheduler --db .\quota_scheduler.db demo
```

Linux/macOS 示例：

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m quota_scheduler --db ./quota_scheduler.db demo
```

分步执行：

```bash
python -m quota_scheduler --db ./quota_scheduler.db init --samples sample_data --load-requests
python -m quota_scheduler --db ./quota_scheduler.db admit --all
python -m quota_scheduler --db ./quota_scheduler.db organize --strategy auto
python -m quota_scheduler --db ./quota_scheduler.db schedule --max-parallel 2 --time-budget 2000ms
python -m quota_scheduler --db ./quota_scheduler.db complete --round 1
python -m quota_scheduler --db ./quota_scheduler.db summary
python -m quota_scheduler --db ./quota_scheduler.db recover
```

HTTP API：

```bash
python -m quota_scheduler --db ./quota_scheduler.db serve --samples sample_data --bootstrap --load-requests
# 打开 http://127.0.0.1:8000/
```

运行测试：

```bash
pytest -q
```

## 目录与模块

| 路径 | 作用 |
|---|---|
| `quota_scheduler/units.py` | `40Gi`、`512Mi`、`250m` 等量纲解析、整数溢出和负数检查 |
| `quota_scheduler/specs.py` | 节点、配额组、模型、请求、配置的解析、别名和归一化 |
| `quota_scheduler/db.py` | SQLite WAL、两阶段意图记录、单写者事务和写辅助函数 |
| `quota_scheduler/accounting.py` | 配额/节点派生占用、碎片和一致性不变量 |
| `quota_scheduler/state_machine.py` | 七种请求状态、合法迁移和批状态常量 |
| `quota_scheduler/batching.py` | 相邻请求分组、贪心/显存降序装箱、样本保持拆分、延迟上限 |
| `quota_scheduler/service.py` | 准入、抢占、批组织、轮次调度、完成回收、恢复与诊断 |
| `quota_scheduler/api.py` | Flask JSON API |
| `quota_scheduler/cli.py` | CLI、完整演示场景和可执行验收流程 |
| `sample_data/` | 确定性节点、配额组、模型、请求轨迹和配置 |
| `tests/` | 38 项 pytest 验收测试 |

## 数据与内部单位

- 显存统一存为字节整数，支持二进制 `Ki/Mi/Gi/Ti` 和 `KiB/MiB/GiB/TiB`，也支持十进制 `KB/MB/GB/TB`。
- 算力份额和毫秒时间统一存为千分之一整数，例如 `250m -> 250`、`1s -> 1000`。裸整数表示已经是内部千分值/毫秒值。
- 最大内部整数为有符号 64 位 `9223372036854775807`。负数、非整数结果、未知单位、NaN/Infinity 和溢出均返回 `E_SPEC_INVALID`，并带具体字段路径。
- 节点和请求都必须引用已声明的配额组，否则返回 `E_QUOTA_GROUP_UNKNOWN`。未知模型和节点分别返回 `E_MODEL_UNKNOWN`、`E_NODE_UNKNOWN`。
- 请求算力预留默认为 `samples_per_batch * unit_sample_cost`；请求可用 `compute_required` 显式覆盖。批延迟公式为 `batch_samples * unit_sample_cost`。

节点字段支持 `total_memory`/`total_memory_bytes`、`available_compute`/`compute_capacity`、`occupied_memory`、`occupied_compute`、`quota_group`、`healthy` 等别名。请求支持 `memory_demand`、`samples_per_batch`、`expected_latency`、`priority`、`quota_group`、可选 `node_id` 等字段。

## 配额口径

配额组占用不是单独的可漂移计数器，而是每次从持久化事实派生：

```text
group.memory_used = sum(member_nodes.occupied_memory)
                  + sum(active reservations.memory)
group.compute_used = sum(member_nodes.occupied_compute)
                   + sum(active reservations.compute)

node.memory_used = node.occupied_memory
                 + sum(active node_allocations.memory)
node.compute_used = node.occupied_compute
                  + sum(active node_allocations.compute)
```

准入成功时，`requests.status = ADMITTED` 与 `reservations.active = 1` 在同一事务提交。超额时返回 `E_QUOTA_EXCEEDED`，明细同时给出节点基础占用、已预留量、请求量、上限和缺口；失败事务不留下部分预留。

通过 `submit_request(..., auto_admit=True)` 提交且准入失败时，请求本身会以 `PENDING` 保留，准入和抢占副作用通过 SQLite 保存点回滚；之后可显式调用准入接口重试。

## 请求状态机

| 状态 | 合法后继 |
|---|---|
| `PENDING` | `ADMITTED`, `REJECTED` |
| `ADMITTED` | `BATCHED`, `REJECTED`, `PREEMPTED` |
| `BATCHED` | `RUNNING`, `PREEMPTED` |
| `RUNNING` | `COMPLETED`, `PREEMPTED` |
| `PREEMPTED` | `PENDING` |
| `COMPLETED` | 终态 |
| `REJECTED` | 终态 |

非法迁移返回 `E_INVALID_TRANSITION`，事务回滚且不修改请求状态或迁移历史。每次成功迁移写入 `state_transitions`，包含 `from_state`、`to_state`、触发原因、全局操作序号 `op_seq`、`op_id` 和时间。

资源驱动的迁移通过对应业务接口完成：准入、批组织、轮次派发、完成和抢占会同时修改占用事实。通用 `transition` 接口只处理资源语义明确的手工迁移，避免只改状态而资源不动。

## 批组织与延迟取舍

- 请求按提交序号排序，只有同一模型、同一节点且相邻的 ADMITTED 请求构成一个候选组。
- 批样本上限为模型 `max_batch_samples`，批合计显存与算力不得超过节点剩余容量。
- 请求样本数超过模型上限或延迟上限时会被拆成多个保持总量的片段。所有片段的 `samples` 之和等于原请求，所有片段内存按比例分配且总和严格等于原预留内存。
- 批延迟超过任一成员期望上限时不会成批。若单个样本都已超过延迟上限，返回 `E_BATCH_TOO_LARGE`；可拆分时输出每个片段的拆分约束、原样本数和总量保持校验。
- 请求数超过 `batch_strategy_threshold` 时，`auto` 会同时评估提交顺序贪心（`greedy_submission`）和显存降序首次适配（`memory_descending`），按未放置请求数、批数、总延迟和稳定排序选择，并输出候选指标与选择原因。
- 节点碎片导致请求无法放入时，报告 `E_BATCH_UNPLACEABLE` 和所有已检查成员节点的剩余量；自动选节点的请求使用确定性的 best-fit 规则。

## 并行轮次调度

每轮调度同时受 `global_max_parallel_batches` 和 `round_time_budget` 限制：

- 主排序为优先级降序；同优先级按轮次变化的配额组轮转顺序；组内按估计延迟、提交序号和批 ID 排序。
- 预算内且并发槽可用的批变为 `RUNNING`，其成员请求从 `BATCHED` 迁移到 `RUNNING`。
- 未派发批进入有界 `QUEUED` 队列。队列已满时整轮返回 `E_EXEC_QUEUE_FULL` 并原子回滚，不留下部分派发。
- 报告包含 `selected`、`queued`、`skipped`、`time_used_millis` 和跳过原因（并发上限或时间预算）。
- `round_dispatches(round_no, batch_id)` 主键保证同一批在同一轮只派发一次；重复调用同一轮返回已持久化报告并标记 `replayed=true`。
- 抢占后批状态为 `PREEMPTED`，调度器不会再次选中它。

完成批时会在同一事务中停用节点分配；当请求的所有活动批都完成时，再迁移到 `COMPLETED` 并释放配额预留。

## 抢占与回收

准入超额且开启抢占时，只选择同一配额组内严格更低优先级的请求。抢占以批为原子闭包：如果受害请求与其它请求共享批，则整个批及其成员一起被抢占，避免出现“批已失效但成员仍在执行”的状态。

抢占事务同时完成：

1. 请求迁移为 `PREEMPTED` 并记录迁移历史；
2. 相关批迁移为 `PREEMPTED`；
3. 停用节点分配；
4. 停用配额预留；
5. 重新计算配额组和节点占用并运行一致性不变量。

如果校验不通过，事务返回 `E_QUOTA_INCONSISTENT` 并整体回滚，不会出现只改状态不改占用的结果。

## 持久化、并发与恢复

- SQLite 使用 `journal_mode=WAL`、`synchronous=FULL`、外键约束和 `BEGIN IMMEDIATE` 写事务。业务层是单写者，多读者通过 WAL 快照读取。
- 每个业务写操作的流程是：先提交 `intents.status = PREPARED`，同步追加一条 `<db>.intents.jsonl` 审计记录，再开启业务事务做检查和所有事实变更，最后在同一事务中把意图标为 `COMMITTED` 并提交。SQLite 的 `intents` 表是恢复判定的权威来源，JSONL 文件用于离线回放和强杀后观察 `PREPARED` 事件。
- 如果进程在业务事务提交前被强杀，SQLite 会回滚整笔业务变更，但已持久化的 `PREPARED` 意图会保留。重启后的 `recover()` 扫描这些未完成意图，标记为 `ABORTED`，并仅依据数据库中的事实输出恢复报告。
- Flask 服务启动时默认调用 `recover(abort_running=true)`：把重启前遗留的 `RUNNING` 批标记为 `ABORTED`，把对应请求迁移为 `PREEMPTED` 并真实释放预留，防止恢复后误以为旧执行仍在继续。CLI 服务可用 `--no-recover-on-start` 关闭；手工恢复可用 `recover --abort-running`。
- 同一 `request_id` 通过唯一约束和串行写事务只生效一次；并发重复提交会复用已持久化结果，不会重复预留。
- 请求状态、预留、批和节点分配在同一提交边界内，读者只能看到提交前或提交后的完整快照。

故障注入使用环境变量 `QUOTA_SCHEDULER_CRASH_AT`，例如 `ORGANIZE_BATCHES:before_commit` 或 `ADMIT_REQUEST:before_commit`。该机制仅用于验证，默认关闭。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康与不变量检查 |
| `GET` | `/api/summary` | 请求/批状态计数、配额、节点和不变量 |
| `POST` | `/api/requests?auto_admit=1` | 提交单个请求或请求数组 |
| `GET` | `/api/requests` | 请求列表，可用 `status` 过滤 |
| `GET` | `/api/requests/<id>` | 请求、预留、批和迁移轨迹 |
| `GET` | `/api/requests/<id>/transitions` | 状态迁移轨迹 |
| `POST` | `/api/requests/<id>/admit` | 配额准入 |
| `POST` | `/api/requests/<id>/transition` | 手工状态迁移 |
| `POST` | `/api/requests/<id>/preempt` | 手工抢占 |
| `POST` | `/api/batches/organize` | 批组织，支持 `strategy`、`strict`、`request_ids` |
| `GET` | `/api/batches`、`/api/batches/<id>` | 批与成员、事件、派发明细 |
| `POST` | `/api/scheduler/rounds` | 调度一轮 |
| `GET` | `/api/scheduler/rounds/<n>` | 轮次报告 |
| `POST` | `/api/scheduler/rounds/<n>/complete` | 完成该轮所有 RUNNING 批 |
| `POST` | `/api/batches/<id>/complete` | 完成单批 |
| `GET` | `/api/quota` | 配额组与节点派生占用 |
| `GET` | `/api/intents` | 意图日志 |
| `POST` / `GET` | `/api/recovery` | 执行恢复 / 查看最近恢复报告 |

错误响应统一为：

```json
{"ok": false, "error": {"code": "E_QUOTA_EXCEEDED", "message": "...", "field": "...", "details": {}}}
```

## 稳定错误码

| 错误码 | 含义 |
|---|---|
| `E_SPEC_INVALID` | 字段缺失、单位/数值非法、负数或溢出 |
| `E_QUOTA_GROUP_UNKNOWN` | 节点或请求引用未声明配额组 |
| `E_QUOTA_EXCEEDED` | 配额组显存或算力不足，附占用/预留/请求明细 |
| `E_INVALID_TRANSITION` | 非法请求状态迁移或非法批状态操作 |
| `E_BATCH_TOO_LARGE` | 单样本已超延迟上限或批规模非法 |
| `E_BATCH_UNPLACEABLE` | 节点剩余显存/算力无法容纳请求 |
| `E_EXEC_QUEUE_FULL` | 有界等待队列已满 |
| `E_QUOTA_INCONSISTENT` | 配额、节点、预留或批分配一致性校验失败 |
| `E_REQUEST_NOT_FOUND` / `E_BATCH_NOT_FOUND` | 对象不存在 |
| `E_MODEL_UNKNOWN` / `E_NODE_UNKNOWN` | 模型或节点未知 |
| `E_NODE_UNHEALTHY` | 明确选择或候选节点不健康 |

## 实际验证

详细记录见 `VALIDATION.md`。摘要：

- `pytest -q`：`38 passed`。
- `sample_data` 场景：6 个请求顺序准入，最后的高优先级请求真实抢占 `req-vision-c`；生成 5 个批；3 轮调度分别在 `1160ms`、`1820ms`、`240ms` 预算内完成 2、2、1 个批。
- 最终 5 个请求 `COMPLETED`、1 个 `PREEMPTED`，全部预留已释放，所有不变量通过。
- 强杀测试分别在批组织事务和准入事务提交前终止子进程；重启后均扫描到 1 条 `PREPARED` 意图，业务状态保持最后完整版本，恢复后无遗留未完成意图。
