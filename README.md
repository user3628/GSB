# GPU 资源池配额调度与推理批调度诊断工作台

面向本地单机的离线诊断工具：依据确定性 JSON 样本，评估一组推理服务请求能否被
放入若干逻辑 GPU 节点，并诊断配额传播、批组织取舍、抢占回收与强制终止后的恢复。
全部资源数值使用整数运算，不依赖真实 GPU、不访问任何外部服务。

## 1. 环境准备（Windows / WSL 通用）

要求 Python 3.11+（本仓库在 Python 3.14 验证）。

```powershell
# Windows PowerShell
py -3 -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python scripts\generate_samples.py
.venv\Scripts\python -m pytest tests/ -q
```

```bash
# WSL / Linux（本次实际验证环境）
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/generate_samples.py
.venv/bin/python -m pytest tests/ -q
```

依赖：`fastapi`、`uvicorn`、`numpy`、`pytest`；SQLite 使用标准库 `sqlite3`。
不使用 Docker 或任何容器。

## 2. 目录结构

```
gpworkbench/
  units.py        量纲解析（40Gi/512Mi/250m），纯整数、溢出与负数检查
  spec.py         节点/配额组/模型/请求规格解析与跨记录一致性校验
  statemachine.py 七态状态机与合法迁移路径
  batching.py     批组织规划器（GREEDY_FF / MEM_DESC_BF、延迟与碎片处理）
  database.py     SQLite 连接、WAL、schema
  repository.py   仓储层与意图（intent）两阶段提交
  scheduler.py    调度服务：准入、批调度、抢占、迁移、崩溃恢复
  api.py          FastAPI 应用（仅绑定 127.0.0.1）
scripts/
  generate_samples.py 固定种子样本生成（small/medium/large/invalid）
  cli.py               命令行诊断入口
  run_demo.py          端到端验收演示，产出 data/demo_report.json
tests/                 pytest 套件（39 个用例）
data/samples/          确定性样本
```

## 3. 核心设计

### 3.1 量纲与归一化（业务要求 1）

- 显存：`Ki/Mi/Gi/Ti/B` 与裸字节整数统一归一化为字节整数（1024 进制）。
- 算力：`250m` 与裸整数统一归一化为千分之一份额整数。
- 统一做负数与 int64 溢出检查；任一规格非法抛 `E_SPEC_INVALID`，
  解析发生在数据库事务之外，失败时不写入任何状态（有专门测试断言库仍为空）。

### 3.2 配额准入（业务要求 2）

配额组占用 = 成员节点 `base_used_memory`（节点已占用显存）
+ 组内 `ADMITTED/BATCHED/RUNNING` 请求的预留量。准入在**单个事务**内完成
状态迁移与预留量扣减；超额抛 `E_QUOTA_EXCEEDED`，详情携带当前占用与剩余量，
请求保持 `PENDING` 且预留量为 0，不存在“节点已接受而配额未扣减”的中间态。

### 3.3 状态机（业务要求 3）

合法迁移路径：

```
PENDING   -> ADMITTED | REJECTED
ADMITTED  -> BATCHED | REJECTED | PREEMPTED
BATCHED   -> RUNNING | ADMITTED(重组) | PREEMPTED
RUNNING   -> COMPLETED
PREEMPTED -> ADMITTED | REJECTED
COMPLETED / REJECTED 为终态
```

非法迁移抛 `E_INVALID_TRANSITION` 且保持原状态；每次迁移写入 `request_events`，
记录触发原因、关联节点与批标识、意图标识。

### 3.4 批组织与策略取舍（业务要求 4、5）

- 仅同模型、同配额组、健康节点（`HEALTHY`）可放置；按提交序号 `seq` 保序。
- 批合计显存不得超过节点剩余显存，批样本数不得超过模型 `max_batch_samples`。
- 估计延迟 = 批样本总数 × 模型单位样本代价（整数乘法）；超过批内最严
  `latency_limit_ms` 时关闭当前批另开新批；单请求即超限的请求进 `oversized`
  清单并对应 `E_BATCH_TOO_LARGE` 语义，不参与成批。
- 组内请求规模超过阈值（默认 500）时自动从 `GREEDY_FF`（提交顺序首适应）
  切换到 `MEM_DESC_BF`（显存降序最佳适应），策略可由调用方显式覆盖。
- 输出所选策略、每节点装箱利用率、碎片拒绝清单；首轮碎片后按确定性规则
  用 `MEM_DESC_BF` 重组一次，仅当结果更优才采用。`reorganize_batches` 对存量
  批做拆批重建，全程不触碰配额预留量（有测试断言重组前后预留量一致）。

### 3.5 并发与可见性（业务要求 6）

- SQLite WAL + `BEGIN IMMEDIATE`：多读者并发快照读，写者全库串行；
  进程内再按配额组加锁实现“组单写者”，不同组互不阻塞。
- 状态与预留量在同一事务更新，读者不可能看到 `BATCHED` 却仍是旧预留量。
- 同一 `request_id` 重复提交只生效一次，后续调用返回 `deduplicated=true`
  并复用既有状态。
- 在途批处置语义：`RUNNING` 批不可抢占；`PLACED` 批成员被抢占时，
  空批标记 `ABORTED`，仍有成员的批就地重算样本/显存/算力与延迟。

### 3.6 抢占与回收（业务要求 7）

高优先级请求配额不足时，按“优先级升序、提交顺序”在同组 `ADMITTED/BATCHED`
请求中选择受害者，直到释放量覆盖显存与算力双缺口；受害者在**同一事务**内
迁移 `PREEMPTED` 并把预留显存/算力真实清零。无可抢占对象或释放仍不足时
返回 `E_PREEMPTION_NOT_POSSIBLE`。回收后受害者可再次通过准入校验。

### 3.7 持久化意图与崩溃恢复（业务要求 8）

所有写操作走两阶段：

1. 独立小事务写入 `intents` 行，状态 `PENDING`（先落意图）；
2. 第二个事务应用全部业务变更并把意图置为 `COMMITTED`，原子提交。

因此持久化事实只有两种：`COMMITTED` 意图的效果必然全部落库；`PENDING`
意图的效果必然全部未落库，不存在部分写入。进程被 kill -9 后重启调用
`/recovery/run`：扫描 `PENDING` 意图，能基于库内事实前滚的（SUBMIT/ADMIT/
BATCH/PREEMPT/REJECT）重新执行幂等应用，无法仅凭事实前滚的标记 `ABORTED`，
输出恢复报告，含恢复前后快照、占用差异、状态计数差异与恢复事件。

## 4. HTTP 服务（仅本机回环）

```bash
.venv/bin/python -m uvicorn gpworkbench.api:app --host 127.0.0.1 --port 8000
```

主要端点：`POST /spec/load`、`GET /quota/groups`、`GET /nodes`、
`POST /requests/{id}/admit|reject|preempt|transition`、
`POST /batching/run`、`POST /batching/reorganize/{group}`、
`POST /batches/{id}/start|complete`、`POST /recovery/run`、
`GET /intents`、`GET /events`。业务错误统一返回稳定业务码
（`E_SPEC_INVALID`、`E_QUOTA_EXCEEDED`、`E_INVALID_TRANSITION`、
`E_BATCH_TOO_LARGE`、`E_PREEMPTION_NOT_POSSIBLE` 等）。

## 5. CLI 与演示

```bash
.venv/bin/python scripts/generate_samples.py
.venv/bin/python scripts/run_demo.py            # 端到端验收，写 data/demo_report.json
.venv/bin/python scripts/cli.py load data/samples/workload_small.json
.venv/bin/python scripts/cli.py quota
.venv/bin/python scripts/cli.py batch --group grp-alpha
.venv/bin/python scripts/cli.py recover
```

## 6. 实际验证结果

- `pytest tests/ -q`：39 passed，覆盖量纲解析、规格失败零写入、配额准入、
  非法迁移、批组织/延迟/碎片、抢占回收、在途批处置、kill -9 恢复前滚与中止、
  HTTP（真实 uvicorn + 127.0.0.1）与并发读不变量。
- `scripts/run_demo.py` 结果（详见 `data/demo_report.json`）：
  - small 36 请求：GREEDY_FF，30 批，碎片 3；
  - medium 480 请求：GREEDY_FF，配额拒绝 420，碎片 12；
  - large 2400 请求：自动切换 MEM_DESC_BF，配额拒绝 2340，碎片 12；
  - 非法样本：`E_SPEC_INVALID` 且库内零写入；
  - 抢占：1 个低优先级受害者 `PREEMPTED` 且预留量清零，高优先级请求同事务准入；
  - 崩溃恢复：BATCH 意图 PENDING 时模拟 kill -9，重启后 `ROLL_FORWARD`，
    报告给出前后占用差异，无部分写入。
