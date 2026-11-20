# 本地 COW 快照链组件

面向本地块设备仿真场景的写时复制（copy-on-write）快照链实现。服务使用 FastAPI + SQLite，可由上层备份/回滚编排器通过本地 HTTP 调用。所有校验、故障恢复与演示均使用工作区内的固定样本，不依赖 Docker、外部数据库或网络服务。

## 交付内容

- `app/`：SQLite 模型、业务服务、状态机、恢复扫描、FastAPI 路由。
- `tests/`：业务码、COW、增量链、合并、回滚、并发、幂等和强制中止恢复测试。
- `scripts/e2e_demo.py`：固定种子端到端演示，包含一次真实 `os._exit` 强杀与重启恢复。
- `scripts/setup_windows.ps1`、`run_windows.ps1`、`test_windows.ps1`：Windows 本地离线安装、运行与验证。
- `wheelhouse/`：Windows `cp311`–`cp314` 离线 wheel 包，已用 `pip --no-index` 核对可解析。
- `samples/seed.txt`、`samples/blocks.json`：固定随机种子和确定性块样本。
- `artifacts/e2e/e2e_demo_result.json`：最近一次端到端验证的落盘结果。
- `docs/VALIDATION.md`：实际验证命令、输出和结论。

## Windows 环境

要求 Python 3.11 或以上。脚本默认从 `PATH` 查找 `python`；如使用 Windows launcher，可先直接执行 `py -3.11 -m venv .venv`，再运行安装步骤。

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup_windows.ps1
```

安装完全使用本地 `wheelhouse/`，不会访问包索引。也可手工执行：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --no-index --find-links wheelhouse -r requirements.txt
```

启动单 worker 本地服务：

```powershell
.\scripts\run_windows.ps1 -Port 8000
```

或：

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
```

运行测试与强杀恢复演示：

```powershell
.\scripts\test_windows.ps1
```

## 运行配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `COW_DB_PATH` | `data/cow_snapshots.db` | SQLite 文件路径 |
| `COW_VOLUME_ID` | `vol0` | 单卷标识 |
| `COW_BLOCK_COUNT` | `8` | 卷的固定块数 |
| `COW_FAULT_INJECTION` | `0` | 为 `1` 时允许端到端脚本触发强杀 |

强制终止测试钩子仅在 `COW_FAULT_INJECTION=1` 时可用。正常业务部署应保持关闭。服务按单 uvicorn worker 设计；同一卷的写入、快照、合并、删除和回滚由进程内卷锁串行化，SQLite WAL 保证只读查询看到完整提交视图。

## 数据模型与不变量

- `volumes`：固定块数、卷级单调 `consistency_seq`。
- `block_versions`：不可变块版本，保存块号、版本号、SHA-256、原始内容与最近写入时刻。相同内容的旧版本不会因后续写入消失。
- `current_blocks`：卷当前视图，仅保存每块指向的不可变版本。
- `snapshots`：快照元数据、父快照、模式、状态、一致性序号、增量引用数。
- `snapshot_refs`：快照冻结的版本引用。全量快照保存全部块；增量快照只保存相对父快照版本发生变化的块。
- `snapshot_ref_history`：合并时被替换的目标引用归档。
- `snapshot_events`：创建、提交、合并、删除、回滚和恢复修复审计事件。
- `intents`：操作前持久化的两阶段意图；未完成意图在重启时扫描。
- `requests`：请求标识、规范化请求哈希、持久化响应与 `IN_PROGRESS`/`COMPLETED`/`ABORTED` 状态。
- `cow_events`：首次或再次覆盖被活跃快照引用的版本时产生的 COW 审计。
- `recovery_reports`：每次启动恢复后基于已落盘事实生成的报告。

写入版本号取该块历史最大版本加一，因此回滚后继续写入仍保持单调递增。内容摘要由服务重新计算，只有与请求中的 SHA-256 完全相同才提交；非法块号与摘要错误均在事务前返回，不改变当前视图或一致性序号。

## 快照状态机

```text
CREATING -> COMMITTED | DELETED
COMMITTED -> MERGING | DELETED
MERGING -> MERGED | COMMITTED   (MERGING -> COMMITTED 仅用于恢复中止)
MERGED -> 终态
DELETED -> 终态
```

非法迁移返回 `E_INVALID_TRANSITION`，事务回滚且不改变已持久化状态。合并成功的源快照保留记录并处于 `MERGED`，不直接删除；`merged_into` 指向吸收其状态的目标快照。

## 冻结、COW 与合并

- 创建快照分两阶段：第一阶段在同一事务中写入 `CREATING` 快照、意图和冻结版本引用；第二阶段提交为 `COMMITTED` 并落盘响应。创建期间的写入无法进入已冻结引用集合。
- 快照不复制块内容，只引用不可变 `block_versions`。写入已冻结版本时创建新版本并切换 `current_blocks`，旧版本继续被快照共享，并记录 `cow_events`。
- 增量快照按当前版本号与父链解析结果比较，只保存变化块；查询时由子到父覆盖解析出完整块集合。
- 合并仅接受直接相邻的 `COMMITTED` 父子快照（源是目标的直接子）。目标不得有其他活跃子快照，以维持线性可解析链。第一阶段持久化 `MERGING` 与意图；第二阶段在同一事务中重写目标全量引用、推进目标一致性序号、将源置为 `MERGED`、重挂源的活动子快照并归档目标旧引用。任何读取方只能看到合并前或合并后的完整状态。
- 删除仍有活跃子快照的父快照返回 `E_SNAPSHOT_REFERENCED`，不删除或改写任何引用。叶子快照删除后状态为 `DELETED`，其引用行保留用于历史追溯。

## 回滚裁决

回滚前解析目标快照完整版本集合，逐块检查版本行、快照冻结摘要与实际内容 SHA-256 是否一致。任一内容缺失或摘要失败返回 `E_SNAPSHOT_CORRUPT`，卷当前视图保持不变。

差异输出包括 `write_back_blocks`、`unchanged_blocks`、`overwritten_current_versions`（当前/目标版本与摘要）以及目标/当前一致性序号。执行回滚时在一个 `BEGIN IMMEDIATE` 事务内切换所有需写回的当前块指针并推进卷一致性序号；SQLite WAL 让查询方只看到回滚前或回滚后的完整视图。

## 幂等、并发与恢复

- 每个变更请求可携带 `request_id`。服务持久化规范化请求哈希和最终响应；相同标识与内容重复提交等待首个请求并复用响应，相同标识不同内容返回 `E_CONFLICTING_REQUEST`。
- 同一卷变更由卷锁串行执行；并发触发同一合并时，非阻塞卷锁失败方返回 `E_OPERATION_ACTIVE`。
- 写入、快照提交、合并、删除和回滚前均先提交 `PENDING` 意图。最终业务变更、意图完成标记和请求响应在同一事务提交，避免“操作成功但响应未落盘”导致重复生效。
- 重启时扫描 `PENDING` 意图和 `IN_PROGRESS` 请求。`CREATING` 快照转 `DELETED` 并移除暂存引用；`MERGING` 源转回 `COMMITTED`，目标引用不受影响；已完成原子事务的写入/删除/回滚不会留下半成品。中断请求持久化为 `ABORTED`，响应码 `E_OPERATION_ABORTED`。
- 恢复报告只引用启动时已提交的 SQLite 事实，包含扫描/中止意图、修复快照、恢复的错误码、当前一致性序号、链深和引用块数，并明确不声称进程在强杀前生成过退出报告。

## HTTP API

所有成功响应含 `"ok": true`；失败响应均为 JSON 且含顶层 `code` 业务码。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/volumes/{volume}/blocks/write` | 写入块；请求含 `block_no`、`content` 或 `content_b64`、`sha256`、可选 `request_id` |
| `POST` | `/api/volumes/{volume}/snapshots/full` | 创建全量快照；可选 `snapshot_id`、`request_id` |
| `POST` | `/api/volumes/{volume}/snapshots/incremental` | 创建增量快照；需 `parent_id` |
| `POST` | `/api/volumes/{volume}/snapshots/{source}/merge` | 合并到直接父快照；需 `target_snapshot_id` |
| `POST` | `/api/volumes/{volume}/snapshots/{id}/delete` | 删除叶子快照 |
| `DELETE` | `/api/volumes/{volume}/snapshots/{id}` | 删除别名，支持 `request_id` 查询参数 |
| `POST` | `/api/volumes/{volume}/snapshots/{id}/rollback` | 执行回滚并返回差异 |
| `GET` | `/api/volumes/{volume}/chain` | 快照链、状态计数、链深与引用块数 |
| `GET` | `/api/volumes/{volume}/snapshots/{id}/diff` | 只读回滚差异预览 |
| `GET` | `/api/volumes/{volume}/snapshots/{id}/blocks` | 解析后的完整块版本集合与引用计数 |
| `GET` | `/api/volumes/{volume}/blocks` | 当前卷完整视图与一致性序号 |
| `GET` | `/api/volumes/{volume}/blocks/{no}/versions` | 单块历史版本与活跃快照引用数 |
| `GET` | `/api/volumes/{volume}/recovery-report` | 最近一次启动恢复报告 |
| `GET` | `/health` | 本地健康检查 |

主要业务码：`E_BLOCK_NOT_FOUND`、`E_BLOCK_CORRUPT`、`E_SNAPSHOT_REFERENCED`、`E_SNAPSHOT_CORRUPT`、`E_INVALID_TRANSITION`、`E_CONFLICTING_REQUEST`、`E_OPERATION_ACTIVE`、`E_OPERATION_ABORTED`、`E_SNAPSHOT_NOT_ADJACENT`、`E_INVALID_REQUEST`。

## 本地验证

```bash
.venv/bin/python -m pytest -q
.venv/bin/python scripts/e2e_demo.py
```

最近一次结果：`24 passed`；端到端演示链深 `3 -> 2`、活跃引用块 `11 -> 9`、回滚写回 `1` 块且未受影响 `7` 块；强制退出码 `86` 后重启报告为 `RECOVERED`，修复 `snap-inc-2: MERGING -> COMMITTED`，一致性检查通过。详见 `docs/VALIDATION.md`。

## 设计边界

- 这是本地单卷、单 worker 组件；未实现跨主机复制、多写者协调或网络认证。
- 不回收历史块版本。该策略换取快照/合并/恢复的可追溯性与简单一致事务；真实设备场景可在此模型上增加离线 GC。
- 合并采用“子状态吸收进直接父快照”的线性链压缩；目标若已有其他活跃子分支会拒绝合并，以避免静默改变其他分支 delta 的父语义。

