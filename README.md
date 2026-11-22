# 增量物化视图工作台

一个面向增量计算、物化视图刷新与依赖失效诊断的本地工作台。项目从空目录构建，不依赖 Docker、外部账号或网络资源。领域事实全部持久化在 SQLite；基表批次、依赖图替换、视图刷新、幂等请求和崩溃恢复共享同一套事务与版本语义。

## 交付内容

- `mv_workbench/`：Python 3.11+ 领域服务、FastAPI 接口、关系算子执行器、依赖图、SQLite 存储和诊断页面。
- `samples/`：由脚本生成的固定 JSON 样本，包含基表列式数据、两个视图定义版本和增量批次。
- `scripts/generate_samples.py`：确定性样本生成器；重复执行得到逐字节一致的文件。
- `scripts/demo.py`：通过真实本地 HTTP 服务执行端到端演示，并在刷新中途强制终止子进程、重启后验证恢复。
- `tests/`：19 项 pytest 验收测试。
- `artifacts/demo_result.json`：最近一次端到端运行的真实结果和持久化事实摘要。

## Windows 本地启动

在 PowerShell 中从项目根目录执行：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 重新生成确定样本（可选；仓库已包含同样内容）
python scripts\generate_samples.py

# 清空演示数据库
Remove-Item -Force data\workbench.db* -ErrorAction SilentlyContinue

python -m mv_workbench
```

默认仅监听回环地址 `127.0.0.1:8765`，诊断页面为 <http://127.0.0.1:8765/>，OpenAPI 文档为 <http://127.0.0.1:8765/docs>。可用环境变量修改：

```powershell
$env:MV_WORKBENCH_DB = "data\workbench.db"
$env:MV_WORKBENCH_SAMPLES = "samples"
$env:MV_WORKBENCH_HOST = "127.0.0.1"
$env:MV_WORKBENCH_PORT = "8765"
python -m mv_workbench
```

端到端演示会自行清空 `data/demo.db`、选择空闲回环端口、启动/停止服务、启动强制崩溃子进程，并重写 `artifacts/demo_result.json`：

```powershell
python scripts\demo.py
```

运行测试：

```powershell
python -m pytest -q
```

> 本次实际验证在提供的工作区（Linux/WSL）内完成。实现不使用 POSIX 专有 API，PowerShell 命令、`pathlib` 路径、SQLite WAL 和子进程退出码均按 Windows 兼容方式编写；Windows 本地只需按上述命令安装依赖并启动。

## HTTP API

成功响应统一为 `{"ok": true, "data": ...}`；业务失败统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_STALE_BASE",
    "message": "Batch base_version is not the current base-table version.",
    "context": {"table": "sales", "base_version": 1, "current_version": 2}
  }
}
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/health` | 服务与启动恢复状态 |
| `GET` | `/api/base-tables` | 基表、当前版本与快照摘要 |
| `GET` | `/api/base-tables/{table}?version=N` | 指定基表快照和版本化行 |
| `POST` | `/api/base-tables/{table}/batches` | 提交一个带 `base_version` 的新增/修正/逻辑删除批次 |
| `GET` | `/api/view-definitions?version=N` | 当前或历史视图定义 |
| `PUT` | `/api/view-definitions` | 整体替换视图定义，生成新依赖图版本 |
| `GET` | `/api/graph?version=N` | 边、拓扑层、顺序和图版本历史 |
| `GET` | `/api/views` | 当前全部视图状态和最近物化版本 |
| `GET` | `/api/views/{view}?version=N&delay_ms=0` | 按绑定版本查询行级内容、分组索引和新鲜度 |
| `GET` | `/api/views/{view}/freshness` | 状态、缺口、物化版本 |
| `POST` | `/api/refreshes` | 触发串行、幂等的拓扑刷新 |
| `GET` | `/api/refreshes/{request_id}` | 刷新请求的持久化状态和响应 |
| `GET` | `/api/diagnostics?limit=N` | 图分层、视图状态、最近失效/重算轨迹 |
| `GET` | `/api/recovery` | 最近恢复报告及历史 |
| `GET` | `/api/recovery/startup` | 本次进程启动时写入的恢复报告 |
| `POST` | `/api/admin/state-transition` | 验证状态机迁移；非法迁移返回 `E_INVALID_TRANSITION` |

刷新请求示例：

```powershell
$body = @{ request_id = "ops-2026-001"; mode = "all_stale" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/api/refreshes `
  -ContentType "application/json" -Body $body
```

`mode` 支持：

- `all_stale`：选择所有非 `FRESH` 视图及其下游闭包。
- `views`：只刷新 `views` 中指定视图；默认包含下游闭包。
- `all`：刷新当前图中全部视图。

`include_downstream=false` 时，若被选视图仍依赖未选中的 `STALE`/`PARTIAL` 上游，刷新不会误用旧依赖，而是让该视图进入 `PARTIAL` 并持久化具体缺口。`delay_ms` 和 `fail_views` 是本地诊断/测试钩子，用于扩大并发窗口和模拟计算失败。

## 领域模型

### 基表与批次

固定样本位于 `samples/base_tables.json`，表结构采用列式数组：

- `columns`：列名、数据类型、空值可空性。
- `columns_data`：按列组织的值。
- `row_versions`、`deleted`：行版本和逻辑删除标记。
- `null_bitmap`：逐列空值位图。
- `row_digests`、`snapshot_digest`：确定性 SHA-256 摘要。
- `version`、`generated_at`：基表快照版本和生成时间。

第一个列定义是行主键。新增行必须提供完整非空列；修正行可以只提供变化列，服务会在事务内与当前行合并并做完整类型/空值校验；逻辑删除只需主键与 `_deleted: true`。批次 `base_version` 必须等于事务内读到的当前基表版本，否则返回 `E_STALE_BASE`，基表、视图状态、请求和提交序号都不变化。JSON 解析失败同样返回 `E_STALE_BASE`，符合题目指定的业务码。

每个成功批次都会：

1. 关闭旧行版本的有效区间并写入新行版本；
2. 生成新基表快照、空值位图和内容摘要；
3. 在同一事务中找出直接受影响的视图，并把它们及全部下游标记为 `STALE`；
4. 写入逐视图失效事件并提交批次意图。

### 视图与依赖图

样本图有两个拓扑层：

```text
Layer 0: v_region_peak  v_region_revenue  v_return_reasons  v_sales_active
Layer 1: v_channel_refund  v_high_peak_regions  v_revenue_audit
```

覆盖投影、过滤、分组聚合、视图到视图依赖、基表到视图连接等形态。依赖图验证顺序为：

1. 视图定义结构、算子、聚合函数和表达式合法性；
2. 未声明基表/视图引用 → `E_REFERENCE_UNKNOWN`；
3. 环检测 → `E_DEPENDENCY_CYCLE`，上下文包含环上视图标识；
4. Kahn 拓扑排序并生成确定性分层。

整体替换视图定义会增加 `graph_version`。历史定义、边、分层、摘要和提交序号保存在 `graph_versions`；旧版本仍可通过 `GET /api/view-definitions?version=1` 与 `GET /api/graph?version=1` 查询。未变化视图保留现有物化结果；变化视图、新增视图和它们的所有下游进入 `STALE`。

### 状态机

```text
FRESH      -> STALE | REFRESHING | INVALID
STALE      -> REFRESHING | INVALID
REFRESHING -> FRESH | PARTIAL | STALE | INVALID
PARTIAL    -> STALE | REFRESHING | INVALID
INVALID    -> STALE | REFRESHING
```

`PARTIAL` 与 `INVALID` 都持久化 `gaps_json`。非法迁移在写入任何状态前失败，返回 `E_INVALID_TRANSITION`，上下文附当前状态与允许目标。

### 分组级部分失效

聚合函数分为：

- 可增量（distributive）：`sum`、`count`；
- 需要整视图重算：`min`、`max`、`avg`、`quantile`、`median`。

以固定修正批次为例，`sales` 发生东区金额修正、北区逻辑删除和南区新增。刷新 `v_region_revenue` 时：

- 输出受影响分组：`east`、`north`、`south`；
- 只发布 3 个受影响分组的新行；
- 未受影响的 `west` 分组直接复用上一版本行，分组摘要也与上一版本相同；
- 结论写入 `mode=incremental_groups` 与依据文本。

`v_region_peak` 含 `max` 和 `quantile`，因此即使只有一个输入组变化，也必须整视图重算 4 个组；响应明确列出 `non_incremental_functions` 和“not closed under correction/deletion”的依据，禁止发布部分陈旧最值。

执行器会先确定性推导候选完整结果用于防御性校验；发布阶段严格按上述判定写入：可增量聚合只替换变化组，其他组复制旧版本行与摘要；不可增量聚合替换全部组。

### 原子刷新与可见性

写入路径由进程内单写者锁串行化。刷新遵循：

1. 在独立事务中持久化 `REFRESH_ACCEPTED` 提交、请求 `IN_PROGRESS`、刷新意图和所选视图的 `REFRESHING` 状态；
2. 只读执行器按拓扑顺序计算上游到下游；
3. 在单个最终事务中插入所有新视图版本、分组索引、行、刷新事件，并把所有视图状态一起改为 `FRESH` 或 `PARTIAL`；
4. 同一最终事务把意图和请求标记为完成。

因此读者不会看到“上游已 `FRESH`，下游仍 `FRESH` 但结果未重建”的中间可见状态。刷新期间读取者只看到统一发布的旧物化版本和明确的 `REFRESHING` 状态。

SQLite 使用 WAL。每次查询单独开启读事务，并在第一个 SELECT 后持有快照。未指定版本时，查询先绑定当前 `materialized_version`；刷新在查询期间提交新版本也不会改变该读事务的结果。旧版本保留，因此显式历史查询可继续完成；请求了从未提交或被淘汰的版本时返回 `E_VERSION_RETIRED`。

### 幂等与冲突

`request_id` 是持久化主键。相同标识、相同规范化内容只执行一次，后续请求复用已存储响应；相同标识但 `mode`、`views`、`include_downstream` 或诊断失败集合不同，返回 `E_CONFLICTING_REQUEST`。进程内并发重复请求会在单写者锁上等待，然后读取首个请求结果，不会创建第二个视图版本。

### 意图、强制终止与恢复

基表批次、定义替换、级联失效和刷新都先持久化意图，再提交领域事务。刷新意图对应独立的已提交 `REFRESHING` 状态，因此进程在计算或最终发布前被强制终止时，数据库仍保留可审计事实。

重启时 `initialize()` 在一个事务中扫描 `intents.status='RUNNING'`：

- 刷新意图：把仍为 `REFRESHING` 的所选视图改为 `PARTIAL`，缺口为 `INCOMPLETE_REFRESH`；保留最后完整物化版本；请求标为 `FAILED`。
- 批次/定义意图：因最终事务未提交，直接标为 `ABORTED`，领域数据保持上一完整提交状态。
- 写入恢复提交、逐视图 `RECOVERED_PARTIAL` 事件和 `recovery_reports`。

恢复报告包含 `persisted_fact_only=true`、恢复前最后完整提交序号、未完成意图和逐视图最后物化版本。报告不声称进程在强制终止时生成了完整退出报告；该报告只在重启后依据持久化事实写入。

## SQLite 事实表

- `base_tables`、`base_snapshots`、`base_rows`、`base_row_history`
- `graph_versions`
- `view_states`、`view_versions`、`view_group_index`、`view_rows`
- `state_transitions`、`invalidation_events`
- `intents`、`commits`、`refresh_requests`、`recovery_reports`

数据库位于 `data/workbench.db`（演示为 `data/demo.db`），可通过删除 `*.db`、`*.db-wal`、`*.db-shm` 复现初始状态。

## 可视化页面

根路径页面完全离线，展示：

- 依赖图拓扑层和边摘要；
- 每个视图的 `FRESH`/`STALE`/`REFRESHING`/`PARTIAL`/`INVALID` 徽标、物化版本、物化时图版本与缺口；
- 最近失效/重建传播、触发标识、深度、受影响分组和重算组数；
- 点击视图卡片查看绑定版本的行级内容与分组索引；
- “刷新全部 STALE/PARTIAL”按钮。

## 实际验证结果

### 自动化测试

在本次工作区执行 `python -m pytest -q`：

```text
..................                                                       [100%]
19 passed
```

输出仅有 Starlette/FastAPI 在当前 Python 版本上的第三方弃用警告，无失败。测试覆盖：

- 固定样本逐字节可复现；
- 新增行完整校验与修正行仅提交变化列的事务内合并；
- 图分层、环、未知引用；
- 陈旧批次无副作用、非法状态迁移无副作用；
- 批次级联失效、可增量聚合分组修补、不可增量聚合整表重算；
- 依赖缺口和计算失败进入 `PARTIAL`；
- 定义替换、图历史、级联重建；
- 刷新期间多只读查询版本绑定、查询跨越版本替换；
- 并发重复刷新幂等与冲突；
- 子进程强制终止后恢复；
- HTTP 业务码、页面可达和错误 JSON 结构。

### 端到端结果

最近一次 `python scripts/demo.py` 的真实结果见 `artifacts/demo_result.json`，摘要如下：

- 回环端口：动态选择 `127.0.0.1` 空闲端口并成功启动服务。
- 首次全量构建：7 个视图全部 `FRESH`。
- 修正批次：基表 `sales` 从版本 1 提交为版本 2；6 个直接/下游视图级联 `STALE`，独立 `returns` 分支保持 `FRESH`。
- 陈旧探测批次：被拒绝，业务码 `E_STALE_BASE`，当前基表版本仍为 2。
- `v_region_revenue`：`incremental_groups`，受影响分组 `east/north/south`，重算 3 组，保留 1 组；`west` 分组行和 SHA-256 摘要均与上一版本相同。
- `v_region_peak`：`full_recompute`，重算 4 组，依据函数 `max, quantile` 不可在修正/删除下安全合并。
- 定义整体替换：图版本 1 → 2，新增 `v_region_order_bands`，变化及下游 4 个视图级联 `STALE` 后刷新为 `FRESH`；图 v1 历史仍可查询。
- 强制终止：独立进程持久化刷新意图后以退出码 97 被强制终止；重启报告状态 `RECOVERED`，8 个所选视图进入 `PARTIAL`，缺口 `INCOMPLETE_REFRESH`，请求状态 `FAILED`，最后完整物化版本 2 保留且仍可查询。

## 约束与说明

- 本项目面向本地单进程运维工作台；请使用默认的单个 uvicorn worker。进程内单写者锁负责并发，SQLite `BEGIN IMMEDIATE` 负责数据库级写入串行化。
- 服务默认无认证并只绑定 `127.0.0.1`。不要直接暴露到不受信任网络。
- 历史视图版本当前不自动回收，便于版本一致查询和审计。若未来增加保留策略，淘汰版本的查询应返回 `E_VERSION_RETIRED`。
- 最大/分位等不可增量聚合选择整视图重算优先保证正确性，不以牺牲新鲜度换局部更新。
- 强制终止不会执行 Python 清理逻辑；恢复结论只来自 SQLite 已提交的意图、状态、版本和提交序号。
