# 资源编排依赖图诊断工作台 — 技术设计说明

## 1. 范围与运行方式

工作台面向服务资源编排依赖图，提供本地确定性的提交（整份快照 / 增量片段）、
派生结构维护（拓扑层级、下游影响集合、批次归属、跨资源权重一致度）与低延迟诊断查询。

- 纯本地运行：FastAPI + uvicorn 只绑定 `127.0.0.1`，SQLite 文件落盘，无任何线上服务或外部账号依赖；
- 不使用容器；Windows 使用 `scripts/run_server.bat`，POSIX 使用 `scripts/run_server.sh`；
- Python 3.10+（开发验证环境为 Python 3.14），依赖见 `requirements.txt`。

启动：

```bat
pip install -r requirements.txt
scripts\run_server.bat
```

数据库默认位置：`data/graph_bench.db`（可用环境变量 `GRAPH_BENCH_DB` 覆盖），
启用 SQLite WAL 模式，伴随 `-wal` / `-shm` 文件；恢复报告同时写入 `logs/recovery.log`。

## 2. 模块划分

| 模块 | 职责 |
| --- | --- |
| `graph_bench/canonical.py` | 确定性 JSON 规范化与 SHA-256 摘要 |
| `graph_bench/models.py` | 资源、快照、增量片段的不可变数据模型 |
| `graph_bench/validation.py` | 规格校验与快照信封校验 |
| `graph_bench/graph.py` | 拓扑分层、Tarjan 环检测、影响面、批次、权重一致度 |
| `graph_bench/storage.py` | SQLite schema、WAL、单写者锁、意图日志、崩溃点 |
| `graph_bench/service.py` | 提交事务边界、幂等、级联重建、预算查询、恢复 |
| `graph_bench/api.py` | 回环 HTTP 接口与业务码翻译 |

## 3. 数据模型与校验契约

资源字段：`id`（非空字符串、提交内唯一）、`replicas`（非负整数）、
`parents`（父资源标识数组）、`weight_milli`（千分比整数，合法区间 `[1, 1000]`）、
`desired_version`（非空字符串）。

- 标识重复、父引用不存在、自依赖、重复父引用、权重越界 → `E_SPEC_INVALID`（HTTP 422），
  错误详情携带每条违规的规则与资源标识；校验在写意图之前完成，失败不修改任何已提交状态。
- 快照信封携带 `version`（严格单调递增）、`generated_at`、`checksum`。
  摘要只覆盖 `version / generated_at / resources`（父列表先排序），规范化规则见
  `canonical.py`：字典按 key 排序、紧凑分隔符、UTF-8、`allow_nan=False`。
- 摘要不匹配或版本不单调 → `E_SNAPSHOT_INVALID`（HTTP 409），旧快照保持完全可用。

## 4. 增量片段与版本推进

片段结构：`base_version + upserts[] + removals[] (+ generated_at)`。

- `base_version != 当前版本` → `E_STALE_BASE`（HTTP 409），状态不变；
- 同一基础版本的两个并发片段在单写者锁下串行，第二个必然读到已推进的当前版本而收到 `E_STALE_BASE`；
- 应用顺序：删除校验 → 合并 upsert/removal → 对合并后的完整资源集合重新做规格校验 →
  新版本号固定为 `当前版本 + 1` → 全量重算派生结构 → 整体提交；
- 成功响应显式返回 `base_version / new_version / touched_resources / cascade_invalidated`，
  调用方据此识别版本变化。

## 5. 图算法

边方向为 `父 -> 子`，父先于子编排。

### 5.1 拓扑分层与环

- Kahn 算法按层剥离入度为 0 的节点，同层节点按标识排序，保证父层级严格小于子层级；
- 无法剥离的节点即“被环污染”节点（环成员及其唯一可达下游），进入 `blocked_resources`；
- Tarjan SCC 找环并展开为有序环路径：自环输出 `[x, x]`，多节点环从最小标识出发沿边回到起点，
  通过 HTTP 查询时返回 `E_DEPENDENCY_CYCLE`（HTTP 200）并在 `details` 中附带环路径、
  受阻清单与未污染资源的部分分层结果。

### 5.2 下游影响集合

对每个有可信层级的资源做记忆化 DFS，输出排序后的全部传递后代（不含自身）。
环上节点不参与，其影响集合留空并以 `status=blocked_by_cycle` 表达。

### 5.3 批次归属

批次与层级一一对应：`batch-k` 即第 `k` 层，批次内资源按标识排序。

### 5.4 跨资源权重一致度

共享同一父资源的直接子节点互为兄弟；无父节点的根资源组成根分组。

```
组内一致度 = 1 - 组内 weight_milli 的总体标准差(ddof=0) / 1000
```

单成员分组为 `1.0`；同一资源属于多个兄弟分组时取最低分；结果四舍五入到 6 位小数。
该定义完全确定，可在样本与测试中复现核对。

## 6. 级联失效与原子重建

派生结构不做局部打补丁：每次提交都对新版本的完整资源集合一次性全量重算，
并与资源、依赖边在**同一个 `BEGIN IMMEDIATE` 事务**内写入：

1. 第一阶段事务：写 `intent_log`（PENDING）并独立提交，保证意图先于任何状态变化落盘；
2. 第二阶段事务：写 `snapshots / resources / edges / derived / graph_meta`，
   更新 `meta.current_version`，把意图置为 COMMITTED，最后 `commit`。

WAL 下读者打开自己的读事务，只能看到事务前或事务后的完整版本，
因此不存在“上游已更新、下游仍旧值”的中间可见状态。
级联失效集合 = 被改动资源本身 ∪ 旧版本中其下游（见提交响应的 `cascade_invalidated`）。

## 7. 单写者、多读者与幂等

- 进程内 `threading.RLock` + SQLite `BEGIN IMMEDIATE` 构成单写者通道，所有提交串行；
- 读路径不拿进程锁，仅开启短读事务，WAL 保证读写互不阻塞；
- 幂等键为请求体 `request_id`；服务端对规范化请求体计算 SHA-256 指纹存入意图表：
  - 同 `request_id` 同指纹：后续调用等待写锁后复用首次结果，响应带 `idempotent_replay=true`；
  - 同 `request_id` 不同指纹：`E_CONFLICTING_REQUEST`（HTTP 409）；
  - 崩溃后遗留的 ABORTED 意图允许同内容重试（重新置 PENDING 后执行）。

## 8. 诊断查询与一致性契约

所有查询响应都标注 `snapshot_version`；显式传 `version` 可读取历史完整版本。

- `GET /views/layers`：层级、环与受阻清单；
- `GET /views/batches?budget_ms=`：预算内返回完整批次；超预算返回已完成批次、
  `completed=false` 与 `remaining_batches`，数据全部来自同一版本；
- `GET /resources/{id}/impact?budget_ms=&delay_per_item_ms=`：预算内逐个产出下游，
  超时返回部分 `downstream` 与 `remaining`，二者不重叠且并集等于完整集合；
  `delay_per_item_ms` 仅用于确定性地验证预算行为；
- 同一版本重复查询逐位一致：所有集合排序、字典按 key 输出、浮点固定 6 位小数。

## 9. SQLite Schema 与意图日志表

表：`meta`、`snapshots`、`resources`、`edges`、`derived`、`graph_meta`、
`intent_log`、`recovery_reports`（建表语句以 `storage.py` 中 `SCHEMA_SQL` 为准）。

意图日志表结构（中断恢复的扫描入口）：

```sql
CREATE TABLE intent_log (
    intent_id      TEXT PRIMARY KEY,
    kind           TEXT NOT NULL,           -- snapshot / increment
    request_hash   TEXT NOT NULL,
    status         TEXT NOT NULL,           -- PENDING / COMMITTED / ABORTED
    base_version   INTEGER,
    target_version INTEGER,
    detail_json    TEXT NOT NULL,
    error_code     TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
```

## 10. 强制终止与重启恢复

测试用崩溃点由环境变量 `GRAPH_BENCH_CRASH_POINT` 控制
（`before_version_insert` / `after_derived_before_commit` / `before_commit`），
命中后服务调用 `os._exit(86)` 模拟 `kill -9`。此时第二阶段事务未 commit，
SQLite 下次打开数据库时自动丢弃未提交页。

启动时 `BenchService.recover()`：

1. 扫描所有 PENDING 意图；
2. 若目标版本已完整落盘则补齐为 COMMITTED（防御性分支），否则标记 ABORTED，
   错误码 `ABORTED_AFTER_CRASH`；
3. `meta.current_version` 不被改写，天然指向最后一个完整版本；
4. 生成恢复报告并写入 `recovery_reports` 表与 `logs/recovery.log`。

报告只引用数据库中已落盘事实（PENDING 意图、完整版本列表、恢复前后当前版本），
不承诺在强制终止瞬间生成完整退出报告。端到端验证见 `tests/test_recovery.py`，
它通过真实子进程触发崩溃再重启，而非内存模拟。

## 11. HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查与当前版本 |
| POST | `/snapshots` | 提交整份快照（body: `request_id + payload`） |
| POST | `/increments` | 提交增量片段 |
| GET | `/snapshots/current` | 当前完整快照 |
| GET | `/snapshots/{version}` | 指定历史版本快照 |
| GET | `/views/layers` | 拓扑分层 / 环契约 |
| GET | `/views/batches` | 批次视图（支持预算） |
| GET | `/resources/{id}` | 资源规格 + 派生结构 |
| GET | `/resources/{id}/impact` | 下游影响面（支持预算） |
| GET | `/intents` / `/intents/{id}` | 意图日志查询 |
| GET | `/recovery` | 启动恢复报告与历史恢复记录 |

业务码：`E_SPEC_INVALID`、`E_SNAPSHOT_INVALID`、`E_STALE_BASE`、
`E_DEPENDENCY_CYCLE`、`E_CONFLICTING_REQUEST`、`E_INTENT_NOT_FOUND`、`E_NO_SNAPSHOT`。
