# 本地 SQL 逻辑计划引擎

这是一个面向 Windows/本地执行的小型 SQL 逻辑计划引擎。它把一条 SELECT
语句解析为位置可追踪的 AST，转换为逻辑计划，执行确定性规范化，再用本地固定
SQLite 样本验证改写前后结果一致。

核心只依赖 Python 标准库：`sqlite3`、`http.server`、`json`、`threading` 等。
测试使用标准库 `unittest` 编写，同时可被 `pytest` 发现和执行。

## 交付内容

- SQL 子集解析器：SELECT、投影别名、单表/多表 FROM、JOIN、WHERE 布尔表达式、
  `IS [NOT] NULL`、`IN`/`NOT IN`、GROUP BY、COUNT/SUM/MIN/MAX、HAVING、
  ORDER BY、LIMIT。
- 逻辑计划模型：`Scan`、`Filter`、`Project`、`Aggregate`、`Sort`、`Limit`、`Join`。
- 稳定序列化：规范 JSON 可逐字段往返解析；节点含稳定 ID 与 `origin_ids` 血缘。
- 规范化规则：谓词下推、投影裁剪、常量折叠；每条应用/拒绝均有节点前后快照。
- 安全保护：HAVING、聚合输入、外连接、排序键等冲突会拒绝改写并记录冲突节点。
- 异常注入：`E_PARSE`、`E_PLAN_TOO_LARGE`、`E_INVALID_AGGREGATE`、
  `E_REWRITE_ABORTED`，全部由请求参数触发。
- 本地 HTTP 服务、请求级幂等、同语句并发合并、SQLite 意图/版本持久化。
- 强制终止后的启动恢复报告，以及仅在安全退出时生成的完整退出摘要。
- 固定样本、固定 SQL 清单、端到端演示、重复运行一致性验证。

## 快速开始（Windows PowerShell）

要求 Python 3.11+，且 `python`、`pip` 在 PowerShell 中可用。

```powershell
cd <项目目录>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt

python -m sqlplan_engine doctor
python -m sqlplan_engine init
python -m pytest -q
python -m sqlplan_engine demo
```

若 PowerShell 阻止激活脚本，可只对当前进程执行：

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

也可以直接运行提供好的脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\check.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run-demo.ps1
```

### POSIX / WSL 本地验证

```bash
python3 -m sqlplan_engine doctor
python3 -m unittest discover -v
python3 -m sqlplan_engine demo
```

本仓库的实际验证环境为 WSL/Linux、Python 3.14.4、SQLite 3.46.1；当前容器没有
`pip`，因此实际测试用 `unittest` 执行。测试文件不依赖 pytest 专有 API，安装
pytest 后运行 `python -m pytest -q` 即可。Windows 路径与进程逻辑均使用标准库
跨平台 API，PowerShell 脚本已提供；最终验收命令可在 Windows 上直接重复。

## 固定样本与语句清单

样本生成器：`sqlplan_engine/sample.py`。

```bash
python3 -m sqlplan_engine init
```

会生成 `data/sample.sqlite3`，包含固定表：

- `departments(id, name, region, budget)`
- `employees(id, name, dept_id, salary, active, manager_id)`
- `projects(id, dept_id, title, cost)`

所有 INSERT 顺序、数值、字符串均为常量；重复执行生成相同数据。固定清单在
`samples/statements.json`，包含正常改写和自然非法聚合两类用例。

## 目录

```text
sqlplan_engine/
  lexer.py       位置感知词法分析
  parser.py      递归下降语法分析
  ast.py         位置感知 AST
  planner.py     AST -> 逻辑计划
  plan.py        节点模型、规范序列化、摘要、schema 校验
  expressions.py 规范表达式、遍历、常量折叠
  rewriter.py    谓词下推、投影裁剪、常量折叠、安全拒绝
  executor.py    本地参考执行器，用于验证语义等价
  store.py       意图、版本、请求、语句缓存、恢复
  service.py     单写者提交、异常注入、幂等与业务码
  server.py      标准库 ThreadingHTTPServer
  demo.py        子进程强制终止/重启恢复端到端演练
  cli.py         命令行入口
samples/         固定 SQL 清单
data/            固定 SQLite 样本
tests/           unittest/pytest 兼容测试
scripts/         Windows 与 POSIX 脚本
artifacts/       demo_report.json 等实际验证产物
```

## 计划模型与稳定序列化

节点结构：

```json
{
  "type": "Filter",
  "id": "n0002",
  "children": [],
  "attributes": {},
  "origin_ids": ["n0002"]
}
```

- `type` 只能是 `Scan/Filter/Project/Aggregate/Sort/Limit/Join`。
- `id` 在单次构建中从 `n0001` 开始确定性分配。
- 改写移动的节点保留原 ID；新节点写入包含来源 Filter 的 `origin_ids`。
- `rewrite_log` 的每条记录包含 `rule`、`status`、`node_id`、`conflict_node`、
  `before`、`after` 和原因。
- `node_mapping` 给出改写前节点到最终节点集合的映射，便于比较差异。

`normalized_plan` 是键排序、压缩分隔符、UTF-8 的规范 JSON；重复执行和
`deserialize_plan()` 往返均得到完全相同的文本。`plan_text` 是面向人的树形展示，
不用于往返解析。

主要属性：

| 节点 | 属性 |
|---|---|
| Scan | `table`, `alias`, `columns` |
| Filter | `source` (`WHERE`/`HAVING`), `predicate` |
| Project | `items[]`，含内部 `name`、SQL 可见 `output_name`、`expr`、`visible` |
| Aggregate | `group_by[]`, `aggregates[]` |
| Join | `join_type`, `condition` |
| Sort | `keys[]`，含表达式、方向、NULL 顺序 |
| Limit | `limit` |

内部列名与 SQL 可见列名分离，因此 `SELECT d.name, e.name` 既能保持唯一内部引用，
又能返回两个原始 `name` 列名。

## 逻辑计划顺序

```text
Scan / Join
  -> WHERE Filter
  -> Aggregate
  -> HAVING Filter
  -> Project（必要时附带隐藏 ORDER BY 键）
  -> Sort
  -> 最终 Project（移除隐藏键）
  -> Limit
```

未投影的 ORDER BY 表达式会生成隐藏 Project 项，排序后再由最终 Project 移除。

## 规范化规则

### 谓词下推

- 将 `AND` 拆成可独立移动的合取项。
- INNER/CROSS JOIN：仅引用一侧的谓词推到该侧，最终落在 Scan 正上方。
- LEFT JOIN：仅左表谓词可下推；右表谓词保留在 Join 上方。
- RIGHT JOIN：仅右表谓词可下推。
- FULL JOIN：保守地不向任一侧下推。
- HAVING：不允许跨 Aggregate 下推，记录拒绝原因和 Aggregate 冲突节点。

### 投影裁剪

- 汇总所有 Filter、Join、Aggregate、Project、Sort 的列引用。
- Scan 只保留仍被存活表达式引用的列；`COUNT(*)` 场景 Scan 列可为空。
- 裁剪后重新执行计划 schema 校验；若发现任何丢失引用，立即恢复并记录拒绝。

### 常量折叠

- 折叠纯常量算术、比较、布尔运算、`IS NULL`、`IN`。
- 折叠 `x + 0`、`x - 0`、`x * 1`、`x / 1` 等保持 NULL 语义的安全恒等式。
- `WHERE TRUE`/`HAVING TRUE` 的 Filter 可移除；`FALSE` 不静默删除。
- 最多固定轮折叠，并有总检查预算，不会无限循环。

### 安全拒绝而非静默改错

以下依赖会被保护，尝试裁剪时会产生 `status: "rejected"` 记录：

- HAVING 引用的分组或聚合输入列。
- 聚合分组表达式和聚合参数。
- 连接条件与可能影响外连接语义的列。
- ORDER BY 使用的隐藏/未投影排序键。

拒绝记录一定含规则名、原因和 `conflict_node`。例如：

```text
refusing to prune employees.salary: sort key would be lost
```

本地参考执行器会分别执行改写前后计划并比较结果，响应字段
`semantic_equivalence.original_plan_equals_normalized_plan_result` 记录事实。

## HTTP 服务

启动：

```powershell
python -m sqlplan_engine serve --host 127.0.0.1 --port 8765 `
  --sample-db data\sample.sqlite3 --store-db runtime\plans.sqlite3
```

接口：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康、存储计数、配置 |
| GET | `/v1/samples` | 固定 SQL 清单与样本路径 |
| POST | `/v1/plans` | 提交/复用计划 |
| GET | `/v1/plans/{request_id}` | 读取已提交计划结果 |
| GET | `/v1/intents?request_id=...` | 该请求的全部意图事实 |
| GET | `/v1/intents/{intent_id}` | 单条意图事实 |
| GET | `/v1/recovery-report` | 最近一次启动恢复报告 |
| POST | `/v1/admin/shutdown` | 本地安全退出并生成完整退出摘要 |

### 提交示例

PowerShell：

```powershell
$body = @{
  sql = "SELECT name, salary FROM employees WHERE active = 1 ORDER BY salary DESC LIMIT 3"
  request_id = "manual-demo-001"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/v1/plans `
  -ContentType "application/json" -Body $body
```

成功响应关键字段：

```json
{
  "ok": true,
  "business_code": "OK",
  "request_id": "manual-demo-001",
  "version_id": 1,
  "reused": false,
  "normalized_plan": "{...}",
  "plan_text": "Limit(...)",
  "node_summary": [],
  "rewrite_log": [],
  "rejections": [],
  "node_mapping": {},
  "query_result": {"columns": [], "rows": []},
  "semantic_equivalence": {
    "original_plan_equals_normalized_plan_result": true
  }
}
```

错误响应始终是 JSON：

```json
{
  "ok": false,
  "business_code": "E_PARSE",
  "message": "expected ... (line 1, column 42)",
  "context": {
    "line": 1,
    "column": 42,
    "offset": 41,
    "sql_excerpt": "..."
  },
  "request_id": "bad-001"
}
```

## 异常注入

注入只通过请求参数生效，不需要修改源码。支持 JSON 对象、查询参数和布尔快捷参数。

| `inject.kind` | HTTP | 业务码 | 注入事实 |
|---|---:|---|---|
| `malformed_sql` | 400 | `E_PARSE` | 在此次请求的 SQL 末尾追加无效 `WHERE`，必然产生带位置的语法错误 |
| `plan_too_large` | 413 | `E_PLAN_TOO_LARGE` | 请求级 `limit`（默认 1）与全局上限取小，实际节点数超限 |
| `invalid_aggregate` | 422 | `E_INVALID_AGGREGATE` | 加入聚合校验探针，报告非分组列冲突；自然非法聚合也返回该码 |
| `rewrite_aborted` | 409 | `E_REWRITE_ABORTED` | `abort_after` 中断标记（0 表示立即中断）；不发布半成品计划 |
| `crash_after_intent` | 无响应 | 进程退出 70 | 仅演练使用：落盘 pending 意图后真实 `os._exit(70)` |

JSON 形式：

```json
{"sql":"SELECT ...","request_id":"inj-1","inject":{"kind":"plan_too_large","limit":1}}
```

查询参数形式：

```text
POST /v1/plans?inject=malformed_sql&request_id=inj-2
```

快捷参数：

- `inject_malformed_sql=true`
- `inject_plan_node_limit=1`
- `inject_invalid_aggregate=true`
- `inject_rewrite_abort_after=0`

业务码：

| 业务码 | 含义 |
|---|---|
| `OK` | 成功 |
| `E_PARSE` | 词法/语法错误，含行、列、偏移与 SQL 片段 |
| `E_PLAN_TOO_LARGE` | 计划节点数超限，含实际值和上限 |
| `E_INVALID_AGGREGATE` | 非法聚合引用/嵌套/聚合位置错误 |
| `E_REWRITE_ABORTED` | 注入中断或改写预算耗尽，无半成品发布 |
| `E_SEMANTIC` | 未知表/列、歧义列等语义错误 |
| `E_REQUEST_CONFLICT` | 同一 request_id 提交了不同 SQL |
| `E_NOT_FOUND` | 请求/意图/路由不存在 |
| `E_BAD_REQUEST` | 请求格式或注入参数非法 |
| `E_SAMPLE_NOT_READY` | 固定样本数据库尚未生成 |
| `E_INTERNAL` | 兜底内部错误，仍返回 JSON，不泄漏堆栈 |

## 并发、幂等与持久化

一个进程内使用单个写者锁；读接口不拿写锁，只读取已提交 SQLite 行。

提交顺序：

1. 检查同一 `request_id` 是否已提交；相同 SQL 直接复用，不同 SQL 返回冲突。
2. 无注入时检查语句缓存；相同 SQL 的并发请求等待单写者，后到者复用版本。
3. 在 `intents` 表写入 `pending` 意图，并提交事务。
4. 构建、校验、改写、执行验证。
5. 在一个事务中写入 `versions`、`requests`、`statement_runs`，并把意图改为
   `committed`。
6. 失败时把意图改为 `aborted`；版本表和请求表不会出现半改写结果。

读者只能看到事务提交后的完整 `requests.result_json`。语句缓存命中时，不同请求
复用同一 `version_id` 和同一规范化计划。

## 中断恢复

服务启动时扫描 `intents.status='pending'`：

- 没有完整版本：标记 `aborted`，动作 `aborted_without_committed_version`。
- 已有完整版本：保留最后版本，动作 `aborted_and_retained_last_complete_version`。
- 报告写入 `recovery_reports`，包含扫描数、恢复数、恢复后 pending 数、逐意图
  before/after 状态、SQL 哈希和最后完整版本号。

恢复报告明确设置：

```json
{
  "complete_exit_report_available": false,
  "complete_exit_report_reason": "A forced termination can leave only persisted intent and version facts; no complete in-process exit report is implied."
}
```

强制终止不会假装生成了完整退出报告。只有经过信号处理或
`POST /v1/admin/shutdown` 的安全退出，CLI 才写出
`safe_exit_summary.complete = true` 的完整退出摘要。

## 可复现端到端演练

```powershell
python -m sqlplan_engine demo
```

演示会：

1. 重新生成固定样本并清空演示运行库。
2. 子进程启动真实本地 HTTP 服务。
3. 提交 5 条正常语句，输出规范化计划、逐节点摘要、规则日志、拒绝日志和查询结果。
4. 触发四类异常注入并核对不同业务码。
5. 6 个请求并发提交同一 SQL，核对仅执行一次、版本与计划完全一致。
6. 同一 `request_id` 再提交，核对复用同一版本。
7. 通过 `crash_after_intent` 写入 pending 意图后强制退出子进程（退出码 70）。
8. 重启服务，核对 pending -> aborted、完整版本数不变、强杀前无退出摘要。
9. 通过本地安全退出接口关闭服务，核对完整退出摘要此时才生成。
10. 写入 `artifacts/demo_report.json`。

### 实际验证结果

- `python3 -m unittest discover -v`：33 项全部通过。
- 正常语句：5/5 的改写前后计划执行结果一致，且与原始 SQLite SQL 结果一致。
- 四类注入：`E_PARSE` 400、`E_PLAN_TOO_LARGE` 413、
  `E_INVALID_AGGREGATE` 422、`E_REWRITE_ABORTED` 409。
- 并发同语句：8 个单元测试并发请求中 1 个实际执行、7 个复用；演示 HTTP 并发为
  1 个实际执行、5 个复用，版本 ID 相同。
- 强制终止：子进程退出码 70；意图 `pending -> aborted`；版本数 `7 -> 7`；
  强杀后不存在完整退出摘要。
- 安全退出后完整摘要生成成功。
- demo 报告连续两次运行字节级一致，SHA-256：
  `dad8e1cdbcb8c1b13ecd3a453d796a961988a4c3d9ec0ffdb2c3ef947f52c016`。

完整事实保存在 `artifacts/demo_report.json`。

## 测试覆盖

```bash
python3 -m unittest discover -v
# 安装 pytest 后等价于：
python3 -m pytest -q
```

- 解析语法、位置、错误码。
- 计划模型、稳定序列化、往返、节点 ID、映射、重复可见列名。
- 三类改写、外连接安全、HAVING/排序键拒绝、语义等价。
- 四类注入、自然非法聚合、半成品不可见。
- 请求幂等、语句复用、并发单次执行。
- pending 恢复、已有版本保留、真实子进程强制退出恢复。
- HTTP JSON 错误、查询参数注入、恢复报告接口。

## 确定性说明

- 样本内容和语句清单固定。
- 计划节点 ID、属性排序、节点摘要、规则日志顺序固定。
- HTTP 响应 JSON 使用键排序。
- demo 每次重置运行库；报告剔除仅用于审计的时间戳字段。
- 强制退出后的恢复来自 SQLite 的意图/版本事实，而不是内存退出状态。

## 边界

- SQL 子集有意保持小：不实现子查询、窗口函数、`UNION`、`DISTINCT`、`LIKE`、
  `OFFSET`、多语句事务或任意标量函数。
- 逻辑计划执行器用于本地语义校验，不是高性能数据库执行器。
- 未鉴权的本地 HTTP 服务仅应绑定 `127.0.0.1`；`/v1/admin/shutdown` 也是本地
  演练接口，不应暴露到不可信网络。
- 不使用 Docker、外部数据库或在线服务。

