# 本地确定性多表连接顺序优化器

这是一个面向本地确定性样本数据的左深连接顺序优化与代价评估组件。服务仅绑定 `127.0.0.1`，通过 HTTP 接收查询标识、快照版本和计划模式，返回完整代价分解。

## 交付内容

- `join_planner/`：快照校验、选择率、左深枚举、日志域基数、定点代价、并发服务、持久化意图日志和 FastAPI 接口。
- `data/snapshots/`：三版固定统计快照，均带版本、生成时间、SHA-256 摘要、行数、NDV 和相关列系数。
- `data/queries/queries.json`：三条确定性查询，分别覆盖多表等值/范围、相关列修正和自连接。
- `scripts/generate_samples.py`：重新生成固定样本及摘要。
- `scripts/run_server.py`：本地 uvicorn 启动入口，并写 PID 文件。
- `scripts/kill_server.py`：按 PID 优雅或强制终止本地服务；Windows 使用 `taskkill /F`，POSIX 使用 `SIGKILL`。
- `scripts/demo.py`：真实启动 uvicorn，加载 v2 快照，输出三条查询的 fast/exact 顺序与代价。
- `scripts/crash_recovery_demo.py`：真实启动服务、等待构建意图落盘、强杀进程、重启并打印恢复报告。
- `tests/`：23 个 pytest 用例，包含 HTTP、并发、版本隔离和真实进程强杀恢复。

## 环境与安装

本机实际验证环境：

- Python `3.14.4`，满足 `>=3.11`
- FastAPI `0.165.0`
- uvicorn `0.54.0`
- pytest `9.2.0`
- 未使用 Docker 或其他容器，未访问外部数据源

Windows PowerShell：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\generate_samples.py
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\run_server.py --host 127.0.0.1 --port 8765
```

Linux/macOS：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/generate_samples.py
.venv/bin/python -m pytest -q
.venv/bin/python scripts/run_server.py --host 127.0.0.1 --port 8765
```

Windows 也可执行 `scripts\start_server.bat`。默认只监听回环地址。若端口被占用，pytest 与演示脚本会自动申请空闲回环端口。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查、当前快照版本、关系上限、快速预算 |
| `GET` | `/v1/snapshots/current` | 当前快照的版本、摘要和关系行数 |
| `POST` | `/v1/snapshots/load` | 激活快照目录内的新版本，请求体为 `{"filename":"snapshot_v2.json"}` |
| `GET` | `/v1/queries` | 列出本地查询目录 |
| `POST` | `/v1/plans` | 构建或复用计划 |
| `GET` | `/v1/plans/{query_id}` | 并发读取已物化计划；可选 `mode`、`snapshot_version` 查询参数 |
| `GET` | `/v1/recovery/report` | 最近一次持久化恢复报告 |

计划请求示例：

```json
{
  "query_id": "q_product_priority",
  "snapshot_version": 2,
  "mode": "exact"
}
```

`mode` 为 `fast` 或 `exact`。完整请求键是 `(query_id, snapshot_version, mode)`；完全相同的请求并发提交时只由一个请求执行构建，其余等待并复用同一结果。响应头 `X-Plan-Result` 为 `built` 或 `reused`。同一查询的 fast/exact 结果使用不同计划并分别缓存。

所有失败响应均为 JSON，顶层一定包含业务码：

```json
{
  "code": "E_VERSION_RETIRED",
  "message": "The requested snapshot version is not active.",
  "details": {
    "requested_version": 1,
    "current_version": 2
  }
}
```

已覆盖的业务码：

- `E_SNAPSHOT_INVALID`：摘要、结构、NDV、范围或版本单调性校验失败；内存中的旧快照不变。
- `E_SEARCH_SPACE_TOO_LARGE`：关系数超过 `max_relations`，响应给出行数、上限和 `n!` 候选规模，不静默截断。
- `E_VERSION_RETIRED`：新请求引用非当前版本；若请求在提交时已绑定旧版本，则可继续完成该不可变版本，响应中以 `snapshot_binding` 标明。
- `E_INVALID_QUERY`：请求结构、谓词、列、关系连通性或非法笛卡尔分支校验失败。
- `E_PLAN_NOT_FOUND`：没有对应的已物化计划。
- `E_INTERNAL`：未预期内部错误，不返回堆栈。

## 样本格式

快照结构节选：

```json
{
  "version": 1,
  "generated_at": "2026-01-10T00:00:00Z",
  "checksum_algorithm": "sha256",
  "checksum": "cdd23ad8...",
  "relations": {
    "orders": {
      "rows": 100000,
      "columns": {
        "customer_id": {"ndv": 9000},
        "amount": {"ndv": 2400, "min": 1, "max": 5000}
      },
      "correlations": [
        {"columns": ["priority", "status"], "coefficient": 0.82}
      ]
    }
  }
}
```

摘要按除 `checksum` 外的完整 JSON 做排序键、紧凑分隔符的 SHA-256。加载顺序严格为“解析与校验 -> 获取写锁 -> 比较版本 -> 整体替换”；失败不会触碰当前快照。相同或更旧版本也返回 `E_SNAPSHOT_INVALID`。

查询支持：

- 关系字符串，或 `{"alias":"e1","table":"employees"}`；别名是搜索单位，表名是统计快照键。
- `equality`：列列等值或列字面量等值。
- `range`：单列范围，或两列范围的域重叠。
- `self_join`：两个不同别名指向同一张表。

两条关系之间只有存在二元谓词边时才可扩展左深前缀。查询关系图必须连通；否则返回 `E_INVALID_QUERY`，不会生成笛卡尔积。

## 估算与代价

选择率：

- 等值：`1 / max(left_ndv, right_ndv)`；字面量：`1 / ndv`。
- 范围：查询区间与快照 `[min,max]` 的重叠宽度除以列域宽度，结果截断到 `[min_selectivity, 1]`。
- 自连接：`1 / max(outer_ndv, inner_ndv)`。
- 声明相关列系数时，在该组谓词同时可用后只乘一次系数；每步合并选择率再截断到 `(0,1]`。
- 未声明相关系数的谓词标记 `assumption_source=independence_assumption`；所有假设也列在计划的 `assumptions` 中。

左深基数使用对数域：

```text
log(C_i) = log(C_{i-1}) + log(new_rows)
           + sum(log(predicate_selectivity))
           + sum(log(correlation_coefficient))
```

每个谓词通过已应用谓词 ID 集合保证恰好应用一次。中间基数截断到 `[1, max_cardinality]`，每个步骤同时输出 `log2_estimated_rows`、`estimated_rows` 和有限 `finite_upper_bound`。

代价定义：

```text
scan               = sum(relation_rows * 0.002)
join               = sum((previous_estimated_rows + new_relation_rows) * 0.010)
intermediate_write = sum(estimated_rows_after_join * 0.004)
total              = scan + join + intermediate_write
```

`scan`、`join`、`intermediate_write`、`total` 以及每个步骤分项均以 6 位定点小数输出，舍入为 `ROUND_HALF_EVEN`，每分项最大绝对舍入误差为 `0.0000005`。另输出 12 位定点 `comparison_total`，仅用于计划比较。

比较使用相对容差 `1e-9`。差异在容差内视为平局，以 `selected_order` 的升序字典序稳定取胜；因此同输入重复构建逐位一致。

快速模式最多评估配置的 `fast_candidate_budget` 个完整候选，输出 `mode=fast`、`candidates_evaluated`、`pruned_branches` 和 `optimality_guarantee`。`pruned_branches` 的定义是未评估的完整合法左深顺序数。精确模式枚举全部合法顺序。快速模式只评估精确搜索的子集，其代价不会低于全局最低代价（比较仍使用上述相对容差）。

## 并发、版本与恢复

- 计划构建由进程内单一构建锁串行化；已物化计划由多个 HTTP 读取线程并发读取。
- 请求在提交时捕获不可变快照对象。构建期间切换快照不会混用统计；计划携带 `snapshot_binding.captured_checksum`，并在版本已切换时标记 `retired_during_build=true`。
- 每次有效构建开始前，先在 `runtime/intents/` 原子写入 `started` 意图并 `fsync`。
- 计划先原子写入 `runtime/plans/`，再将意图原子改为 `completed`。
- 启动时扫描意图：`started` 条目改为 `aborted_recovered`，若存在该次构建的孤儿计划则删除；报告只列持久化文件中的事实，来源标记为 `persisted_intent_files_only`。

## 运行演示

```bash
.venv/bin/python scripts/demo.py
.venv/bin/python scripts/crash_recovery_demo.py
```

Windows：

```powershell
.\.venv\Scripts\python.exe scripts\demo.py
.\.venv\Scripts\python.exe scripts\crash_recovery_demo.py
```

## 实际验证结果

`pytest -q` 结果：`23 passed, 1 warning`。唯一警告来自当前 Starlette 对 `httpx` 版 TestClient 的弃用提示，不影响测试结果。

本地 uvicorn 演示（快照 v2）：

| 查询 | 模式 | 选定顺序 | 总代价 | 搜索 |
|---|---|---|---|---|
| `q_customer_revenue` | fast | `orders -> customers -> order_items` | `6727.298926` | 4/4，剪枝 0 |
| `q_customer_revenue` | exact | `orders -> customers -> order_items` | `6727.298926` | 4/4，剪枝 0 |
| `q_product_priority` | fast | `customers -> orders -> order_items -> products` | `7149.915860` | 4/8，剪枝 4 |
| `q_product_priority` | exact | `orders -> customers -> order_items -> products` | `5940.915860` | 8/8，剪枝 0 |
| `q_employee_manager` | fast | `e2 -> e1` | `122.685951` | 2/2，剪枝 0 |
| `q_employee_manager` | exact | `e2 -> e1` | `122.685951` | 2/2，剪枝 0 |

加载 v2 后请求 v1 返回 HTTP `409`、业务码 `E_VERSION_RETIRED`。`q_product_priority` 的 orders 相关列修正只出现一次，系数为 `0.820000000000`。

真实强杀演示的恢复报告实际包含：

```json
{
  "counts": {
    "scanned": 1,
    "recovered_aborted": 1,
    "completed": 0,
    "already_aborted": 0,
    "corrupt": 0
  },
  "source": "persisted_intent_files_only"
}
```

对应意图从 `started` 变为 `aborted_recovered`；进程在计划完成前被强制终止，因此没有把未完成构建误报为成功。
