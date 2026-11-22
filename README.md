# Loopback Proxy Diagnostics Workbench

面向本地回环环境的正向/反向代理转发诊断工作台。项目不依赖 Docker，不访问外部站点；后端、出口代理、配置样本与端口清单全部位于本仓库。

## 1. 能力概览

- 反向路由：按 HTTP 方法与路径前缀匹配，转发到本地后端。
- 正向路由：按目标主机/端口匹配，经本地 HTTP 出口代理转发。
- 配置快照：单调版本号、规范化 SHA-256 摘要、整份替换。
- 增量提交：只提交受影响路由，携带 `base_version`；同一基础版本并发提交仅一个成功。
- 定点预算：Q6（6 位小数）整数契约，逐步记录理想值、舍入值、舍入方式与累计误差。
- 数值稳定性：上游预算按排序后的精确 Q6 整数累计，并提供 Kahan/朴素 float64 对比基准。
- 原子级联：匹配表项、连接池分组、健康探测计划、转发规则缓存、匹配索引、上游预算聚合在同一 SQLite 事务内写入。
- 不可变读路径：每个在途请求绑定提交时快照；配置更新不改变旧请求的路由版本。
- 请求幂等：同一 `X-Request-Id`/`Idempotency-Key` 并发或重复请求只真实转发一次并复用结果。
- 真实超时：连接/读取超时由 `httpx` 套接字超时控制，外层总等待预算取消并关闭连接。
- 崩溃恢复：提交前持久化意图；强杀后重启扫描 `PREPARED` 意图，恢复到最后一个完整快照版本。
- 可视化诊断页：版本、路由预算、上游聚合、级联轨迹、意图与恢复报告。

## 2. Windows 环境准备

要求 Python 3.11 或更高版本、Windows 本地回环端口可用。

```powershell
py -3.11 --version
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python scripts\check_env.py
```

如 PowerShell 禁止激活虚拟环境，可先执行：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

本项目代码仅使用 Python 标准库、FastAPI、Uvicorn、NumPy、HTTPX 与 pytest，兼容 Windows 的 asyncio/SQLite 行为；未使用 Unix 专用命令或容器。

## 3. 启动与访问

### 3.1 检查回环端口

固定端口清单见 `samples/loopback_ports.json`：

| 服务 | 默认地址 |
|---|---|
| 诊断工作台 | `127.0.0.1:18080` |
| 主测试后端 | `127.0.0.1:18081` |
| 本地出口代理 | `127.0.0.1:18082` |
| 备用测试后端 | `127.0.0.1:18083` |

```powershell
python scripts\check_ports.py
```

### 3.2 单独拉起本地后端与出口服务

```powershell
python -m tools.local_upstreams --mode all
# 等价入口：
python scripts\run_upstreams.py
```

也可以按需启动：

```powershell
python -m tools.local_upstreams --mode backends
python -m tools.local_upstreams --mode egress
```

测试后端提供：

- `/health`：确定性健康响应。
- `/svc/echo`、`/echo`：回显方法、路径、查询串、请求头、请求体哈希。
- `/slow`、`/slow/{delay_ms}`：固定延迟。
- `/hang`：故意挂起，用于真实读取超时验证。
- `/status/{code}`：固定状态码，包括 5xx。
- `/__stats`：已接收转发请求计数，用于幂等仅转发一次验证。

出口代理只接受 absolute-form HTTP 请求且只允许回环目标，并在上游请求中写入 `X-Local-Egress` 标记。

### 3.3 启动工作台

PowerShell：

```powershell
$env:WORKBENCH_DB="runtime\workbench.sqlite3"
$env:WORKBENCH_PORT="18080"
$env:WORKBENCH_ENABLE_FAILPOINTS="0"
python -m uvicorn app.main:app --host 127.0.0.1 --port 18080
```

CMD：

```bat
set WORKBENCH_DB=runtime\workbench.sqlite3
set WORKBENCH_PORT=18080
python -m uvicorn app.main:app --host 127.0.0.1 --port 18080
```

访问：

- 诊断页面：<http://127.0.0.1:18080/>
- OpenAPI：<http://127.0.0.1:18080/docs>
- 健康检查：<http://127.0.0.1:18080/workbench/api/health>

### 3.4 一键端到端演示

```powershell
python scripts\run_demo.py
```

演示脚本会自行启动测试后端、出口代理和工作台，完成全量提交、转发、超时、幂等、增量提交、级联轨迹、强制终止与重启恢复，并在 `runtime/demo_report.json` 写出持久化结果摘要。

## 4. 配置接口

### 4.1 全量快照

`POST /workbench/api/config/snapshot`

```json
{
  "version": 1,
  "routes": [
    {
      "route_id": "reverse-main",
      "kind": "reverse",
      "methods": ["GET", "POST"],
      "path_prefix": "/svc",
      "upstream": "http://127.0.0.1:18081",
      "timeout_ms": 600,
      "retries": 2,
      "headers": {"X-Workbench-Sample": "v1"},
      "health_status": "healthy",
      "priority": 300
    }
  ]
}
```

- 服务端计算 `SHA-256(canonical JSON)`，摘要覆盖 `version` 与按 `route_id` 排序后的路由。
- 可传可选 `digest`；若提供且不匹配则返回 `E_SNAPSHOT_INVALID`。
- 新版本必须严格大于当前版本。
- 提交使用整份路由集合，不会与旧快照混合。

完整样本：`samples/routes_snapshot_v1.json`。

### 4.2 增量片段

`POST /workbench/api/config/incremental`

```json
{
  "base_version": 1,
  "upserts": [
    {
      "route_id": "reverse-main",
      "kind": "reverse",
      "methods": ["GET", "POST"],
      "path_prefix": "/svc",
      "upstream": "http://127.0.0.1:18083",
      "timeout_ms": 600,
      "retries": 2,
      "headers": {"X-Workbench-Sample": "v2-secondary"},
      "health_status": "healthy",
      "priority": 300
    }
  ],
  "delete_route_ids": []
}
```

- `upserts` 只包含受影响路由；`delete_route_ids` 可选。
- 服务端合并到当前完整快照，生成 `base_version + 1` 版本。
- `base_version` 不是当前版本时返回 `E_STALE_BASE`，已提交版本、摘要与产物均不改变。
- 两个并发增量携带同一基础版本时，SQLite `BEGIN IMMEDIATE` 与进程内版本锁共同保证只有一个成功。

完整样本：`samples/routes_incremental_v2.json`。

### 4.3 诊断 API

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/workbench/api/state` | 当前完整版本、路由、预算、产物摘要、聚合与事件 |
| GET | `/workbench/api/state?version=N` | 指定历史版本完整状态 |
| GET | `/workbench/api/versions` | 版本轨迹与最近提交 |
| GET | `/workbench/api/budgets/explain?route_id=...` | 单路由逐步预算与误差 |
| GET | `/workbench/api/budgets/benchmark` | 固定混合量级朴素/Kahan 对比 |
| GET | `/workbench/api/intents` | 意图日志与恢复报告 |
| GET | `/workbench/api/recovery` | 启动恢复事实报告 |
| POST | `/workbench/api/config/snapshot` | 全量快照提交 |
| POST | `/workbench/api/config/incremental` | 增量提交 |

## 5. 转发用法

### 5.1 反向转发

匹配方法 + 路径前缀后，原样保留路径、查询串与请求体：

```powershell
curl.exe -X POST "http://127.0.0.1:18080/svc/echo?x=1" -H "X-Test: demo" -d "hello"
```

上游状态码与响应体直接回传。响应头附加 `X-Workbench-Version`、`X-Workbench-Route`、`X-Workbench-Attempts`。

### 5.2 正向转发

`/forward/{path}` 后按 `X-Forward-Host` 与 `X-Forward-Port` 匹配正向路由，再经本地出口代理转发：

```powershell
curl.exe "http://127.0.0.1:18080/forward/echo?q=1" `
  -H "X-Forward-Host: 127.0.0.1" `
  -H "X-Forward-Port: 18081"
```

仅允许回环目标与 `http` 出口。测试后端回显中的 `X-Local-Egress` 可证明请求经过本地出口。

### 5.3 请求幂等

```powershell
curl.exe "http://127.0.0.1:18080/slow" -H "X-Request-Id: demo-1"
```

- 同一 ID 与同一请求指纹：复用第一次状态码、响应头和响应体，不再次转发。
- 同一 ID 用于不同请求：`409 E_CONFLICTING_REQUEST`。
- 响应返回 `X-Workbench-Request-Id`。

## 6. Q6 定点数值契约

### 6.1 表示

- 内部格式：有符号 Q6 整数，`1 ms = 1_000_000`，所有预算/权重在比较与聚合前统一归一化。
- 舍入：非负有理数 `round_half_up_to_q6`。
- 每条路由：

```text
attempts        = retries + 1
timeout_q6      = timeout_ms * 1_000_000
connect_q6      = round_half_up(timeout_q6 * 853 / 997)
total_wait_q6   = round_half_up(connect_q6 * attempts * 1099 / 1000)
health_factor   = 1 / 5/4 / 7/4  (healthy / degraded / unhealthy)
penalty_q6      = round_half_up(total_wait_q6 * health_factor)
```

实际每步使用上一步的 Q6 舍入值；同时保留一条不中途舍入的理想 `Fraction` 链，用于输出：

- 每步 `ideal_q6`、`rounded_q6`、`rounding_mode`、`rounding_error_q6`。
- 最终 `cumulative_rounding_error_q6` 与逐步绝对舍入误差和。

实现：`app/fixedpoint.py`。

### 6.2 跨路由聚合

- 按已归一化上游 URL 分组。
- 组内路由 ID 按字典序排序，总等待预算与健康惩罚使用精确 Q6 整数求和作为规范结果。
- 该规范结果天然满足结合律与交换律，不受提交顺序、批次拆分或路由数量排列影响。
- 同时用 NumPy float64 做朴素累计与 Kahan 补偿累计，报告两者相对精确整数的误差及保守误差上界。
- 固定混合量级样本：`10^16` 加 20 个 `1`、10 个 `3`；实际朴素误差为 `-10 Q6`，Kahan 误差为 `0 Q6`。

基准实现：`app/benchmark.py`。

## 7. 级联产物与原子事务

每条路由产生四类编译产物：

| 产物 | 内容 |
|---|---|
| `match_entry` | 方法、路径前缀/目标主机端口、优先级 |
| `pool_group` | 上游 origin、连接预算、池分组键 |
| `health_plan` | 健康状态、探测间隔/超时、失败阈值、惩罚权重 |
| `forward_cache` | 上游、方法、转发头、超时与重试缓存键 |

聚合产物：

- `match_index`：完整反向/正向匹配聚合索引。
- `upstream_budget`：共享上游的总等待预算与健康惩罚聚合。

关键字段变化时：

1. 记录意图 `PREPARED` 并提交该意图记录。
2. 编译新完整版本。
3. 在一个 `BEGIN IMMEDIATE` 事务中写入新快照、路由预算、全部产物、级联事件并更新意图为 `COMMITTED`。
4. 事务成功后原子替换进程内不可变运行时指针。

事件先后记录 `INVALIDATED` 与 `REBUILT`。未变化路由的四类产物不产生失效事件；匹配索引及受影响上游聚合会级联重建。查询只按完整 `version` 读取，不会混合两个版本。

数据库表：`intents`、`snapshots`、`route_budgets`、`artifacts`、`artifact_events`、`idempotency`、`recovery_runs`。

## 8. 崩溃恢复

- 提交意图在配置事务之前单独持久化。
- 演示/测试环境可设置 `WORKBENCH_ENABLE_FAILPOINTS=1`，并通过请求头 `X-Workbench-Failpoint: crash-after-intent` 在意图写入后暂停，便于外部强杀进程。
- 强杀时若配置事务未提交，SQLite 自动回滚该事务；意图保持 `PREPARED`。
- 重启时 `SQLiteStore.recover()` 在事务内把未完成意图标记为 `ROLLED_BACK`，加载最后完整快照，并写入 `recovery_runs`。
- 恢复报告只列出数据库中实际存在的未完成意图、最后完整版本/摘要和已放弃的幂等记录，不根据进程日志猜测。

## 9. 业务码

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_STALE_BASE` | 409 | 增量基础版本不是当前版本 |
| `E_SNAPSHOT_INVALID` | 422 | 快照/增量字段、摘要或版本非法 |
| `E_CONFLICTING_REQUEST` | 409 | 请求 ID 已用于不同请求或无可用完成结果 |
| `E_UPSTREAM_UNAVAILABLE` | 502 | 连接拒绝或上游传输失败 |
| `E_UPSTREAM_TIMEOUT` | 504 | 连接、读取或总等待预算真实超时 |
| `E_ROUTE_NOT_FOUND` | 404 | 无匹配路由或版本不存在 |
| `E_BAD_REQUEST` | 400 | 正向目标头等请求参数非法 |

代理自身失败统一返回含 `business_code` 的 JSON。上游 5xx 遵循代理契约直接回传状态码与响应体，并附加 `X-Workbench-Upstream-Failure: upstream_5xx` 以区别于代理异常。

## 10. 测试与演示

```powershell
python -m pytest -q
python scripts\run_demo.py
```

测试覆盖：

- Q6 推导逐位确定性、半入舍入、非零误差传播。
- 路由输入顺序无关、提交顺序无关、批量/拆分聚合一致。
- 全量替换、增量 stale、同基础版本并发、级联事件范围。
- 同请求 ID 并发仅转发一次、冲突请求 409。
- 反向/正向转发、逐跳头过滤、5xx 透传、读取超时、连接拒绝。
- 在途请求绑定旧版本，新请求使用新版本。
- 真实进程强杀后的意图回滚与恢复报告。

实际执行结果见 `VERIFICATION.md`。

## 11. 目录

```text
app/
  benchmark.py      固定 float64/Kahan 基准
  compiler.py       路由预算与产物编译、匹配索引、聚合
  db.py             SQLite schema、原子提交、意图/恢复/幂等
  errors.py         业务码与 JSON 错误
  fixedpoint.py     Q6 契约与误差记录
  main.py           FastAPI API、诊断页、入口
  models.py         路由/快照/增量模型与回环校验
  proxy.py          转发引擎与幂等协调
  service.py        版本状态机与级联提交
  static/           可视化诊断页
samples/            固定路由、服务与端口样本
scripts/            端口检查、服务入口、完整演示
tests/              20 项自动化测试
tools/              本地后端、延迟/挂起服务、出口代理
```

## 12. 边界说明

- 工作台目标是一个本机诊断进程：并发配置串行化使用进程内锁 + SQLite `BEGIN IMMEDIATE`。若同一数据库被多个工作台进程同时写入，SQLite 仍保证事务完整，但请求幂等协调会以 `E_CONFLICTING_REQUEST` 拒绝其他进程中的同 ID 在途请求，而不是跨进程等待。
- HTTP 出口代理只实现诊断所需的 absolute-form HTTP 转发，不实现 `CONNECT`/HTTPS 隧道。
- 为满足本地授权约束，路由上游、正向目标与出口目标均强制回环地址，不访问外部站点。
