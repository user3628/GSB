# Serverless 冷启动与并发控制验证器

这是一个完全本地、可离线复现的 Serverless 运行时实验应用，面向函数平台工程师验证冷启动、实例复用、并发额度、真实超时和强制终止恢复。运行时不使用 Docker，不访问外部目标；所有状态、日志和审计结果均写入工作目录内的 SQLite 与 JSON 文件。

当前实现已通过 34 项 pytest 测试和基于 `samples/` 的一键样本验证。实际结果见 `VERIFICATION.md` 和 `artifacts/verification.json`。

## 功能范围

- 严格解析固定种子生成的函数规格和调用轨迹；不合格批次整批回滚并返回 `E_SPEC_INVALID`。
- 模拟 `CREATING -> READY/BUSY -> DRAINING -> TERMINATED` 实例生命周期，区分冷启动和热启动。
- 按函数并发上限准入；按需额度与预留额度分离；超限调用进入有界优先级队列。
- 通过本地 HTTP 连接真实执行启动与调用，独立连接超时后立即关闭 socket 并原子释放槽位和额度。
- 五类异常注入：创建超时、执行中断、重复调用、预留额度耗尽、轨迹乱序。
- SQLite WAL 持久化规格版本、实例、调用、额度、异常注入和意图日志；重启扫描未完成意图。
- 每函数单写者串行准入；只读查询使用单个 SQLite 快照，不观察半提交状态。
- 同一 `call_id` 去重执行；内容不一致返回 `E_CONFLICTING_CALL`。

## Windows 环境准备

要求 Python 3.11 或更高版本。以下 PowerShell 命令在项目根目录执行：

```powershell
python --version
python -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

本工作区实际解析到的版本记录在 `requirements-lock.txt`。运行示例：

```powershell
# 终端 A：本地异常测试端点，成功端口 8765，主动拒绝端口 8766
python -m tools.test_endpoint --port 8765 --refuse-port 8766

# 终端 B：运行时 API
$env:SIM_ENDPOINT_URL = "http://127.0.0.1:8765"
$env:SIM_START_TIMEOUT_MS = "1000"
$env:SIM_EXEC_TIMEOUT_MS = "2000"
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

加载固定样本并调用：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/v1/samples/load

$body = @{
  call_id = "demo-001"
  function_id = "fn-audit"
  arrival_ms = 100
  execution_us = 20000
  priority = 2
  retry = $false
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/v1/invocations `
  -ContentType "application/json" -Body $body

Invoke-RestMethod http://127.0.0.1:8000/v1/instances?function_id=fn-audit
Invoke-RestMethod http://127.0.0.1:8000/v1/quota
Invoke-RestMethod http://127.0.0.1:8000/v1/audit/invariants
```

默认数据库为 `data/runtime.sqlite3`。可通过 `SIM_DB_PATH` 指向其他本地路径。

## 测试与一键验证

```powershell
python -m pytest
python -m tools.verify_samples
```

也可运行 `.\scripts\verify.ps1`。样本验证会启动自己的 loopback 测试端点、使用临时数据库，执行两次确定性轨迹回放、五类异常注入和两种强制终止恢复，最后写入 `artifacts/verification.json`。

## 本地测试端点

`tools/test_endpoint.py` 提供：

- `GET /start?duration_us=N`：真实等待后成功返回，用于实例初始化。
- `GET /execute?duration_us=N`：真实等待后成功返回，用于调用执行。
- `GET /hang?duration_us=N`：保持连接不返回；检测到客户端断开后记录 `hang_disconnects`。
- `GET /stats`：本地连接和挂起统计。
- `--refuse-port`：独立 TCP 端口，接受连接后立即关闭，用于主动拒绝连接验证。

客户端超时的含义是“本端关闭了该 HTTP 连接”，不是声称远端已经开始执行的代码被线程或进程终止。远端测试端可能继续运行到自身逻辑结束。

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/functions/load` | 原子加载函数规格批次 |
| GET | `/v1/functions` | 函数版本、实例计数和累计指标 |
| POST | `/v1/traces/load` | 原子加载轨迹，默认校验到达时间单调性 |
| GET | `/v1/traces/{batch_id}` | 查询规范化轨迹记录 |
| POST | `/v1/traces/{batch_id}/replay` | 按 `arrival_ms` 调度并发回放；`time_scale=0` 立即并发 |
| POST | `/v1/invocations` | 准入并真实执行一次调用；同一 `call_id` 去重 |
| GET | `/v1/calls/{call_id}` | 查询已提交调用结果 |
| GET | `/v1/snapshot?function_id=...` | 单事务完整快照，含 `commit_seq` |
| GET | `/v1/instances?function_id=...` | 实例池状态与占用槽位 |
| GET | `/v1/quota?function_id=...` | 按需额度与预留额度占用 |
| POST | `/v1/functions/{id}/prewarm?count=N` | 通过真实 HTTP 初始化预热实例 |
| POST | `/v1/instances/{id}/drain` | 进入 DRAINING；空闲时同事务终止 |
| POST | `/v1/instances/{id}/transition` | 显式状态迁移及非法迁移验证 |
| POST | `/v1/injections` | 执行五类异常注入之一 |
| GET | `/v1/injections/{id}/recheck` | 依据持久化前后事实逐项复查 |
| GET | `/v1/audit/invariants` | 审计槽位、额度和终态不变量 |
| GET | `/v1/recovery/latest` | 最新启动恢复报告 |
| POST | `/v1/samples/load` | 加载 `samples/` 下的规格和轨迹 |

所有业务错误均使用 `{"ok": false, "business_code": "...", "message": "...", "detail": {...}}`。超时等调用级终态也会作为调用结果中的 `business_code` 返回，并可通过 `/v1/calls/{call_id}` 重查。

## 业务码

| 业务码 | 含义 |
|---|---|
| `E_SPEC_INVALID` | 规格、轨迹或请求结构不合格；整批未入库 |
| `E_INVALID_TRANSITION` | 非法实例状态迁移；原持久化状态不变 |
| `E_CONCURRENCY_LIMIT` | 函数并发和等待队列均满 |
| `E_QUOTA_DENIED` | 未声明预留额度或预留额度已耗尽 |
| `E_START_TIMEOUT` | 实例启动连接真实超时并已释放资源 |
| `E_EXEC_TIMEOUT` | 调用执行连接真实超时并已释放资源 |
| `E_START_FAILED` | 启动连接被拒绝或发生非超时传输错误 |
| `E_EXEC_FAILED` | 执行连接被拒绝或发生非超时传输错误 |
| `E_CONFLICTING_CALL` | 相同 `call_id` 携带不同调用内容 |
| `E_TRACE_OUT_OF_ORDER` | 轨迹到达时间乱序；整批拒绝 |
| `E_QUEUE_TIMEOUT` | 有界等待队列超时 |
| `E_RECOVERED_INTERRUPTED` | 重启时依据已提交事实终结的中断调用 |

## 五类异常注入

`POST /v1/injections` 请求示例：

```json
{
  "kind": "EXECUTION_INTERRUPT",
  "function_id": "fn-audit",
  "call_id": "inject-exec-001",
  "params": {"execution_us": 500000}
}
```

| `kind` | 注入方式 | 预期业务码 |
|---|---|---|
| `INSTANCE_CREATE_TIMEOUT` | 启动请求指向真实不响应端点 | `E_START_TIMEOUT` |
| `EXECUTION_INTERRUPT` | 执行请求指向真实不响应端点 | `E_EXEC_TIMEOUT` |
| `DUPLICATE_CALL_ID` | 同 ID 第一次成功，第二次改内容 | `E_CONFLICTING_CALL` |
| `QUOTA_BURST` | 并发占用全部预留额度并多发一个 | `E_QUOTA_DENIED` |
| `TRACE_OUT_OF_ORDER` | 提交到达时间倒序的轨迹批次 | `E_TRACE_OUT_OF_ORDER` |

每类注入均先写入 `injection_events`，完成后持久化注入参数、前后指标和结果。`recheck` 会逐项比较业务码、冷启动增量、超时增量、额度归还、实例终态和轨迹批次数量。

轨迹回放使用固定 `call_id`。对同一批次重复回放会复用已有调用结果；需要重新执行应使用新数据库或新批次标识。

## 确定性与恢复边界

- 函数与轨迹样本由 NumPy 固定种子 `20261123` 生成，脚本为 `tools/generate_samples.py`。
- 正常回放采用稳定裁决顺序；耗时数值不参与一致性比较。`tools/verify_samples.py` 在两个全新数据库中比较完整业务判定。
- 进程被强制终止后不存在完整退出报告。重启报告明确声明只依据 SQLite 中已提交事实，将未完成调用置为 `E_RECOVERED_INTERRUPTED`、未完成实例置为 `TERMINATED`，并重新以未终态调用为额度真值。
- 本应用假定一个运行时进程独占数据库；不提供多进程抢占式协调。只读查询可以并发。

更详细的调度、事务和状态机说明见 `docs/DESIGN.md`。

