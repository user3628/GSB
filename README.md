# 本地延迟队列中间件与数据处理管线

这是一个不使用 Docker 的可中断延迟队列实现。它通过 FastAPI 暴露本地 HTTP API，用 SQLite（WAL）保存任务、状态迁移、轮次、投递、意图和恢复报告，以异步 HTTP 真实投递到固定随机种子生成的本地测试端点。

## 已实现能力

- 六态任务状态机：`PENDING -> READY -> DISPATCHED -> COMPLETED / RETRY_WAITING / DEAD_LETTERED`，恢复路径 `DISPATCHED -> READY`。
- 批量任务校验原子性、任务 ID 幂等、冲突内容拒绝。错误码使用 `E_TASK_INVALID` 与 `E_IDEMPOTENCY_CONFLICT`。
- 全局并发上限 + 每轮时间预算 + 单任务资源预算；按优先级、到期时刻、任务 ID 确定性排序。
- 超出本轮并发容量的任务进入有界等待队列；容量不足整个轮次失败并返回 `E_QUEUE_FULL`，不产生部分投递。
- httpx 异步请求 + `asyncio.wait_for` 独立超时 + `Connection: close` + `aclose()`；超时为 `E_DISPATCH_TIMEOUT`，不把等待伪装成超时，也不声称终止服务端失控线程。
- 固定基数指数退避：失败尝试号为 `n` 时等待 `retry_backoff_base_ms * 2^(n-1)`；退避未结束的任务不会被调度。
- 每队列单写者、全局并发信号量、SQLite WAL 多读者快照；同一轮次 ID 并发触发会等待并复用同一次结果，配置不一致返回 `E_CONFLICTING_ROUND`。
- 投递前在同一事务中写入 `OPEN` intent、`IN_FLIGHT` 投递记录、`DISPATCHED` 状态和状态迁移，因此查询不会看到“DISPATCHED 但无投递记录”的中间状态。
- 启动扫描未关闭 intent：把未落盘结果的投递标记为 `INTERRUPTED`，并将任务回滚到 `READY` 重排；恢复报告只陈述 SQLite 中已落盘的事实。
- 结构化结果验证器，根据投递记录、状态迁移、intent 和轮次配置核对并发、重试、退避和死信。

## 目录

```text
delay_queue/                 核心包（API、调度、SQLite、投递、验证器、测试端点）
scripts/                     mock endpoint、样本生成、手工加载、崩溃复现和演示脚本
samples/                     固定种子生成的确定性任务与场景
tests/                       pytest 测试（含真实 SIGKILL 恢复测试）
artifacts/verification_report.json  最近一次完整演示的落盘验收报告
```

## Windows 快速开始

要求 Python 3.10+。在 PowerShell 中执行：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

终端 A：启动确定性本地测试端点（成功、HTTP 失败、flaky、慢响应、挂起均由 task payload 或 task ID 决定）。

```powershell
python .\scripts\mock_endpoint.py --port 8765
```

终端 B：启动 FastAPI 服务。必须使用单进程/单 worker；队列锁、全局并发信号量和轮次等待复用均以单进程内的单写者模型为边界。

```powershell
$env:DQT_DEFAULT_ENDPOINT = "http://127.0.0.1:8765/dispatch"
$env:DQT_DB_PATH = ".\data\delay_queue.db"
$env:DQT_INTENT_DIR = ".\intent_logs"
python -m uvicorn delay_queue.api:app --host 127.0.0.1 --port 8000 --workers 1
```

终端 C：提交固定种子样本并触发一轮调度。

```powershell
$body = Get-Content .\samples\tasks_deterministic.json -Raw -Encoding UTF8
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/api/v1/queues/demo/tasks `
  -ContentType "application/json; charset=utf-8" -Body $body | ConvertTo-Json -Depth 12

$round = @{
  max_concurrency = 3
  time_budget_ms = 700
  max_task_resource = 6
  waiting_queue_capacity = 20
  dispatch_timeout_ms = 120
} | ConvertTo-Json
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/api/v1/queues/demo/rounds/demo-r1 `
  -ContentType "application/json" -Body $round | ConvertTo-Json -Depth 20
```

超时与重试样本：

```powershell
$hang = Get-Content .\samples\tasks_timeout.json -Raw -Encoding UTF8
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/queues/timeout-demo/tasks `
  -ContentType "application/json; charset=utf-8" -Body $hang

$timeoutRound = @{ max_concurrency = 1; time_budget_ms = 500; max_task_resource = 10; waiting_queue_capacity = 5; dispatch_timeout_ms = 60 } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/queues/timeout-demo/rounds/timeout-r1 `
  -ContentType "application/json" -Body $timeoutRound | ConvertTo-Json -Depth 20
```

查询与验证：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v1/queues/demo/status | ConvertTo-Json -Depth 20
Invoke-RestMethod http://127.0.0.1:8000/api/v1/queues/demo/tasks/det-01-success | ConvertTo-Json -Depth 20
Invoke-RestMethod http://127.0.0.1:8000/api/v1/recovery/report | ConvertTo-Json -Depth 20
python .\scripts\verify_round.py --db .\data\delay_queue.db --round-id demo-r1 --pretty
```

也可以直接运行完整演示。它会启动 mock endpoint、真实 uvicorn 服务、执行 HTTP 验收、调用验证器，并写入 `artifacts/verification_report.json`：

```powershell
python .\scripts\run_demo.py
```

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/api/v1/queues/{queue}/tasks` | 原子批量提交；body 可为 `{ "tasks": [...] }` 或任务数组 |
| `POST` | `/api/v1/queues/{queue}/rounds` | body 含 `round_id` 与轮次配置 |
| `POST` | `/api/v1/queues/{queue}/rounds/{round_id}` | 以路径轮次 ID 触发 |
| `GET` | `/api/v1/queues/{queue}/status` | 完整提交版本的队列快照与一致性标志 |
| `GET` | `/api/v1/queues/{queue}/tasks/{task_id}` | 任务、状态迁移、投递和 intent 轨迹 |
| `GET` | `/api/v1/queues/{queue}/rounds/{round_id}` | 轮次持久化记录与结果 |
| `POST` | `/api/v1/queues/{queue}/tasks/{task_id}/transitions` | 受状态机约束的手工状态迁移 |
| `GET` | `/api/v1/recovery/report` | 最近一次启动恢复报告，可加 `?queue=` |
| `GET` | `/api/v1/queues/{queue}/recovery-report` | 队列相关恢复报告 |
| `GET` | `/healthz` | 健康检查 |

兼容别名：`GET /api/v1/queues/{queue}`、`GET /api/v1/queues/{queue}/state`；`POST /api/v1/queues/{queue}/schedule`；`POST /api/v1/tasks`（body 内含 `queue`）；`POST /api/v1/schedule`；`GET /api/v1/recovery`；`GET /api/v1/queues/{queue}/tasks/{task_id}/history`。

业务失败统一为：

```json
{
  "ok": false,
  "code": "E_TASK_INVALID",
  "message": "one or more tasks failed validation; the entire batch was rejected",
  "context": { "atomic": true, "errors": [] }
}
```

主要错误码和 HTTP 状态：

- `E_TASK_INVALID`：422；整批不入库。
- `E_IDEMPOTENCY_CONFLICT`：409；同一 task ID 的不可变内容不同。
- `E_INVALID_TRANSITION`：409；非法状态迁移，原状态不变。
- `E_QUEUE_FULL`：429；本轮到期任务超出并发容量与有界等待容量。
- `E_DISPATCH_TIMEOUT`：出现在轮次结果的 `dispatched[].error_code`、`attempt_errors[].code` 和 `error_codes` 中；HTTP 轮次本身可仍成功并包含成功/失败混合结果。
- `E_CONFLICTING_ROUND`：409；同一 round ID 的队列或配置摘要不同。
- `E_PROCESS_INTERRUPTED`：恢复后重放一个被终止轮次时返回，并记录在恢复报告。

轮次成功响应中的每次投递同时含 `outcome`、`http_status`、`duration_ms`、`error_code`、`state_after`、`ok` 和 `code`，便于统一处理混合结果。

## 任务与状态语义

任务字段：

```json
{
  "task_id": "det-01-success",
  "due_at_ms": 0,
  "priority": 3,
  "remaining_retries": 2,
  "resource_requirement": 1,
  "payload": { "mode": "success" },
  "endpoint": "http://127.0.0.1:8765/dispatch"
}
```

- `due_at_ms >= 0`；`priority` 为 0-9 的整数，0 最高；`remaining_retries >= 0`；`resource_requirement >= 1`。
- `endpoint` 可省略，默认取 `DQT_DEFAULT_ENDPOINT`。默认只允许 loopback 地址；确实需要其它地址时显式设置 `DQT_ALLOW_NON_LOCAL_ENDPOINTS=1`。
- 初次提交保存为 `PENDING`。到期后调度器在同一写事务中转为 `READY`。
- 投递成功为任意 2xx。失败或超时先扣减剩余重试次数；仍大于 0 时进入 `RETRY_WAITING`，否则进入 `DEAD_LETTERED`。
- 所有迁移都写入 `task_transitions`，包含前后状态、原因、毫秒时间戳和上下文。

状态迁移表：

| From | To |
|---|---|
| `PENDING` | `READY` |
| `READY` | `DISPATCHED` |
| `DISPATCHED` | `COMPLETED`, `RETRY_WAITING`, `DEAD_LETTERED`, `READY`（仅恢复） |
| `RETRY_WAITING` | `READY` |
| `COMPLETED` / `DEAD_LETTERED` | 无 |

手工迁移接口不允许直接创建 `DISPATCHED`/`RETRY_WAITING`，也不允许从 `DISPATCHED` 手工跳出，以维护投递记录不变量。

## 调度与预算

轮次配置：

```json
{
  "max_concurrency": 3,
  "time_budget_ms": 700,
  "max_task_resource": 6,
  "waiting_queue_capacity": 20,
  "dispatch_timeout_ms": 120
}
```

- 实际轮次并发为 `min(max_concurrency, DQT_GLOBAL_CONCURRENCY)`。
- 到期候选先按 `(priority, due_at_ms, task_id)` 升序；资源超限者记 `RESOURCE_BUDGET_EXCEEDED`。
- 前 N 个进入投递，余下进入本轮有界等待清单；超过 `waiting_queue_capacity` 时整轮在修改任务前返回 `E_QUEUE_FULL`。
- 全局槽位采用短时准入等待，避免占住部分槽位后空耗整个轮次预算；没有拿到槽位的任务保持 `READY`，记 `GLOBAL_CONCURRENCY_LIMIT` 或 `TIME_BUDGET_EXHAUSTED`。
- 请求超时取 `min(dispatch_timeout_ms, 轮次剩余时间)`。若剩余可发送窗口小于 5 ms，则不发送并把任务记为本轮跳过，优先牺牲覆盖而不是突破并发/时间边界。
- `time_budget_ms = 0` 是确定性低预算用例：进入并发/等待候选的到期任务会转为 `READY`，资源超限任务保持 `PENDING`，本轮投递数为 0，并返回按排序生成的跳过清单。

## 持久化与强制终止恢复

SQLite 表：`tasks`、`task_transitions`、`rounds`、`dispatch_attempts`、`intents`、`recovery_reports`。

一次投递的写顺序：

1. 同一事务插入 `OPEN` intent。
2. 插入 `IN_FLIGHT` dispatch attempt。
3. 更新任务为 `DISPATCHED` 并写迁移。
4. 提交后，才发起 endpoint 请求。
5. endpoint 结果、attempt 终态、任务下一状态、迁移和 intent 关闭在同一次写事务中提交。

若进程在步骤 4 或步骤 5 中途被强制终止：

- 步骤 4 中断：启动扫描发现 `DISPATCHED + IN_FLIGHT + OPEN intent`，把 attempt 标为 `INTERRUPTED`，任务回滚为 `READY`，记录 `RECOVERY_ROLLBACK_TO_READY`，轮次标为 `INTERRUPTED`。
- 步骤 5 未提交：SQLite 原子回滚，随后的启动恢复执行同样的回滚。
- 若 endpoint 实际已收到请求但结果未落盘，恢复报告会明确写 `wire_outcome_unknown: true`、`wire_result_persisted: false`，不推断成功或失败。因此语义是至少一次投递，测试端点应允许重复请求。
- 强制终止的旧进程不可能生成完整退出报告。`graceful_exit_report_available_for_prior_process` 固定为 `false`；恢复报告来源固定为 `STARTUP_RECOVERY`，只引用重启时可见的 SQLite 行。

## 确定性样本与测试端点

`samples/tasks_deterministic.json` 由 `scripts/generate_samples.py` 使用 NumPy `default_rng(20261122)` 生成。场景文件是 `samples/scenario.json`；非法批次是 `samples/tasks_invalid.json`；挂起重试样本是 `samples/tasks_timeout.json`。

Mock endpoint 行为：

- `payload.mode = "success"`：HTTP 200。
- `payload.mode = "fail"`：默认 HTTP 503，可传 `status`。
- `payload.mode = "flaky"`：同一 task ID 第一次 500，之后 200。
- `payload.mode = "slow"`：等待 `sleep_seconds` 后 200。
- `payload.mode = "hang"`：等待 `hang_seconds` 后才尝试响应，用于验证客户端独立超时。

`hang` 测试中客户端会取消请求、关闭连接并记录 `E_DISPATCH_TIMEOUT`。服务端 Python 线程不会被声称已强制终止；mock 的线程只用于模拟挂起端点，客户端事实以实际连接关闭和持久化投递记录为准。

## 实际验证结果

本工作区执行结果：

```text
pytest -q --disable-warnings --maxfail=1
18 passed, 136 warnings in 10.12s
```

警告均来自 Python 3.14 下 FastAPI/Starlette 的依赖弃用提示，不是本项目测试失败。

```text
python scripts/run_demo.py
all_checks_passed: true  (21/21)
validator demo-r1: true
validator demo-r2-budget-zero: true
validator timeout-r1: true
validator timeout-r2: true
validator crash-demo-round: true
```

演示观察到的关键事实：

- 12 条确定性任务一次性入库；原样重复提交 `inserted=0`、`duplicates=12`。
- 第一轮 11 个到期候选，并发 3，实际投递 3，8 个进入有界等待队列，未突破并发。
- `time_budget_ms=0` 轮次投递 0，跳过 8 个到期候选，原因均为预算耗尽或等待队列。
- 挂起任务第一轮为 `TIMEOUT -> RETRY_WAITING`，退避后第二轮为 `TIMEOUT -> DEAD_LETTERED`，错误码均为 `E_DISPATCH_TIMEOUT`。
- 队列容量不足返回 `E_QUEUE_FULL`；重复轮次不同配置返回 `E_CONFLICTING_ROUND`；非法迁移返回 `E_INVALID_TRANSITION`。
- 查询快照 `dispatching_without_attempt=0`、`consistency.ok=true`、`open_intents=0`。
- 完整 SIGKILL 恢复场景会启动子进程、等待 `DISPATCHED + IN_FLIGHT + OPEN intent` 落盘、强制终止进程，再由新服务启动恢复为 `READY/INTERRUPTED`，并把同在 `pytest` 与 `run_demo.py` 中核对的恢复事实写入报告。

详细落盘结果见 `artifacts/verification_report.json`。

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DQT_DB_PATH` | `data/delay_queue.db` | SQLite 文件 |
| `DQT_INTENT_DIR` | `intent_logs` | 意图日志目录；启动时创建，SQLite `intents` 是权威日志 |
| `DQT_DEFAULT_ENDPOINT` | `http://127.0.0.1:8765/dispatch` | 默认投递地址 |
| `DQT_GLOBAL_CONCURRENCY` | `64` | 进程内跨队列总并发 |
| `DQT_MAX_CONCURRENCY` | `4` | 默认轮次并发 |
| `DQT_TIME_BUDGET_MS` | `2000` | 默认轮次时间预算 |
| `DQT_MAX_TASK_RESOURCE` | `100` | 默认单任务资源预算 |
| `DQT_WAITING_QUEUE_CAPACITY` | `100` | 默认有界等待容量 |
| `DQT_DISPATCH_TIMEOUT_MS` | `1000` | 默认单请求超时 |
| `DQT_RETRY_BASE_MS` | `200` | 指数退避基数 |
| `DQT_ALLOW_NON_LOCAL_ENDPOINTS` | `0` | 是否允许非 loopback endpoint |
| `DQT_ROUND_WAIT_TIMEOUT_MS` | `10000` | 重复触发仍在运行轮次时的最长复用等待 |

## 边界说明

- 设计目标是本地单进程测试中间件。不要使用多个 uvicorn worker 共享同一 SQLite 文件；否则进程内锁和全局信号量无法提供所声明的单写者/全局并发保证。
- 外部 endpoint 调用与数据库事务不可能组成分布式原子事务，因此崩溃恢复采用持久化事实驱动的回滚和至少一次投递语义。
- 恢复不会声称知道进程被终止瞬间 endpoint 端发生了什么；“未知”会被显式记录，而不是补造成功/失败结果。
