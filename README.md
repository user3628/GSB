# 本地消息投递语义中间件（SQLite / 本地 HTTP）

这是一个面向本地微服务测试环境的单机消息投递中间件。组件以 SQLite 持久化队列、消息、投递尝试、确认、消费组位点、请求幂等结果与意图日志；消费者通过本地 HTTP 端点接收消息，并通过 `/v1/acks` 确认。

实现覆盖：

- `at_least_once` 与 `exactly_once` 两种确认语义。
- 队列内无缺口生产序号、消息标识入队幂等。
- `PENDING / DISPATCHED / ACKED / REDELIVERED / DEAD_LETTERED` 消息状态机。
- 全局并发、每队列在途、每轮时间预算三重约束并行调度。
- 有界准入窗口、队列优先级、同优先级队列轮转和确定性降级依据。
- 真实 HTTP 读超时；超时后关闭 `requests.Session` 并立即释放并发槽。
- 队列级单写者、WAL 一致快照、独占租约、请求标识等待并复用结果。
- 两阶段意图日志和进程被强制终止后的恢复报告。

所有运行路径均限制在回环地址与本地 SQLite，不使用 Docker，不访问外部网络。

## 已验证结果

实际执行环境为 WSL2 Ubuntu / Python 3.14.4 / SQLite 3.46.1（非容器）；实现本身只使用 Python 3.10+ 标准库、Flask、requests 与 SQLite，可直接在 Windows 10+ 运行。

- 自动化测试：`21 passed`。
- 复现实验：6/6 场景通过。
- 强杀投递：恢复报告 `dispatch_rolled_back_to_pending`，消息 `DISPATCHED -> PENDING`。
- 强杀确认：恢复报告 `ack_rolled_back_to_unconfirmed`，消息 `ACKED -> DISPATCHED`，确认记录标记 `ROLLED_BACK`。
- 完整结果：`artifacts/verification_report.json`。
- 阅读版记录：`docs/VERIFICATION.md`。

## 快速开始（Windows 10+）

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

初始化和加载固定样本：

```powershell
python -m mqmw.cli init --db artifacts\mw.db --recover
python -m mqmw.cli load-samples --db artifacts\mw.db
```

启动本地消费者测试端点和中间件 API（两个终端）：

```powershell
python tools\test_consumer.py --port 8091
python -m mqmw.cli serve --db artifacts\mw.db --port 8080 --ack-base-url http://127.0.0.1:8080 --reaper-interval-ms 200
```

执行一轮恰好一次投递（样本消费者会自动确认）：

```powershell
$body = @{
  request_id = "round-exact-1"
  mode = "exactly_once"
  consumer_id = "payments-worker"
  config = @{
    global_concurrency = 2
    per_queue_inflight = 2
    time_budget_ms = 700
    dispatch_timeout_ms = 350
    wait_queue_capacity = 8
  }
} | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/v1/rounds `
  -ContentType "application/json" -Body $body
```

查看恢复报告、快照和某个消息的完整轨迹：

```powershell
Invoke-RestMethod http://127.0.0.1:8080/v1/recovery
Invoke-RestMethod "http://127.0.0.1:8080/v1/snapshot?queue=payments-exact"
Invoke-RestMethod http://127.0.0.1:8080/v1/messages/orders-hot/order-001
```

Linux/macOS 等价命令使用 `.venv/bin/python`。如果 `8080` 被占用，将中间件换到如 `18080`，并同步修改 `--ack-base-url`。

## 一键验证

```powershell
.\.venv\Scripts\python.exe scripts\run_experiment.py --output artifacts\verification_report.json
.\.venv\Scripts\python.exe -m pytest -q
```

实验脚本会动态选择回环端口、生成临时 SQLite 数据库、替换样本消费者端点，并运行双模式、重投、超时、预算降级、并发租约、请求幂等与两次强制终止恢复。不会访问外部网络。

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/v1/health` | 健康状态与当前提交版本 |
| POST | `/v1/queues` | 注册/更新队列及优先级 |
| POST | `/v1/consumers` | 注册消费者、消费组、订阅队列和本地端点 |
| POST | `/v1/enqueue` | 批量入队，消息标识幂等 |
| POST | `/v1/rounds` | 执行一轮预算化并行投递 |
| POST | `/v1/claims` / `/v1/subscribe` | 单条消费者租约领取/拉取订阅 |
| POST | `/v1/acks` | 消费者确认 |
| POST | `/v1/reap` | 扫描可见性超时并重投/死信 |
| POST | `/v1/messages/<queue>/<id>/transitions` | 状态机管理与非法迁移验证 |
| GET | `/v1/queues/<queue>` | 队列状态、在途数与消费组位点 |
| GET | `/v1/messages/<queue>/<id>` | 消息、投递、确认与意图历史 |
| GET | `/v1/snapshot` | 单一读事务中的完整提交版本 |
| GET | `/v1/rounds/<round_id>` | 轮次结果与可追溯轨迹 |
| GET | `/v1/intents` | 意图日志查询 |
| GET | `/v1/recovery` | 最近一次恢复报告 |

所有写接口都支持 `request_id`。相同 `request_id` 的并发请求只有一个成为执行者，其余等待并复用完全相同的成功或错误结果。

## 投递轮次参数

```json
{
  "global_concurrency": 4,
  "per_queue_inflight": 1,
  "time_budget_ms": 1200,
  "dispatch_timeout_ms": 250,
  "wait_queue_capacity": 32,
  "strict_wait_queue": false,
  "max_scan_messages": 5000
}
```

- `global_concurrency`：同时进行 HTTP 派发的全局上限。
- `per_queue_inflight`：每队列已领取且未确认的租约上限。
- `time_budget_ms`：轮次准入截止时间。每个 HTTP 调用的实际超时为 `min(dispatch_timeout_ms, 当前剩余预算)`，因此挂起请求不会把整轮无限拉长。
- `wait_queue_capacity`：有界等待窗口。准入窗口为 `global_concurrency + wait_queue_capacity`。
- `strict_wait_queue=true`：准入窗口不足时整轮返回 HTTP 429 `E_QUEUE_FULL` 且不领取任何消息；默认部分执行并在 `error_codes` 中返回 `E_QUEUE_FULL`。

轮次结果包含 `deliveries`、`redeliveries`、`redelivery_transitions`、`http_accepted`、`dispatch_timeouts`、`dispatch_refusals`、`dead_lettered`、`acks_observed`、逐队列明细、逐消息降级顺序与 `outcomes`。轨迹事件包括 `ROUND_START`、`CLAIMED`、`HTTP_ACCEPTED`、`ACKED`、`DISPATCH_TIMEOUT`、`CANDIDATE_DEGRADED` 和 `ROUND_END`。

## 核心语义

### 状态机

```text
PENDING ──领取──> DISPATCHED ──确认──> ACKED
                     │  │
                     │  └──可见性超时/派发失败──> REDELIVERED ──领取──> DISPATCHED
                     │                                  │
                     └────────重试耗尽──────────────────┴──> DEAD_LETTERED
```

非法迁移返回 `E_INVALID_TRANSITION`，在写任何业务状态前失败。`ACKED` 与 `DEAD_LETTERED` 为终态。

### 两种模式

- 至少一次：派发成功后若确认丢失，可见性超时把消息转为 `REDELIVERED`；下一轮仍按最小生产序号领取，不会跳过后续消息。消费组位点只跨过连续终态消息。
- 恰好一次：确认幂等键为队列作用域内的 `(queue_name, message_id, consumer_id)`。重复确认返回 `E_DUPLICATE_ACK`，已提交确认与待确认计数不会重复扣减。中间件只能保证自身确认记录一次性；消费者外部副作用的幂等需由业务侧实现。

### 持久化与恢复

每个业务写先提交一条 `STARTED` 意图，再把实际状态变更和 `APPLIED` 意图放入同一 SQLite 事务，最后推进为 `COMMITTED`。因此强制终止可能留下：

- 未完成的 `DISPATCH`：恢复时把 `DISPATCHED` 回滚为 `PENDING`，投递尝试标记 `ROLLED_BACK`。
- 未完成的 `ACK`：恢复时把 `ACKED` 回滚为未确认，确认记录标记 `ROLLED_BACK`，并重算消费组位点。

恢复报告只查询持久化表，包含动作、前后状态、回滚项、被中止的请求、恢复后消息/意图计数与恢复后超时扫描结果。启动 `serve` 时默认自动恢复；也可执行：

```powershell
python -m mqmw.cli recover --db artifacts\mw.db --output artifacts\recovery.json
```

## 目录

```text
mqmw/                  中间件、SQLite 存储、调度器、HTTP API、CLI
tools/test_consumer.py 成功/拒绝/挂起/延迟本地消费者端点
samples/               固定队列、消费者、消息批次和轮次样本
tests/                 21 项自动化测试与强制终止子进程脚本
scripts/run_experiment.py 固定样本验收实验
artifacts/verification_report.json 实测报告
docs/ARCHITECTURE.md   设计细节
docs/VERIFICATION.md   实测结果与复现步骤
```

## 边界

- 单节点、单进程调度器；SQLite 负责跨连接写序列化，队列锁负责进程内单写者。不是分布式集群中间件。
- `exactly_once` 指确认记录和计数的恰好一次，不承诺消费者外部副作用恰好一次。
- API 与消费者端点默认只允许回环主机。CLI 的 `serve` 也只允许绑定回环地址。
- 不使用 Docker、消息代理、云服务或外部网络。
