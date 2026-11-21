# 本地对象存储网关：分片上传会话协调组件

这是一个不依赖外部服务、可直接在单机运行的 Python 3.11 实现。它解决多上传会话并行推进时的分片幂等、写后回读、全局/后端并发限制、配额预算、时间预算降级、请求幂等和进程强杀恢复问题。

实现只访问 `127.0.0.1` 上的本地 HTTP 测试端点；运行期不需要 Docker、GPU 或网络服务。

## 快速开始

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt

pytest -q
python -m scripts.demo
```

版本固定为 Python 3.11、FastAPI `0.115.6`、uvicorn `0.34.0`、httpx `0.28.1`，开发测试使用 pytest `8.3.4`。

演示脚本会自行启动：

- 网关：`127.0.0.1:18100`
- 后端 A/B/C：`127.0.0.1:18101`、`18102`、`18103`
- SQLite：`artifacts/demo.sqlite3`
- 机器可读结果：`artifacts/demo-report.json`

也可以分别运行：

```bash
python scripts/run_backend.py --backend-id backend-a --port 18101
python scripts/run_gateway.py --port 18100 --db var/gateway.sqlite3
```

`sqlite3` 使用 Python 内置模块，不需要安装系统包。

## 目录

```text
app/
  backend_sim.py   可注入 normal/mismatch/hang/fail 的本地后端
  db.py            SQLite 模式、事务与启动恢复
  main.py          FastAPI 路由、统一错误与请求幂等包装
  manifest.py      顺序无关的对象清单摘要
  scheduler.py     并发闸门、预算裁决、配额预留、写后回读
  service.py       会话/分片业务规则与幂等请求
samples/
  backends.json    后端容量、并发、单会话分片上限与估算延迟
  sessions.json    固定会话、优先级、分片大小与可复算 SHA-256
scripts/
  demo.py          完整端到端演示
  sigkill_worker.py  用于真实 SIGKILL 的子进程调度器
tests/             12 个业务与恢复测试
```

## API

失败响应统一为：

```json
{
  "code": "E_QUOTA_EXCEEDED",
  "message": "backend quota cannot accommodate this part",
  "context": {
    "remaining_bytes": 24576,
    "requested_bytes": 30000
  }
}
```

主要端点：

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/v1/sessions` | 发起会话 |
| POST | `/v1/sessions/{id}/parts` | 提交分片 |
| POST | `/v1/sessions/{id}/finalize` | 显式校验连续性并汇总 |
| GET | `/v1/sessions/{id}` | 会话、分片、对象元数据与版本快照 |
| GET | `/v1/sessions/{id}/parts` | 分片清单 |
| POST | `/v1/scheduler/rounds` | 执行一轮并行调度 |
| GET | `/v1/scheduler/rounds/{id}` | 查询轮次轨迹 |
| GET | `/v1/backends` | 所有后端配额余量 |
| GET | `/v1/backends/{id}/quota` | 单后端配额 |
| GET | `/v1/recovery/report` | 最近一次启动恢复报告 |
| GET | `/v1/recovery/reports?limit=20` | 恢复报告历史 |
| GET | `/v1/audit/events?limit=100` | 持久化审计事件 |

`Idempotency-Key` 请求头或 JSON 的 `request_id` 都可作为幂等键。相同键、相同请求等待并复用第一次结果；相同键、不同请求返回 `E_CONFLICTING_REQUEST`。

### 发起会话

```json
{
  "object_key": "drive/alpha.dat",
  "backend_id": "backend-a",
  "max_parts": 5,
  "total_parts": 5,
  "priority": 10,
  "session_id": "sess-alpha",
  "conflict_policy": "reject_active"
}
```

数值更小的 `priority` 优先。`total_parts` 默认等于 `max_parts`，且不得超过它或后端单会话限制。对象键已有 `ACTIVE` 会话时返回 `E_SESSION_CONFLICT` 和现有 `existing_session_id`。

### 提交分片

```json
{
  "seq": 3,
  "size_bytes": 4096,
  "sha256": "fd0baf2f4ed3f7aeeb714fa049560ec4eaf8ce5b6dcc215436d403e8defde982",
  "arrived_at": "2026-01-01T00:00:03.000+00:00"
}
```

允许乱序到达，但 `seq` 必须在声明范围内。最终连续性和缺失序号在汇总时检查；缺口返回 `E_PART_SEQ_INVALID`，不改变会话状态。重复序号只在首次摘要一致时幂等成功；摘要不同返回 `E_PART_MISMATCH`，首次摘要不变。

### 调度轮次

```json
{
  "request_id": "round-001",
  "time_budget_ms": 2000,
  "queue_limit": 8,
  "write_timeout_ms": 800,
  "read_timeout_ms": 800,
  "session_ids": ["sess-alpha", "sess-beta"]
}
```

每个任务执行一次后端写和一次独立超时的回读。响应包含确定性排序的 `trace`、任务业务码、调度指标、配额变化和错误上下文。若一批任务全部失败，HTTP 状态映射到该业务码，完整轮次轨迹位于 `context.round`。

## 业务码

| 业务码 | 场景 |
|---|---|
| `E_SESSION_CONFLICT` | 同一对象键已有活动会话 |
| `E_PART_SEQ_INVALID` | 序号越界；或汇总时序号不连续 |
| `E_PART_MISMATCH` | 重复序号提交了不同摘要 |
| `E_SCHED_QUEUE_FULL` | 就绪任务超过全局并发与有界等待队列容量 |
| `E_BACKEND_TIMEOUT` | 写或回读的独立 HTTP 超时；连接已真实关闭 |
| `E_READBACK_MISMATCH` | 后端回读摘要与提交摘要不同 |
| `E_QUOTA_EXCEEDED` | 后端配额不足以容纳当前分片 |
| `E_CONFLICTING_REQUEST` | 幂等键被不同请求内容复用 |
| `E_BACKEND_UNAVAILABLE` | 连接拒绝或后端传输失败 |
| `E_BACKEND_BUSY` | 后端并发限制拒绝 |
| `E_SESSION_NOT_ACTIVE` | 已结束会话提交新分片 |
| `E_PARTS_INCOMPLETE` | 汇总时仍有分片未通过回读 |

`E_TIME_BUDGET_DEFERRED` 作为任务级降级依据出现，不被视为 HTTP 失败。`E_INVALID_REQUEST` 统一承载 Pydantic 参数校验失败。

## 一致性与调度设计

### 事务顺序

1. 轮次规划先持久化 `scheduling_rounds`、`round_tasks` 和 `schedule_intents(PREPARED)`。
2. 任务取得并发名额后，在一个 `BEGIN IMMEDIATE` 事务中原子完成配额预留、`quota_reservations(RESERVED)`、分片 `UPLOADING` 和意图 `UPLOADING`。
3. HTTP 写入与回读在事务外执行。每次请求使用独立 `httpx.AsyncClient` 和独立超时，并在 `finally` 中 `aclose()`。
4. 成功时在单个事务中同时提交：配额从 `reserved` 转 `committed`、分片 `VERIFIED`、意图 `COMMITTED`、会话完成数/版本、对象元数据 `DRAFT/READY` 和最终摘要。
5. 失败或超时在单个事务中释放预留、标记分片 `DEFERRED`、记录业务码与会话新版本。

因此不会出现“分片已计入完成但配额未预留/未提交”或“会话可汇总但回读未通过”的可见中间状态。会话查询在一个 SQLite 读事务中同时读取会话、分片、对象元数据和配额，并返回 `version` 与 `read_version`。

### 并发与预算

- 全局 `asyncio.Semaphore(global_concurrency)` 控制跨后端总并发。
- 每个后端独立 `Semaphore(max_concurrency)` 控制后端写入名额。
- 等待队列容量为 `min(请求 queue_limit, 配置 queue_limit)`；运行中的多个轮次共享准入计数器。超限轮次以 `E_SCHED_QUEUE_FULL` 拒绝，且不预留配额、不持有并发名额。
- 同一轮只接受最多 `global_concurrency + queue_limit` 个任务；同一后端同一分片存在活动意图时由部分唯一索引禁止重复派发。
- 预算采用确定性的准入成本账本：按 `priority ASC, session_created_at ASC, session_id ASC, seq ASC` 排序，每任务计入后端 `estimated_latency_ms`。剩余预算不足时，按此顺序标记 `DEFERRED`，并在 detail 中记录 `budget_consumed_ms`、`budget_remaining_ms`、`required_ms` 与 `downgrade_rank`。已经派发的任务不因预算被中止。
- 调度轮次之间串行执行，轮次内部的任务跨会话并行。

### 对象摘要

对象摘要是顺序严格确定的清单摘要：对 `seq` 升序的每个分片，追加规范化 JSON 行

```json
{"seq":1,"sha256":"...","size_bytes":4096}
```

再做一次 SHA-256。只包含序号、大小和分片摘要，因此与网络到达顺序、SQLite 行顺序无关。原始分片字节不经过网关，契约只携带大小与摘要，故这里明确命名为对象清单摘要。

### 强杀恢复

启动时扫描 `PREPARED/RESERVED/UPLOADING` 意图和 `RUNNING` 轮次：

- `RESERVED` 配额按意图回滚，预留记录改为 `RELEASED`。
- `UPLOADING` 分片恢复到意图记录中的首次稳定状态（通常为 `PENDING`）。
- 未完成轮次改为 `ABORTED`。
- 已 `COMMITTED` 的分片、配额和对象元数据保留。
- 恢复报告写入 `recovery_reports`，`scope` 固定为 `startup_recovery_only`，并明确说明强杀进程不可能产生完整优雅退出报告。

`scripts/demo.py` 实际启动 `scripts/sigkill_worker.py` 子进程，在观察到“2 个任务已提交 + 1 个任务持有配额并在挂起写中”后发送 `SIGKILL`，再重启网关读取恢复报告。

## 后端故障注入

每个后端进程支持：

```bash
curl -X POST http://127.0.0.1:18102/admin/mode \
  -H 'content-type: application/json' \
  -d '{"mode":"mismatch"}'

curl -X POST http://127.0.0.1:18102/admin/mode \
  -H 'content-type: application/json' \
  -d '{"mode":"hang","hang_seconds":30}'
```

模式包括 `normal`、`mismatch`、`hang`、`fail`。连接拒绝通过指向未监听的本地端口测试，网关返回 `E_BACKEND_UNAVAILABLE` 并回滚配额。后端自身也执行容量、单会话分片数、并发写入限制，并记录峰值并发。

摘要不一致的副本在同一摘要正常重试时会被修复；这使网关的 `DEFERRED` 重试具有真实修复效果。

## 实际验证结果

验证日期：2026-11-22（Asia/Shanghai）。实际命令：

```text
$ .venv/bin/pytest -q
12 passed, 1 warning in 4.74s

$ .venv/bin/python -m scripts.demo
checks: 59 passed
business codes observed: E_BACKEND_TIMEOUT, E_CONFLICTING_REQUEST,
  E_PART_MISMATCH, E_PART_SEQ_INVALID, E_QUOTA_EXCEEDED,
  E_READBACK_MISMATCH, E_SCHED_QUEUE_FULL, E_SESSION_CONFLICT
```

并行轮次实际轨迹为 12/12 任务 `COMMITTED`；全局峰值为 4，后端 A/B/C 峰值分别未超过 2/2/1。

| 会话 | 最终对象摘要 |
|---|---|
| `sess-alpha` | `1dbe2f0b68550d23abb60b2bf0d34d6be8c7c205618a62ad08ebf43f8e026ea1` |
| `sess-beta` | `c26b6c1f5f7e0cef01d79f258dc5cc46f7fe479e67c5dc82d67dbc234e319961` |
| `sess-gamma` | `39da5b8c0ec103055eb2a2b7d6e6b37f65449582d9ef2500723b695be71f90a1` |
| `sess-mismatch` | `f91183bea30b275ccf448ac4f3e3dafadd92d21b30c6052a5ff000609cc16c2c` |
| `sess-timeout` | `023d8982c54d5b2235cb79d0d94c8c49d7157d99addf5cb56aab7a3b5548f271` |
| `sess-crash-ok` | `68c8c129af107ed839c11f24ab951531c88079cb66baf62af9a9709f4ea5b374` |

关键配额结果：

```text
并行完成后   A committed=20480 reserved=0; B=16384/0; C=12288/0
配额耗尽后   B 仍为 committed=16384 reserved=0，剩余 24576
超时回滚后   C reserved=0，分片为 DEFERRED
强杀恢复后   A=26624/0; B=17408/0; C=20480/0
```

强杀恢复报告实际记录：

```text
scope=startup_recovery_only
aborted_rounds=[round-c4422d92ad764e73]
rolled_back_intents=2
committed_intents_preserved=2
sess-crash-ok=COMPLETED 2/2
sess-crash-hang=ACTIVE parts=[PENDING, PENDING]
```

完整轨迹、每次配额快照和检查明细在 `artifacts/demo-report.json`；终端实测输出在 `artifacts/pytest-output.txt` 与 `artifacts/demo-output.txt`。
