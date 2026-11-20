# 本地 TCP 连接状态与拥塞控制仿真裁决工作台

这是一个仅使用本地回环网络、SQLite 与确定性整数运算实现的 TCP 状态/拥塞控制仿真服务。它把毫秒事件轨迹推进为逐连接状态、拥塞窗口、慢启动阈值、RTT/RTO 估计，并为每个事件持久化检查点与可查询快照。

关键特性：

- 事件推进、整批预检、状态机非法迁移拒绝与稳定业务错误码。
- 慢启动、按 RTT 比例增长的拥塞避免、三次重复 ACK 快速重传/恢复、超时回退与至少一个 RTT 的增长抑制。
- RFC 6298 风格的整数 RTT/RTO 估计，不使用浮点数累计仿真状态。
- 每事件原子提交检查点与快照；整批和分批提交得到逐位一致的快照摘要。
- 单写者批次推进、幂等批次复用、并发只读快照、`expected_base_checkpoint_id` 乐观并发校验。
- 进程被强杀后扫描未完成批次与意图，回滚到最后一个完整检查点，并输出只基于已持久化事实的恢复报告。
- 固定样本、可重复端到端脚本、pytest 测试和本轮实测记录。

## 快速开始

目标环境为 Windows 上的 Python 3.11+；本轮验证在 WSL2 Linux 上执行了同一套本地代码。无需 Docker，不访问外部站点。

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest -q
python scripts\demo.py
```

如果依赖来自本地 wheelhouse：

```powershell
python -m pip install --no-index --find-links C:\offline-wheels -r requirements.txt
```

启动服务，必须只监听回环地址；保持单个 uvicorn worker，因为单写者协调是进程内协调加 SQLite `BEGIN IMMEDIATE`：

```powershell
$env:TCP_SIM_DB_PATH = "data\tcp_sim.db"
python -m uvicorn tcp_sim.api:app --host 127.0.0.1 --port 8765 --workers 1
```

健康检查：`GET http://127.0.0.1:8765/healthz`。

## 目录

```text
tcp_sim/                    FastAPI 服务、纯整数模拟器、SQLite 持久化与服务层
samples/fixed_trace.json    固定确定性事件轨迹
scripts/demo.py             整批/分批/并发查询/强杀恢复端到端演示
tests/                      模拟器、API 一致性、幂等并发、恢复与错误码测试
data/demo.db                演示实际生成的本地 SQLite 数据库
artifacts/demo_result.json  演示实际输出与断言结果
VALIDATION.md               本轮实际验证命令与结果
```

## 事件轨迹格式

每条记录必有：

```json
{
  "ts_ms": 118,
  "conn_id": "c1",
  "event_type": "dup_ack",
  "payload_bytes": 0,
  "ack_seq": 8000
}
```

- `ts_ms`：非负整数毫秒；同一批次及相对上次已提交时间必须全局非递减。相等时间戳按输入顺序裁决。
- `event_type`：`send`、`ack`、`loss`、`timeout`、`dup_ack`。大小写规范化，也接受对应中文别名。
- `payload_bytes`：非负整数。对 `send` 是数据字节数；对 `ack` 是本次确认载荷字节数。
- `ack_seq`：可选的累计确认序号。建议 `ack`/`dup_ack` 显式提供；缺失时 `ack` 派生为 `highest_ack_seq + payload_bytes`，`dup_ack` 派生为 `highest_ack_seq`。

控制语义：

- 新连接的第一个 `send` 视为 SYN，`CLOSED -> SYN_SENT`；`SYN_SENT` 中的后续 `send` 视为 SYN 重传自环。
- `SYN_SENT` 的首个 `ack` 完成握手，迁移到 `ESTABLISHED`。
- `ESTABLISHED` 中 `payload_bytes > 0` 的 `send` 只追加待确认数据，不改变状态；`payload_bytes == 0` 的 `send` 表示 FIN，迁移到 `FIN_WAIT`。
- FIN 被 ACK 后进入 `TIME_WAIT`。

### 合法状态迁移

| 当前状态 | 事件 | 下一状态 |
|---|---|---|
| `CLOSED` | `SEND` | `SYN_SENT` |
| `SYN_SENT` | `SEND` | `SYN_SENT` |
| `SYN_SENT` | `ACK` | `ESTABLISHED` |
| `SYN_SENT` | `LOSS` / `TIMEOUT` | `SYN_SENT` |
| `ESTABLISHED` | `SEND(bytes>0)` | `ESTABLISHED` |
| `ESTABLISHED` | `SEND(bytes=0)` | `FIN_WAIT` |
| `ESTABLISHED` | `ACK` / `DUP_ACK` / `LOSS` / `TIMEOUT` | `ESTABLISHED` |
| `FIN_WAIT` | `ACK` | `TIME_WAIT` |
| `FIN_WAIT` | `DUP_ACK` / `LOSS` / `TIMEOUT` | `FIN_WAIT` |
| `TIME_WAIT` | `ACK` / `DUP_ACK` / `LOSS` / `TIMEOUT` | `TIME_WAIT` |

其他组合返回 `E_INVALID_TRANSITION`。批次在写任何模拟状态前先完整预检；因此批次内前半段合法、后半段非法时，整批也不会推进。

## 拥塞控制与 RTT 裁决算法

所有窗口、阈值、RTT、RTO 均为整数（字节或微秒）。默认配置见 `samples/fixed_trace.json` 和 `tcp_sim/config.py`。

### 窗口增长

- 慢启动：`cwnd < ssthresh` 时，每个新 ACK 执行 `cwnd += payload_bytes`。达到或跨过阈值后，下一个新 ACK 进入拥塞避免。
- 拥塞避免：按 RTT 比例使用整数公式

```text
delta = max(1, floor(MSS * acked_bytes * base_rtt_us / (cwnd * srtt_us)))
cwnd += delta
```

- 三次重复 ACK：`ssthresh = max(cwnd // 2, min_cwnd)`，`cwnd = max(ssthresh + 3 * MSS, min_cwnd)`，进入快速恢复；后续重复 ACK 每个增加一个 MSS；新 ACK 退出恢复时 `cwnd = ssthresh`。
- 超时：`ssthresh = max(cwnd // 2, min_cwnd)`，`cwnd = initial_cwnd`，重复 ACK 计数清零，RTO 指数退避。
- 超时后的增长至少滞后一个 RTT：`growth_blocked_until_us = timeout_ts_us + (srtt_us if srtt_us > 0 else base_rtt_us)`。该窗口内的新 ACK 不增长窗口，并在快照决策中写明 `TIMEOUT_GROWTH_LAG_SUPPRESSED(...)`。
- 若超时抑制窗口内又出现三次重复 ACK，快速重传和恢复标记仍会执行，但恢复窗口不得高于抑制前窗口；额外重复 ACK 的窗口膨胀同样被抑制并写入原因。
- 所有重传、恢复和超时路径最终都钳制 `cwnd >= min_cwnd`。

### ACK 分类与 RTT 去重

- `ack_seq < highest_ack_seq`：乱序 ACK，忽略；不更新 RTT，不计入重复 ACK 次数。
- `ack_seq == highest_ack_seq`：重复 ACK；不更新 RTT，只增加重复计数。
- `ack_seq > highest_ack_seq`：新 ACK；只更新一次 RTT 并清零重复计数。
- RTT 样本来自最早被该 ACK 覆盖且未被重传的段；若覆盖段全部重传或没有可验证样本，则不更新。SYN_SENT 握手 ACK 可用最近 SYN 发送时间取一次样本。
- 整数估计：首样本 `srtt = sample`、`rttvar = sample / 2`；后续

```text
rttvar = (3 * rttvar + abs(srtt - sample)) // 4
srtt   = (7 * srtt + sample) // 8
rto    = clamp((srtt + 4 * rttvar) * backoff, min_rto_us, max_rto_us)
```

## 一致性与快照规则

每个事件在一个 SQLite 事务中原子写入：

1. `CHECKPOINT_WRITE` 与 `SNAPSHOT_COMMIT` 意图记录；
2. 完整模拟状态检查点及其 SHA-256；
3. 同时刻快照及其 SHA-256；
4. run 的最新检查点、事件游标和时间游标；
5. batch 已处理事件数；
6. 意图状态置为 `COMMITTED`。

快照 JSON 不包含 run/batch/checkpoint UUID，只包含模拟事实，因此同一轨迹在任意分批方式下的相同事件位置可直接逐位比较。`POST /v1/runs/compare` 对 26 个固定样本快照比较 SHA-256 序列，本轮整批、分批、强杀恢复三条路径均为相同摘要。

`GET /v1/runs/{run_id}/state?at_ms=...` 返回“已提交事件中，时间戳不大于 `at_ms` 的最后一个完整快照”。为了避免查询到会被后续同时间戳事件改变的中间结果：

- run 已 `FINALIZED` 时，目标时间稳定。
- run 仍 `ACTIVE` 且 `at_ms < committed_head_ts_ms` 时，目标时间稳定。
- 否则查询可在 `wait_ms` 内等待提交；超时返回 `E_SNAPSHOT_NOT_READY`，不会返回可能变化的旧结果。

查询在单次 SQLite 读事务中同时读取 run 游标与快照；窗口与状态来自同一快照载荷，不会看到“窗口已增长而状态仍旧”的拼接结果。

## 并发、幂等与批次基准

- 所有批次推进由进程内单写者锁串行化；SQLite 使用 WAL 与 `synchronous=NORMAL`，多读者可并发读取已提交快照。该同步级别覆盖“进程被强杀”的恢复目标，但不承诺机器掉电后的持久性。
- 同一 `batch_id` 并发重复提交时，第一个请求推进，其他请求等待并复用持久化结果；批次只推进一次。
- 同一 `batch_id` 携带不同请求摘要时返回 `E_IDEMPOTENCY_CONFLICT`。
- 分批续写应把上一批返回的 `committed_checkpoint_id` 作为下一批 `expected_base_checkpoint_id`；当前头部已变化时返回 `E_STALE_BASE`。
- 不同 run 有独立检查点、快照和意图；并发提交不同 run 的批次不会共享状态。
- 为获得该保证，请使用一个 uvicorn worker。横向多进程部署需要额外的跨进程写锁/选主，不在本工作台范围。

## SQLite 与强制终止恢复

核心表：`runs`、`batches`、`checkpoints`、`snapshots`、`intent_log`、`recovery_reports`。

进程启动时扫描 `RUNNING` 批次和残留意图：

- 找到批次基准检查点并校验 SHA-256；
- 删除该批次在基准之后写入的检查点与快照；
- 把 run 回退到基准事件索引/时间/检查点；
- 批次标记为 `ABORTED`，意图标记为 `ABORTED`/`ABANDONED`；
- 持久化恢复报告，包含强杀前已持久化的处理数量、回滚检查点、删除行数和 run 状态。

报告只引用 SQLite 中已有的事实，并明确声明：强制终止后不承诺存在完整优雅退出报告；续算必须从报告给出的持久化检查点开始。检查点摘要不一致时返回 `E_CHECKPOINT_CORRUPT`，不尝试静默修复。

演示/测试专用的确定性强杀钩子：

```powershell
$env:TCP_SIM_CRASH_BATCH_ID = "crash-batch-2"
$env:TCP_SIM_CRASH_AFTER_CHECKPOINTS = "5"
```

目标批次完成第 5 个事件检查点后，POSIX 使用 `SIGKILL`，Windows 使用 `os._exit(97)`。正常服务不要设置这两个变量。

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/simulations/batches` | 提交轨迹批次或分批后缀 |
| `GET` | `/v1/batches/{batch_id}` | 查询批次状态与持久化结果 |
| `GET` | `/v1/runs/{run_id}` | 查询 run 头部与配置 |
| `GET` | `/v1/runs/{run_id}/state?at_ms=&conn_id=&wait_ms=` | 指定仿真时刻的完整快照 |
| `GET` | `/v1/runs/{run_id}/window-trace?conn_id=` | 窗口/阈值/状态演进轨迹 |
| `GET` | `/v1/runs/{run_id}/diagnostics` | 计数、结论与最小窗口不变量 |
| `POST` | `/v1/runs/compare` | 两次仿真逐事件快照比对 |
| `GET` | `/v1/checkpoints/{checkpoint_id}` | 校验并读取检查点 |
| `GET` | `/v1/recovery/reports/latest` | 最近一次恢复报告 |
| `POST` | `/v1/recovery/scan` | 手动执行意图/未完成批次扫描 |
| `GET` | `/v1/error-codes` | 稳定错误码目录 |

提交示例：

```json
{
  "trace_id": "fixed-tcp-v1",
  "run_id": "full-run",
  "batch_id": "full-all",
  "expected_base_checkpoint_id": null,
  "final_batch": true,
  "config": {
    "mss_bytes": 1000,
    "initial_cwnd_bytes": 4000,
    "min_cwnd_bytes": 1000,
    "initial_ssthresh_bytes": 8000,
    "base_rtt_us": 40000,
    "min_rto_us": 200000,
    "max_rto_us": 60000000,
    "max_events_per_batch": 10000,
    "latency_budget_ms": 5000
  },
  "events": [
    {"ts_ms": 0, "conn_id": "c1", "event_type": "send", "payload_bytes": 0},
    {"ts_ms": 10, "conn_id": "c1", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0}
  ]
}
```

成功响应有顶层 `{"ok": true, "data": ...}`；业务错误有顶层 `{"ok": false, "error": {"code", "message", "context"}}`，便于自动区分。

批次响应包含 `latency_ms`、`latency_budget_ms`、`within_latency_budget`。单批默认最多 10000 个事件，固定样本整批响应预算为 5000 ms，本轮 26 事件实测约 18 ms，10000 事件压力测量约 3619 ms；超过单批上限请拆分并使用检查点基准续写。

## 错误码

必需错误码：

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_TRACE_INVALID` | 400 | 非法事件类型/字段/时间戳非单调/超批次上限 |
| `E_INVALID_TRANSITION` | 409 | 状态机未声明的迁移 |
| `E_BATCH_NOT_FOUND` | 404 | 批次标识不存在 |
| `E_CHECKPOINT_CORRUPT` | 500 | 检查点或快照摘要/JSON 不一致 |
| `E_STALE_BASE` | 409 | 批次基准不是 run 当前提交头 |

扩展错误码：`E_SNAPSHOT_NOT_READY`、`E_CONNECTION_NOT_FOUND`、`E_RUN_NOT_FOUND`、`E_RUN_FINALIZED`、`E_BATCH_ABORTED`、`E_IDEMPOTENCY_CONFLICT`、`E_CONFIG_INVALID`、`E_CONFIG_MISMATCH`、`E_REQUEST_INVALID`、`E_INTERNAL`。

## 测试与实测

```powershell
python -m pytest -q
python scripts\demo.py
```

本轮实际结果：pytest `16 passed`；演示脚本完成整批、四批拆分、并发等待查询、`E_TRACE_INVALID`/`E_INVALID_TRANSITION`/`E_STALE_BASE`/`E_BATCH_NOT_FOUND`、`SIGKILL` 退出码 `-9`、5 个已提交事件回滚、从事件 17（118 ms）检查点续算，并与整批运行得到相同的 26 快照摘要。完整记录见 `VALIDATION.md`，原始输出见 `artifacts/demo_result.json`。

固定样本已覆盖：慢启动、拥塞避免、两次乱序 ACK、三次重复 ACK 快速重传、一次超时重传与增长滞后、FIN_WAIT、TIME_WAIT。
