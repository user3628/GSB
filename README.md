# HTTP/2 多路复用诊断工作台

面向本地回环环境的确定性 HTTP/2 多路复用诊断系统。输入固定帧序列样本，系统按连接级/流级流量窗口推进状态，以权重和优先级组织 DATA 调度，持久化每条流的调度轨迹、状态迁移和恢复边界。整条链路只使用本机 HTTP、Python 3.11 和 SQLite，不依赖 Docker 或外部网络。

## 交付内容

- `h2workbench/`：帧校验、流状态机、流量窗口、加权调度器、SQLite 持久化、FastAPI 接口。
- `samples/`：正常多路复用、窗口阻塞、流重置/结束、窗口上限截断、强杀恢复样本。
- `tests/`：29 项 pytest，覆盖事务回滚、窗口、状态、精确/近似调度、并发可见性、历史快照、千流截断、100 MB 预算和真实子进程强杀恢复。
- `scripts/demo.py`：启动真实 uvicorn 进程，完成正常调度、阻塞、重置、窗口截断和强制终止恢复演示。
- `scripts/start.sh`、`scripts/bootstrap.sh`：Linux/WSL 本地启动脚本。
- `scripts/start.ps1`、`scripts/bootstrap.ps1`、`scripts/demo.ps1`：Windows PowerShell 本地启动脚本。
- `docs/design.md`：调度和恢复不变量的技术说明。
- `docs/verification.md`：本次实际验证命令、结果与核验摘要。

## 环境与启动

本项目实际使用 Python `3.11.18` 验证。`requirements.txt` 锁定 FastAPI、uvicorn、pytest 和 httpx 版本。

Linux / WSL：

```bash
./scripts/bootstrap.sh
.venv/bin/python -m h2workbench.server --db var/h2workbench.db --host 127.0.0.1 --port 8765
curl http://127.0.0.1:8765/api/health
```

若使用 `uv` 管理解释器，可先执行 `uv python install 3.11`，再运行 `./scripts/bootstrap.sh`。仓库中的 `.python-version` 固定为 `3.11`。

Windows PowerShell：

```powershell
winget install Python.Python.3.11   # 若本机尚未安装 Python 3.11
.\scripts\bootstrap.ps1
.\scripts\start.ps1 -DbPath var\h2workbench.db -Port 8765
Invoke-RestMethod http://127.0.0.1:8765/api/health
```

`server.py` 拒绝绑定 `127.0.0.1`、`localhost`、`::1` 之外的地址。打开 `http://127.0.0.1:8765/` 可查看诊断工作台页面。

## 样本格式

每个样本提交到 `POST /api/samples`：

```json
{
  "sample_id": "normal-001",
  "connection_id": "conn-001",
  "batch_size": 64,
  "config": {
    "connection_window": 65535,
    "stream_window": 65535,
    "round_budget": 16384,
    "candidate_threshold": 256,
    "candidate_limit": 64
  },
  "frames": [
    {
      "connection_id": "conn-001",
      "stream_id": 1,
      "frame_type": "HEADERS",
      "flags": ["END_HEADERS"],
      "payload_bytes": 0,
      "window_increment": 0,
      "priority": 0,
      "weight": 4
    },
    {
      "connection_id": "conn-001",
      "stream_id": 1,
      "frame_type": "DATA",
      "flags": ["END_STREAM"],
      "payload_bytes": 4096,
      "window_increment": 0
    }
  ]
}
```

- `frame_type` 支持 `HEADERS`、`DATA`、`WINDOW_UPDATE`、`RST_STREAM`、`SETTINGS`；别名 `type` 也可用。
- `flags` 支持字符串、数组或整数位掩码；使用 `END_STREAM`、`END_HEADERS`、`ACK`、`CONNECTION_CLOSE` 等名称。
- DATA 表示本地待发送帧，只允许出现在已打开且可本地发送的流上。
- `batch_size` 只影响处理批次记录，不参与内容哈希；同一份帧在任意批次划分下得到相同的最终状态、窗口和调度轨迹。
- 连接配置只在连接首次创建时生效，后续样本沿用已持久化配置。

## 业务语义

### 窗口

- 每个连接保存 `initial_connection_window` 和 `connection_window`；每个流保存 `initial_window`、`window`、`sent_unacked`。
- 发送 DATA 时同时扣减连接窗口与流窗口，并增加 `sent_unacked`/`sent_total`。
- 连接或流任一窗口不足时，已能发送的字节先按预算提交；无法继续的流产生 `E_FLOW_CONTROL_BLOCKED`，上下文同时包含连接余量、流余量、所需字节、待发送字节和原因。
- WINDOW_UPDATE 增量被限制在初始窗口以内。超出部分截断为可应用余量，并写入 `protocol_violations`；已发送未确认字节按实际应用增量确认。

### 状态机

| 触发 | 合法源状态 | 目标状态 |
| --- | --- | --- |
| 本地 HEADERS | IDLE | OPEN |
| 本地 HEADERS + END_STREAM | IDLE / OPEN | HALF_CLOSED_LOCAL |
| 远端 HEADERS | IDLE | OPEN |
| 远端 HEADERS + END_STREAM | IDLE / OPEN | HALF_CLOSED_REMOTE |
| 本地 DATA + END_STREAM 发送完成 | OPEN | HALF_CLOSED_LOCAL |
| 本地 DATA + END_STREAM 发送完成 | HALF_CLOSED_REMOTE | CLOSED |
| RST_STREAM | OPEN / HALF_CLOSED_LOCAL / HALF_CLOSED_REMOTE | RESET |
| 连接关闭 | 任意活动状态 | CLOSED |

`CLOSED`、`RESET`、`HALF_CLOSED_LOCAL` 不允许再发送本地 DATA；`RESET`/`CLOSED` 流不会被调度，其待发送任务被取消。非法迁移返回 `E_INVALID_TRANSITION`，样本帧不会产生任何已提交状态变化。

### 调度

- 每个调度轮有固定 `round_budget`。优先级数字越小越优先；不同优先级按 `priority_scale` 分配轮预算，同一优先级按流权重进行整数比例分配。
- 精确模式考虑所有可发送流；当可发送流数超过 `candidate_threshold` 时，使用确定性候选排序和 `candidate_limit` 截断。
- 近似轮输出 `candidate_count`、`selected_candidate_count`、`truncated_count`、被遗漏字节上界、遗漏权重比例和说明。未选流在后续轮重新排名，不会永久丢失。
- 每个运行结果包含逐流 `diagnoses`：已发送字节、参与轮次、最终状态、窗口和结论（完成、部分、阻塞、重置、关闭等）。
- 小规模样本即使提高阈值也不会进入近似排序，精确/近似配置产生逐条相同的轨迹。
- 每轮先提交 `SCHEDULE_ROUND` 意图，再在单个 SQLite 事务中同时更新连接窗口、流窗口、任务余量、状态迁移和轨迹。进程强杀只可能留下未提交意图，恢复时标记为中止，不会留下半轮状态。

### 一致性与并发

- 每个连接一个进程内串行写锁；全部写路径使用 `BEGIN IMMEDIATE`。
- 查询使用只读事务快照，连接窗口和所有流窗口在同一次快照中读取，不会看到“流窗口已减、连接窗口未减”的中间态。
- 同一 `sample_id` 相同内容只推进一次；并发重复提交等待首个提交并复用结果。相同 ID 不同内容返回 `E_CONFLICTING_SAMPLE`。
- 生产启动应保持单个 uvicorn worker，以满足进程内单写者约束；SQLite 仍提供事务级文件一致性。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/samples` | 校验并提交完整帧样本 |
| `GET` | `/api/samples/{sample_id}` | 样本元数据与调度状态 |
| `POST` | `/api/samples/{sample_id}/schedule` | 触发该样本调度；重复调用复用结果 |
| `GET` | `/api/samples/{sample_id}/trace` | 样本调度轨迹与阻塞项 |
| `POST` | `/api/connections/{id}/schedule` | 调度连接上全部待发送 DATA |
| `GET` | `/api/connections` | 连接列表 |
| `GET` | `/api/connections/{id}` | 一致快照：连接窗口、各流窗口与状态 |
| `GET` | `/api/connections/{id}/streams/{stream_id}` | 单流窗口与状态 |
| `GET` | `/api/connections/{id}/trace` | 连接调度轨迹，可按样本/流过滤 |
| `GET` | `/api/connections/{id}/transitions` | 状态迁移记录 |
| `GET` | `/api/connections/{id}/violations` | 窗口截断等协议违例 |
| `POST` | `/api/connections/{id}/window-update` | 追加一条本地 WINDOW_UPDATE 意图 |
| `POST` | `/api/connections/{id}/close` | 关闭连接并停止调度 |
| `GET` | `/api/recovery` | 仅基于已持久化事实的恢复报告 |

统一错误体：

```json
{
  "error": {
    "code": "E_FLOW_CONTROL_BLOCKED",
    "message": "one or more DATA frames are blocked by flow-control windows",
    "connection_id": "conn-001",
    "context": {
      "connection_window": 0,
      "streams": [
        {"stream_id": 1, "connection_window": 0, "stream_window": 100, "required_bytes": 100}
      ]
    }
  }
}
```

核心业务码：`E_FRAME_INVALID`、`E_FLOW_CONTROL_BLOCKED`、`E_INVALID_TRANSITION`、`E_CONFLICTING_SAMPLE`、`E_SAMPLE_NOT_FOUND`。另有连接/流不存在、配置错误、运行中止等扩展码。

## 恢复

数据库包含 `intents`、`schedule_rounds` 和 `recovery_reports`。调度轮次顺序为：

1. 提交 `RUNNING` 调度运行记录。
2. 提交 PENDING 意图记录。
3. 在事务中同时应用本轮窗口、任务、状态和轨迹，并把意图标记为 COMMITTED。

重启扫描 PENDING 意图，将对应运行标记为 `ABORTED`，恢复到最后一个已提交轮次；恢复报告只列出已提交轮数、最后提交轮次、连接窗口、状态计数和待发送字节，不承诺强杀进程生成完整退出报告。

演示/测试使用环境变量 `H2WB_CRASH_AT_ROUND=2`（或 `H2WB_CRASH_AFTER_INTENT=1`）在意图提交后执行 `os._exit(137)`；不要在生产运行中设置这些故障注入变量。

## 测试与演示

```bash
.venv/bin/pytest -q
.venv/bin/python scripts/demo.py
```

本次实际结果：`29 passed`；演示脚本以退出码 `137` 强杀真实 uvicorn 进程后恢复成功，报告 `committed_rounds=1`、`last_round=1`、`trace_status=ABORTED`、`pending_intents=0`。千流/1 MiB 与单流/100 MB 规模烟雾测试均精确归零待发字节。详见 `docs/verification.md`。

## 已知边界

- 这是调度诊断模型，不实现 TLS、HPACK 编解码或真实网络 socket 流发送。
- 优先级采用确定性的多级权重策略，不实现 RFC 7540 的完整依赖树/独占权重语义。
- SQLite 的单写者串行由本进程连接锁保证；不要用多个 uvicorn worker 同时写同一数据库。
- 当前容器为 WSL2/Linux，uvicorn 回环端口和完整演示已实测；Windows 启动脚本已提供但未在 Windows 主机上执行。
