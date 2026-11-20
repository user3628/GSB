# 实际验证记录

验证日期：2026-11-21  
执行环境：WSL2 Ubuntu 26.04、Python 3.11.18、SQLite（Python 标准库）、FastAPI 0.165.0、uvicorn 0.54.0、pytest 9.2.0。

## 1. 环境

```text
$ .venv/bin/python --version
Python 3.11.18

$ .venv/bin/python -m pip show fastapi uvicorn pytest httpx | grep -E '^(Name|Version):'
Name: fastapi
Version: 0.165.0
Name: uvicorn
Version: 0.54.0
Name: pytest
Version: 9.2.0
Name: httpx
Version: 0.28.1
```

## 2. 自动化测试

```text
$ .venv/bin/pytest -q
29 passed
```

覆盖点：

- 非法帧类型、负流号、未打开流 DATA：`E_FRAME_INVALID`，整批无持久化推进。
- 非法状态迁移：`E_INVALID_TRANSITION`，连接和迁移表均无部分写入。
- 本地 HEADERS END_STREAM 不得越过已排队 DATA；整样本回滚。
- 流级窗口耗尽：`E_FLOW_CONTROL_BLOCKED`，携带连接窗口、流窗口、所需字节。
- WINDOW_UPDATE 超初始上限：确定性截断并写入协议违例。
- END_STREAM：发送完成后迁移 `OPEN -> HALF_CLOSED_LOCAL`。
- RST_STREAM：重置流不进入调度，任务取消。
- 同 ID 同内容幂等提交；同 ID 异内容冲突；8 线程并发和双引擎实例并发重复提交均只推进一次。
- 调度进行中持续读取连接快照，验证 `initial_window - window == sent_total` 不出现撕裂。
- 精确/近似阈值切换；小样本轨迹逐条一致；大样本候选截断和误差上界。
- 不同 `batch_size` 的样本提交，轨迹、轮次、窗口和状态迁移完全一致。
- 真实子进程在调度第 2 轮前被强杀，重启恢复后只保留第 1 轮提交边界。
- 1000 条流、每流 1 KiB，候选阈值 256/上限 64：发生有界近似截断，最终 1,024,000 字节全部发送，待发归零。
- 单流 100,000,000 字节、每轮预算 1,000,000 字节：100 轮精确完成，连接窗口剩 1024，待发归零。
- 历史样本轨迹保存调度完成时的连接/流窗口快照；后续继续调度其他样本不会改写旧运行的历史查询结果。

## 3. HTTP 与演示

```text
$ .venv/bin/python scripts/demo.py --keep
[startup] uvicorn listening on http://127.0.0.1:39183; db=.../var/demo.db

[1] 完整多路复用调度 COMPLETED
  window: connection=53035/65535, selected_bytes=12500
  transitions: s1 IDLE->OPEN; s3 IDLE->OPEN; s5 IDLE->OPEN;
               s3 OPEN->HALF_CLOSED_LOCAL (data_end_stream_sent)

[2] 窗口不足 E_FLOW_CONTROL_BLOCKED
  stream=1 connection_window=500 stream_window=0 required=700 pending=700
  reason: stream flow-control window is exhausted

[3] RST_STREAM 与 END_STREAM
  states: s7=RESET, s9=HALF_CLOSED_LOCAL
  transitions: s7 IDLE->OPEN; s9 IDLE->OPEN;
               s7 OPEN->RESET (rst_stream);
               s9 OPEN->HALF_CLOSED_LOCAL (data_end_stream_sent)

[4] WINDOW_UPDATE 超初始窗口截断
  stream=0 requested=1000 applied=0 limit=1000
  stream=1 requested=900 applied=0 limit=900

[5] 调度第 2 轮前强制终止进程，exit=137
  action=ROLLED_BACK_TO_LAST_COMMITTED_BOUNDARY
  committed_rounds=1 last_round=1 connection_window=64511 pending_bytes=8976
  trace_status=ABORTED trace_rounds=1 pending_intents=0

DEMO OK
```

演示核验摘要中的正常轨迹摘要为 `ca94489c4909ae2d`。该摘要由固定样本的轨迹和已提交连接快照规范化计算，可用于本机重复运行时的快速比对。

## 4. 分批一致性核验

`tests/test_consistency.py` 将 `samples/normal_multiplex.json` 分别按 `batch_size=1` 与 `batch_size=64` 提交到两个独立数据库，断言：

```text
ingest.transitions == ingest_many.transitions
ingest.connection  == ingest_many.connection
ingest.queued_data == ingest_many.queued_data
run.trace          == run_many.trace
run.rounds         == run_many.rounds
run.connection     == run_many.connection
```

两路均发送 `12500` 字节，连接窗口均停留在 `53035`。

## 5. 强制终止恢复核验

测试在子进程中设置 `H2WB_CRASH_AT_ROUND=2`。第 1 轮完整提交后，第 2 轮意图已提交、状态事务未提交时调用 `os._exit(137)`。重启后：

- `recovery_reports.action = ROLLED_BACK_TO_LAST_COMMITTED_BOUNDARY`
- `committed_round_count = 1`
- `last_committed_round = 1`
- 恢复后的连接窗口等于同一固定样本正常执行时第 1 轮后的窗口
- 恢复轨迹仅包含第 1 轮，运行状态为 `ABORTED`
- `pending_intents = []`
- 第二次重启不会重复生成恢复报告

## 6. Windows 说明

当前执行容器为 WSL2/Linux，已实测真实 uvicorn 回环端口可访问。仓库提供 `scripts/bootstrap.ps1`、`scripts/start.ps1`、`scripts/demo.ps1` 用于 Windows PowerShell；未在物理 Windows 主机上重复执行。Windows 运行时需确保使用项目 `.venv\Scripts\python.exe`，数据库路径可由 `-DbPath` 指定。
