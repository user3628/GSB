# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）  
执行环境：WSL2 Linux，x86_64；代码仅使用 Python/SQLite/FastAPI 与回环 HTTP，可在 Windows Python 3.11+ 运行。  
Python：3.14.4  
FastAPI：0.165.0  
uvicorn：0.54.0  
NumPy：2.5.5  
pytest：9.2.0  
httpx：0.28.1（TestClient 使用）

## 1. 自动化测试

命令：

```bash
python3 -m pytest -q
```

结果：

```text
............                                                             [100%]
16 passed, 1 warning in 1.16s
```

唯一警告来自 FastAPI/Starlette 的 `TestClient` 弃用提示（建议未来迁移 `httpx2`），不影响测试结果。

测试覆盖：

- 固定轨迹的慢启动、拥塞避免、最小窗口不变量、RTO 上下界；
- 两次乱序 ACK 不计入重复 ACK；重复 ACK 不更新 RTT；
- 三次重复 ACK 快速重传、超时后阈值/窗口回退与一个 RTT 的增长抑制，包括超时抑制窗口内快速恢复也不得抬高窗口；
- 整批与四批拆分后的 26 个快照逐位一致；
- 推进中使用 `wait_ms` 的并发查询与最终快照一致；
- 同一 `batch_id` 8 线程并发提交只推进一次；
- 并发不同 run 不共享检查点/意图；
- `E_TRACE_INVALID`、`E_INVALID_TRANSITION`（含批次内前半合法后半非法且整批不推进）、`E_STALE_BASE`、`E_BATCH_NOT_FOUND`、`E_CHECKPOINT_CORRUPT`；
- 真实子进程 `SIGKILL` 后恢复、回滚和从检查点续算；
- 意图链完整性与单批延迟预算。
- 大批次延迟预算：默认单批上限收紧到 10000 事件，独立压力测量 10000 事件响应约 3618.951 ms，低于 5000 ms。
- 多个连接在同一全局轨迹中独立推进，互不串扰。
- 推进中的不稳定查询返回 `E_SNAPSHOT_NOT_READY`，而早于提交头的稳定时刻可立即读取。

## 2. 端到端演示

命令：

```bash
python3 scripts/demo.py
```

退出码：`0`。脚本只访问 `127.0.0.1` 上随机空闲端口，不使用外部站点、账号或容器。

### 2.1 固定轨迹

样本：`samples/fixed_trace.json`，连接 `c1`，26 个事件，覆盖：

- `0-60 ms`：SYN、握手 ACK、四个数据段、慢启动 `cwnd 4000 -> 8000`；
- `110 ms`：进入拥塞避免，`cwnd 8000 -> 9174`，`ssthresh=8000`；
- `115/119 ms`：乱序 ACK（`ack_seq=7000/7600 < 8000`），不计入重复 ACK；
- `116-118 ms`：三次重复 ACK，第三次执行快速重传：`cwnd 9174 -> 7587`，`ssthresh 8000 -> 4587`；
- `200 ms`：超时，`cwnd 7587 -> 4000`，`ssthresh 4587 -> 3793`，增长阻塞到 `217031 us`；
- `205 ms`：新 ACK 到达但早于阻塞边界，窗口保持 `4000`，原因 `TIMEOUT_GROWTH_LAG_SUPPRESSED`；
- `220 ms`：超过一个 SRTT 后恢复增长，`cwnd 4000 -> 4600`；
- `300/310/320 ms`：FIN_WAIT、TIME_WAIT，以及 TIME_WAIT 内的重复确认。

最终诊断计数：

```json
{
  "send_events": 12,
  "ack_events": 12,
  "loss_events": 1,
  "timeout_events": 1,
  "dup_ack_events": 3,
  "new_acks": 5,
  "duplicate_acks": 5,
  "out_of_order_acks": 2,
  "slow_start_acks": 1,
  "congestion_avoidance_acks": 2,
  "fast_retransmits": 1,
  "fast_recovery_inflates": 0,
  "timeout_growth_suppressed": 1,
  "rtt_updates": 4
}
```

最终连接状态：`TIME_WAIT`，`cwnd=4600`，`ssthresh=3793`，`srtt_us=16652`，`rttvar_us=12007`，`rto_us=200000`（处于声明上下界 `[200000, 60000000]` 内）。

### 2.2 整批与分批一致性

- 整批：`batch_id=full-all`，处理 26 个事件，响应实测约 `18.023 ms`，预算 `5000 ms`，`within_latency_budget=true`。
- 分批：`0..5`、`6..11`、`12..17`、`18..25` 四批，每批使用上一批 `committed_checkpoint_id` 作为下一批 `expected_base_checkpoint_id`。

`POST /v1/runs/compare` 结果：

```json
{
  "identical": true,
  "left_snapshots": 26,
  "right_snapshots": 26,
  "left_snapshot_digest": "d70273b0a076b82cf405b27ae32d56d2d2eff8d7e4eb09a41115526295d6cf14",
  "right_snapshot_digest": "d70273b0a076b82cf405b27ae32d56d2d2eff8d7e4eb09a41115526295d6cf14",
  "first_mismatch": null
}
```

### 2.3 并发查询

在分批运行最后一个批次提交期间，并行查询仿真时刻 `0/10/110/118/200/220/310/320 ms`，全部以 `wait_ms=6000` 获取稳定快照；与整批运行相同查询时刻的 `snapshot_sha256` 逐一相等。

示例：

```text
at 118 ms -> event 17, ACTIVE,   sha256=2b5cd141933a681edad0c0419054d3b3ce456a85cd3534f5187951b4d9b7ba31
at 220 ms -> event 22, FINALIZED, sha256=35701b46d9e95395ad25d27f350ecaa5b5412b2bc4bdadd4decb5b32d9c2eebe
at 320 ms -> event 25, FINALIZED, sha256=ae0fd16e0e62756df602c7e5fa6c7d72cc5928bd03e29a04bfa7cb93bbf88e9a
```

每个快照载荷同时包含状态机和窗口，来自同一 SQLite 提交版本。

### 2.4 强杀与恢复续算

演示启动的服务定时按目标批次注入强杀：`crash-batch-2` 完成第 5 个事件检查点后终止。

观测事实：

```json
{
  "batch_id": "crash-batch-2",
  "server_exit_code": -9,
  "client_error": "RemoteDisconnected: Remote end closed connection without response",
  "crash_after_checkpoints": 5
}
```

重启后自动恢复报告：

```json
{
  "forced_termination_detected": true,
  "incomplete_batches_observed": 1,
  "aborted_batches": [
    {
      "batch_id": "crash-batch-2",
      "outcome": "ABORTED_AND_ROLLED_BACK",
      "observed_processed_count": 5,
      "deleted_checkpoints": 5,
      "deleted_snapshots": 5,
      "rollback_checkpoint_id": "ckpt_f3b5be5cd62a4eec8a7f21da226caf44",
      "rollback_event_index": 17,
      "rollback_sim_time_ms": 118
    }
  ],
  "basis": "persisted_sqlite_rows_only",
  "disclaimer": "This report contains only facts persisted before the forced termination. No complete graceful-exit report is promised, and continuation must start from the reported persisted checkpoint."
}
```

随后提交新批次 `crash-resume-batch`，从事件 `18` 续算到 `25`，处理 8 个事件，响应约 `15.415 ms`。`full-run` 与 `crash-run` 的最终比较：

```json
{
  "identical": true,
  "left_snapshots": 26,
  "right_snapshots": 26,
  "left_snapshot_digest": "d70273b0a076b82cf405b27ae32d56d2d2eff8d7e4eb09a41115526295d6cf14",
  "right_snapshot_digest": "d70273b0a076b82cf405b27ae32d56d2d2eff8d7e4eb09a41115526295d6cf14",
  "first_mismatch": null
}
```

### 2.5 错误码演练

演示通过 HTTP 实际得到：

| 业务码 | HTTP | 场景 |
|---|---:|---|
| `E_TRACE_INVALID` | 400 | 非法 `event_type` |
| `E_INVALID_TRANSITION` | 409 | `CLOSED` 连接收到 ACK |
| `E_STALE_BASE` | 409 | 活动 run 提交错误基准检查点 |
| `E_BATCH_NOT_FOUND` | 404 | 查询不存在的批次 |

`E_CHECKPOINT_CORRUPT` 由 pytest 直接破坏 SQLite 检查点内容后验证：读取检查点和继续提交均返回 500，且不自动修复。

## 3. 复现产物

- 完整结构化演示输出：`artifacts/demo_result.json`
- 完整控制台输出：`artifacts/demo_stdout.txt`
- 实际 SQLite 数据库：`data/demo.db`
- 强杀服务日志：`artifacts/demo_server_1.log`
- 重启服务日志：`artifacts/demo_server_2.log`

本轮断言：

```json
{
  "concurrent_queries_matched_final_snapshots": true,
  "forced_termination_recovered_from_checkpoint": true,
  "full_vs_recovered_bitwise_identical": true,
  "full_vs_split_bitwise_identical": true
}
```
