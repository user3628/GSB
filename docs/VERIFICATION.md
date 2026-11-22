# 实际验证记录

本记录来自本工作区实际执行，不是设计预期。机器可读原始报告见 `artifacts/verification_report.json`。

## 环境

- 时间：2026-11-22 20:26:29 UTC（Asia/Shanghai 2026-11-23）
- 平台：WSL2 Ubuntu，Linux 6.18，非容器
- Python：3.14.4
- SQLite：3.46.1
- Flask：3.1.3
- requests：2.34.2
- 网络：仅 `127.0.0.1` 回环
- 数据库：每个场景独立 SQLite 文件，WAL 模式

实现使用 Python 3.10+ 兼容语法，Windows 10+ 运行步骤见 `README.md` 与 `scripts/setup_windows.ps1`。

## 复现命令

```powershell
.\.venv\Scripts\python.exe scripts\run_experiment.py --output artifacts\verification_report.json
.\.venv\Scripts\python.exe -m pytest -q
```

实测结果：

```text
21 passed in 10.25s
```

## 场景汇总

| 场景 | 结果 | 关键实测事实 |
|---|---|---|
| 恰好一次自动确认 + 重复确认 | PASS | 3 条派发、3 条确认；重复确认 HTTP 409 `E_DUPLICATE_ACK`；活动投递仍为 0；消费组位点为 4 |
| 至少一次可见性超时重投 | PASS | 首轮 1 条投递；租约到期转为 `REDELIVERED`；第二轮重投 `attempt=2`；后继 `order-002` 保持 `PENDING`，确认后组位点为 2 |
| 挂起消费者并行隔离 | PASS | 20 条领取、17 条 HTTP 成功、1 条真实超时、2 条拒绝并死信；整轮约 0.631 秒；超时结果 `connection_closed=true`；挂起消息转 `REDELIVERED` |
| 预算降级确定性 | PASS | 严格模式 HTTP 429 `E_QUEUE_FULL` 且 0 次派发；非严格模式两次独立数据库均只领取 2 条、降级顺序完全一致；被降级 8 条保持 `PENDING` |
| 并发租约与请求幂等 | PASS | 4 个并发领取仅 1 个成功且仅 1 条投递记录；同一入队请求并发 4 次全部返回序号 1，队列 `next_seq=2` |
| 强制终止恢复 | PASS | 投递强杀退出码 97，恢复 `DISPATCHED -> PENDING`；确认强杀退出码 97，恢复 `ACKED -> DISPATCHED` |

## 关键原始观测

### 恰好一次

```json
{
  "round_status": "OK",
  "delivered": 3,
  "acked": 3,
  "duplicate_ack": {"http_status": 409, "code": "E_DUPLICATE_ACK"},
  "active_deliveries_after_duplicate": 0,
  "trace_events": ["ACKED", "CLAIMED", "HTTP_ACCEPTED", "ROUND_END", "ROUND_START"]
}
```

### 至少一次重投

```json
{
  "first_round": {"accepted": 1, "claimed": 1},
  "reap_actions": [
    {
      "attempt": 1,
      "delivery_id": 1,
      "message_id": "order-001",
      "new_state": "REDELIVERED",
      "reason": "VISIBILITY_TIMEOUT"
    }
  ],
  "redelivery": {
    "attempt": 2,
    "redelivery_count": 1,
    "second_round_redelivered_attempts": 1,
    "status": "REDELIVERED"
  },
  "group_offset_after_ack": 2,
  "successor_order_002_still_pending": true
}
```

### 并行超时隔离

```json
{
  "elapsed_seconds": 0.631,
  "claimed": 20,
  "accepted": 17,
  "timeouts": 1,
  "dead_lettered": 2,
  "timeout_connection_closed": true,
  "hang_message_state": "REDELIVERED",
  "error_codes": ["E_DISPATCH_REFUSED", "E_DISPATCH_TIMEOUT"]
}
```

### 确定性预算降级

```json
{
  "strict": {
    "http_status": 429,
    "code": "E_QUEUE_FULL",
    "dispatch_attempts": 0
  },
  "claimed": 2,
  "skip_order": [
    "budget-003", "budget-004", "budget-005", "budget-006",
    "budget-007", "budget-008", "budget-009", "budget-010"
  ],
  "pending": [
    "budget-003", "budget-004", "budget-005", "budget-006",
    "budget-007", "budget-008", "budget-009", "budget-010"
  ],
  "identical_degradation_across_fresh_databases": true
}
```

### 强制终止恢复

```json
{
  "dispatch": {
    "forced_exit_code": 97,
    "before_state": "DISPATCHED",
    "after_state": "PENDING",
    "report_action": "dispatch_rolled_back_to_pending"
  },
  "ack": {
    "forced_exit_code": 97,
    "before_state": "ACKED",
    "after_state": "DISPATCHED",
    "report_action": "ack_rolled_back_to_unconfirmed"
  }
}
```

## 自动化测试覆盖

`tests/test_store_semantics.py`：

- 队列序号单调无缺口与入队幂等。
- 非法状态迁移不改变版本与业务状态。
- 可见性超时、按序重投、重试耗尽死信。
- 恰好一次重复确认与待确认计数。
- 连续消费组位点。
- 8 路并发领取仅一个成功。
- 同请求标识并发只执行一次并复用结果。
- 写事务期间 4 个只读快照始终读取完整提交版本。

`tests/test_scheduler_http.py`：

- 恰好一次自动确认、轨迹与重复确认。
- 拒绝导致死信。
- 挂起超时后连接关闭、并发槽释放、其他队列继续。
- 全局并发和每队列在途上限。
- 每队列暂满的候选在租约释放后被重新领取，而不是被过早永久降级。
- 同优先级队列跨轮次持久化轮转。
- 优先级与有界准入队列 `E_QUEUE_FULL`。
- 严格队列满 429 且不产生投递。
- 预算耗尽确定性降级，消息保持 `PENDING`。
- 轮次墙钟时间受剩余预算约束。

`tests/test_recovery.py`：

- 投递中途 `os._exit(97)` 后恢复为 `PENDING`。
- 确认中途 `os._exit(97)` 后恢复为未确认。
- 恢复幂等。
- 恢复后生产序号继续无缺口。

## 未使用项

- 未使用 Docker 或容器。
- 未访问外部网络。
- 未使用外部消息代理或云服务。
- Windows 专用 API 未使用；实现保持跨平台。
