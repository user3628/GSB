# SQLite 事务与强制终止恢复协议

## 写意图协议

每个业务写操作执行以下顺序：

1. 在独立事务中写入 `PREPARED` 意图并提交。
2. 开启 `BEGIN IMMEDIATE` 写事务。
3. 完成业务写入，例如事件、窗口版本、熔断版本、配额版本、复合快照。
4. 在同一事务内把意图更新为 `COMMITTED`。
5. 提交事务。

若业务异常，事务回滚，意图在另一事务中标记为 `ABORTED_REJECTED`。若进程在步骤 2-4 中被强制终止，SQLite 回滚未提交事务，意图保留为 `PREPARED`。

## 不可变版本与头指针

不可变表：

- `window_versions`
- `breaker_versions`
- `quota_versions`
- `composite_snapshots`
- `breaker_transitions`

头指针表：

- `window_heads`
- `breaker_heads`
- `quota_heads`
- `composite_heads`

复合快照显式链接同一窗口版本、熔断版本、配额版本。限流裁决和熔断/配额查询都从复合头读取一个完整快照，不会拼装不同提交版本。

## 启动恢复

启动时在 SQLite 事务内：

1. 查询所有 `PREPARED` 意图。
2. 对 `INGEST` 意图，如果批次行不存在，则插入状态为 `ABORTED` 的批次事实，原因 `RECOVERED_INCOMPLETE_INTENT`；若存在 `COMMITTED` 批次则不改写。
3. 将意图标记为 `ABORTED_RECOVERED`。
4. 校正窗口、熔断、配额头到最大不可变版本。
5. 校正复合头到最大复合快照。
6. 执行 `PRAGMA quick_check`。
7. 把报告写入 `recovery_runs`。

恢复报告比较恢复前后的复合版本，输出 `previous_complete_versions_preserved`。报告只包含数据库事实，不包含被终止进程未能写出的退出信息。

## 故障注入

演示和测试使用环境变量故障点，不参与正常业务：

```text
RATEGUARD_FAULT=exit_after_intent
RATEGUARD_FAULT=exit_after_window_version
RATEGUARD_FAULT=exit_after_breaker_version
RATEGUARD_FAULT=transition_after_intent
RATEGUARD_FAULT=decision_after_intent
```

命中故障点时进程调用 `os._exit(87)`。重启后可以验证 SQLite 原子回滚与恢复报告。

## 边界保证

- 保证：已提交业务版本不被恢复过程删除或覆盖。
- 保证：恢复后不存在 `PREPARED` 意图。
- 保证：被中止的摄入批次可通过批次查询看到 `ABORTED`。
- 不保证：旧进程退出时一定生成退出报告。
- 不保证：调用方在连接断开前一定收到成功响应；调用方应以幂等键重试并查询批次状态。

