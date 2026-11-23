# 设计说明

## 分层

- `policy`、`requests`、`decision` 是纯计算层，不访问 SQLite，也不依赖系统时间。
- `engine` 将一个窗口的请求组织为交互初判和结算重算，维护修正与对账。
- `scheduler` 只负责会话门、等待队列、在途计数和冲突审计。
- `store` 负责 SQLite 迁移、单事务快照、版本链和恢复查询。
- `app` 组合策略、会话、结算与报告；`report` 只做确定性序列化。
- `anomaly`、`verify` 只通过外部样本和参数驱动上述层，不修改源码。

## 事务边界

策略加载事务：

```text
BEGIN
  allocate next policy version
  read previous snapshot and classify content replacement
  INSERT policy snapshot
  UPDATE policy sequence
  [optional crash point: exit 86]
COMMIT
```

窗口结算事务：

```text
BEGIN
  if window_id already committed:
    verify content digest and return existing snapshot
  allocate next settlement version
  INSERT settlement snapshot
  INSERT window progress and request results
  INSERT corrections and ledger entries
  [optional crash point: exit 86]
COMMIT
```

SQLite 的原子提交保证两类写入不会出现“快照已写、进度未写”的半状态。进程在提交前被终止时，WAL/回滚日志会丢弃整个事务。

## SQLite 表

- `snapshot_sequences(kind, next_version)`：policy 与 settlement 的独立版本序列。
- `snapshots`：完整内容、内容摘要、身份集合、变化类型、替换前后摘要、策略/窗口定位。
- `windows`：已提交窗口、会话、状态、策略版本、结算版本、请求数和完整报告片段。
- `window_results`：按 `(settlement_version, session, stream, seq)` 保存逐条裁决。
- `corrections`：可查询的修正码与修改前后决策。
- `ledger_entries`：逐次定点分摊账本。

所有读取查询显式 `ORDER BY`，避免 SQLite 物理顺序影响报告字节。

## 身份与整体替换

整体替换不能只看版本号，也不能只看完整内容摘要是否变化。实现同时比较：

1. 完整内容 SHA-256；用于判断是否 `SAME_CONTENT`。
2. 规范化身份集合；用于区分无交集、部分重叠和同集合改写。

策略身份是 `tunnel_id`；结算身份是 `session_id/stream_id/request_seq`。替换前的摘要从上一快照的 `after_summary` 复制到新快照的 `before_summary`，因此旧版本被删除或覆盖后仍可从历史快照回放。

## 确定性策略

- 策略、候选和报告数组均有显式稳定排序。
- 权重只使用 int64 与固定 scale，不使用浮点。
- 运行报告不包含生成时间、数据库路径、绝对路径或 goroutine 调度信息。
- fixed 延迟时钟按会话隔离，每个裁决的虚拟单调差值固定为 250us。
- 并发验收通过门屏障让配置中的峰值在途、峰值排队稳定可见，再释放处理。
- 报告 JSON 使用固定结构体和两空格缩进，末尾固定一个换行。

## 恢复语义

- 已提交窗口永久保留；同一窗口重放时比较完整内容摘要，一致则幂等返回，不一致返回 `E_RESULT_MISMATCH`。
- 未提交窗口无 `windows` 记录，最新结算版本也不前进，重启后按相同 `window_id` 重新结算。
- 策略链加载先检查 `1..latest` 的连续性。若有缺口，回退到连续前缀的最后一版；没有连续前缀则拒绝加载。
- 版本链不自动“修复”或删除异常快照，避免掩盖证据；验收报告只声明回退版本与原摘要。
