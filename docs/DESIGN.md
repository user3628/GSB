# 架构与一致性设计

## 模块

| 模块 | 职责 |
|---|---|
| `formats/binary.py` | 二进制帧、CRC、可选位图、大小端 |
| `formats/jsonl.py` | 分片 JSONL、顺序与损坏定位 |
| `formats/negotiation.py` | 格式检测、兼容矩阵、整体拒绝 |
| `policies.py` | 权重定点化与时间截断策略 |
| `normalization.py` | 版本差异到统一内部模型 |
| `service.py` | 注册、租约版本、并发续约复用 |
| `importer.py` | 冲突裁决、批次幂等、原子生效 |
| `probes.py` | 真实 HTTP/TCP/租约探测与超时关闭 |
| `scheduler.py` | 全局并发、有界队列、轮次预算、状态提交 |
| `recovery.py` | 未完成意图扫描与事实恢复 |
| `db.py` | SQLite schema、WAL、事务边界 |

## 写操作与意图日志

每个变更操作都先提交一条 `STARTED` 意图，再在业务事务内同时完成数据写入和意图状态更新：

```text
intent STARTED (autocommit)
        |
        v
BEGIN IMMEDIATE
  update instances / history / batch
  update intent COMMITTED
COMMIT
```

因此“业务事实已提交但意图仍 STARTED”不可能同时存在。崩溃后事务自动回滚，启动扫描只会看到真正未提交的 `STARTED` 意图。

注册、续约、导入、探测轮次都遵守此模式。导入额外先把批次写为 `STAGING`；探测额外先把每个尝试写为 `PENDING`。这两类状态是崩溃恢复的持久化线索。

## 探测调度

- 调度器维护全局活动任务数和等待列表。
- 活动数小于 `max_concurrency` 时立即执行；否则等待列表未满 `queue_size` 时入队。
- 容量恰好是 `max_concurrency + queue_size`，额外任务返回 `E_PROBE_QUEUE_FULL`。
- 轮次预算到期会取消活动任务、移除等待任务；HTTP/TCP 的 `finally` 会关闭 writer/socket。
- 所有已执行结果、权重调整、租约过期判定、探测历史和实例健康状态在一个 SQLite 事务中提交。
- 探测开始后实例若被续约或重注册，乐观版本检查会跳过该结果，避免旧探测覆盖新注册。

失败探测按固定步长降低权重但最低保留 1；成功探测保持权重。租约到期优先于探测成功，健康状态记为 `EXPIRED`。

## 导入冲突

比较顺序固定为：

1. `lease_version` 高者覆盖。
2. 相同则 `last_registered_at_ms` 新者覆盖。
3. 相同则 `weight_permille` 高者覆盖。
4. 完全相同为跳过。
5. 前三项全同但端点、TTL、探测方式或参数不同，无法确定优先级，整批拒绝。

裁决先在内存工作副本中完成；任何一条为拒绝，事务只写批次拒绝报告，不写实例。批次 ID 与内容哈希绑定；重复提交返回保存的报告，内容变化返回 `E_CONFLICTING_BATCH`。

## 恢复语义

启动恢复在一个事务中：

- 所有 `STARTED` 意图改为 `ABORTED`。
- `PROBING` 轮次改为 `ABORTED`。
- `PENDING/RUNNING` 探测尝试改为 `ROLLED_BACK`，结果标记 `PENDING_FOR_RETRY`。
- `STAGING` 导入批次改为 `ABORTED`，报告 `atomic_rollback=true`。
- 写入 `recovery_reports`，只包含数据库已经能证明的状态。

恢复不推断“本来可能发生”的探测结果，也不修改已提交实例记录。
