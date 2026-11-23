# 技术设计

## 1. 存储与并发模型

SQLite 使用 WAL、`synchronous=FULL`、外键与 10 秒 busy timeout。Go 进程内所有写操作由 `Service.writeMu` 串行化；每个业务写使用一个 SQLite 事务。读接口使用 `ReadOnly` 事务，使多条 SELECT 位于同一快照。

一次业务写通常包含：

1. 独立事务写入 `requests(RUNNING)` 和 `intents(PREPARED)`；
2. 可选外部协调 HTTP 查询（不持有数据库写锁）；
3. 最终事务：领域状态 + `intents(COMMITTED)` + `requests(COMMITTED,response)`。

因此进程崩溃不会产生“消息已可见但前沿未推进”或“租约已释放但位点已提交”的中间版本。幂等请求的状态行也在同一最终事务中关闭。

## 2. 分区状态

`partitions` 保存权威状态，`consumer_offsets` 保存同一事务内同步的消费位点/租约视图：

```text
next_seq            下一个生产序号
visible_frontier    已确定事务可见性的排他序号
consume_cursor      下一次拉取检查的序号
earliest_retained   回退允许的最早序号
lease_state         FREE | LEASED | EXPIRED
lease_owner/version/expires_at
lease_cursor        当前租约检查起点
lease_last_delivered_seq
replay_active/from/limit
```

生产在分区行锁/事务内读取 `next_seq` 并递增。唯一约束 `(topic,partition_no,seq)` 与事务回滚共同保证无缺口。消息 ID 主键保证重复写入返回既有序号；内容摘要、主题、分区键、事务标记与事务 ID 不一致返回冲突。

## 3. 消息与租约状态机

消息合法迁移：

```text
PENDING_INVISIBLE -> VISIBLE | ROLLED_BACK
VISIBLE -> CONSUMED | DEAD_LETTERED
CONSUMED -> VISIBLE          (仅显式重放准备)
ROLLED_BACK, DEAD_LETTERED   终态
```

租约合法迁移：

```text
FREE -> LEASED
LEASED -> FREE | EXPIRED
EXPIRED -> LEASED
```

统一迁移函数先验证源状态，再执行带 `WHERE status=?` 的更新，最后写 `state_transitions`。验证失败不会执行领域写。

## 4. 有序拉取、提交、超时与回退

拉取步骤：

1. 事务内检查并真实过期空闲租约；
2. 版本校验；同所有者可续租，其他所有者立即 `E_LEASE_HELD`；
3. 从 `lease_cursor` 开始读取，遇到不可见事务即停止；
4. 跳过回滚/死信等终态；返回最小连续可消费序号；
5. 不预留消息，提交前消息保持 `VISIBLE`，因此崩溃不会吞消息。

提交步骤：

1. 校验所有者、租约版本和已交付范围；
2. 预检查整个 `[lease_cursor, through_seq]`，任何不可见事务都会在修改前返回；
3. `VISIBLE -> CONSUMED`，推进 `consume_cursor`；
4. 释放租约并递增版本。

重放：

- 目标必须 `< visible_frontier`、`< consume_cursor` 且不早于保留位点；
- 准备事务把区间内 `CONSUMED` 恢复为 `VISIBLE`，设置 `replay_from` 与原位点 `replay_limit`；
- 拉取上界为 `replay_limit`，新到消息仅追加到 `next_seq`，不推进可见前沿；
- 消费追平重放上限后清除重放状态，重新计算可见前沿并允许新消息进入；
- 整个重放期间 `visible_frontier` 不回退，已消费消息不会被重复计入。

## 5. 事务协调

协调器测试端点接受五种注入：`commit`、`rollback`、`refuse`、`hang`、`conflict`。客户端查询使用 `context.WithTimeout`、独立 HTTP 请求和禁用 keep-alive 的 transport；超时会取消请求，关闭连接并映射 `E_TXN_TIMEOUT`。连接被 hijack 后关闭映射 `E_COORD_REFUSED`。

每个查询及注入参数写 `injection_observations`。客户端侧还记录统一后的裁决/业务码，便于逐项复核。

裁决最终事务中：

```text
COMMIT:
  message PENDING_INVISIBLE -> VISIBLE
  txn PENDING -> COMMITTED
  recompute visible_frontier
ROLLBACK:
  message PENDING_INVISIBLE -> ROLLED_BACK
  txn PENDING -> ROLLED_BACK
  recompute visible_frontier (回滚终态可跨过)
```

已裁决事务直接复用结论，不重复网络查询。`force_recheck` 用于演示矛盾结论：若校验失败，返回 `E_TXN_CONFLICT` 并保留原结论。

## 6. 调度器

调度轮次首先按分区分组。优先级高的分区先形成调度带；同优先级分区按轮转偏移轮询，组内按入队序。调度循环在全局并发额度内启动 goroutine，租约任务额外使用每分区在途计数。

时间预算只控制“何时不再派发”：

- 已启动任务继续完成，不因预算取消；
- 预算不足时剩余任务不修改状态，仍在有界队列；
- 轮次 JSON 轨迹记录每个已完成/跳过任务的优先级、分区、耗时、预算前后值和跳过依据；
- 轮次轨迹持久化到 `scheduler_rounds` 并显示在诊断页。

## 7. 恢复

最终状态和意图完成在同一事务中，因此 `PREPARED` 意图只可能对应尚未提交的领域写。启动恢复扫描 `PREPARED`：

- 读取当前分区/事务/消息事实；
- 如果重放异常活跃则中止并恢复 `replay_limit`；
- 标记意图 `RECOVERED`，记录动作与证据；
- 将仍在 `RUNNING` 的请求标记 `FAILED/E_INTENT_RECOVERED`；
- 写入 `recovery_reports`，诊断页展示同一报告。

演示端点 `/api/debug/crash-after-intent` 会在意图持久化后直接 `os.Exit(75)`，不执行最终事务，从而可重复验证。
