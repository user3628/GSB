# 设计与一致性说明

## 1. 模块划分

| 模块 | 职责 |
| --- | --- |
| `seqtxn/db.py` | SQLite 模式、WAL/FULL 同步、短连接读写事务 |
| `seqtxn/controller.py` | 分区顺序、租约、待决表、事务裁决、意图与恢复 |
| `seqtxn/api.py` | 组件 HTTP API 与业务码映射 |
| `seqtxn/coordinator.py` | 可配置的本地事务协调测试端点 |
| `seqtxn/coordinator_client.py` | 独立超时 HTTP 查询与错误分类 |
| `scripts/run_experiment.py` | 固定样本、故障注入、强制终止与结果归一化 |

## 2. 持久化模型

- `topics`：主题、分区数、`hash|explicit` 路由模式与固定盐。
- `partitions`：每分区最后序号、最终结论水位、投递头、可投递序号、已确认水位、租约代数。
- `messages`：主题、分区、生产序号、生产者/客户端序号、事务标识、体摘要、生产时刻、状态、确认信息。
- `pending_messages`：事务消息的待决表；裁决与消息状态更新在同一事务中删除这些行。
- `transactions` / `txn_queries` / `coordinator_cache` / `txn_conflicts`：首次结论、查询轨迹、幂等结果与冲突报告。
- `leases`：租约历史；`(topic, partition_no) WHERE state='ACTIVE'` 部分唯一索引保证单领取者。
- `intents`：写操作意图。它先于业务效果提交，效果与 `COMMITTED` 标记尽量放在同一事务。
- `audit_events` / `fault_events` / `recovery_runs` / `recovery_events`：可重放观测与恢复事实。

数据库使用 WAL。多个只读连接可并发；所有写事务使用 `BEGIN IMMEDIATE`，在 SQLite 边界串行化。组件内另有分区锁和事务锁，减少同进程争用。

## 3. 分区顺序与水位

每个分区包含四个不同概念：

- `last_sequence`：最近分配的生产序号。
- `visibility_sequence`：已取得 `COMMITTED` 或 `ROLLED_BACK` 最终结论的最大序号。
- `head_sequence`：最小“未确认且未回滚”序号，可能指向待决消息。
- `delivery_sequence`：只有 `head_sequence` 已提交时才非空，是实际可发租约的序号。

严格投递规则：

1. 生产序号只在真正写入消息的同一事务内递增；业务拒绝发生在递增前，所以无缺口。
2. 领取先找 `state != ROLLED_BACK AND acked_at IS NULL` 的最小序号。
3. 该序号为 `PENDING`：返回 `HEAD_PENDING_TRANSACTION`，不创建租约。
4. 该序号为 `COMMITTED`：创建唯一活动租约并返回消息。
5. 回滚序号被跳过，但其生产序号仍在库中保留。

这使“之后已提交消息的事务结论”可以被独立持久化，同时不会越过前面的待决头投递。`visibility_sequence` 可前进到较后的已决序号，而真实投递仍由 `head_sequence` 控制。

## 4. 租约与确认

- 领取请求在分区锁内执行 `BEGIN IMMEDIATE`，先清理已到期 `ACTIVE` 租约，再检查活动租约和投递头。
- 插入新租约时由部分唯一索引兜底；跨进程并发领取也只有一个能成功。
- 租约记录 `attempt`。超时后的下一次领取会复用原序号并增加 attempt，不跳过后继消息。
- 确认必须使用当前活动租约令牌；先确认已确认消息或旧令牌返回 `E_DUPLICATE_ACK` 或租约错误码。
- 迟到确认会先把到期租约落为 `EXPIRED`，再返回 `E_LEASE_EXPIRED`，不会留下表面仍活动的租约。

## 5. 事务协议

生产事务消息：

1. 校验路由与事务是否已终结。
2. 在同一写事务中创建 `PENDING` 事务、分配序号、插入 `messages(PENDING)` 和 `pending_messages`。
3. 更新分区水位；此时消息不可投递。

裁决：

1. 若已有最终结论且 `force=false`，直接返回已存结论并标记 replay。
2. 否则先提交 `TXN_RESOLVE` 意图，再使用固定幂等键 `txn:<id>` 查询协调器。
3. 查询使用独立 `httpx.Client(timeout=...)`；超时退出上下文即关闭套接字。
4. 超时、连接失败、未知结论均不写最终结论，事务保持 `PENDING`。
5. 得到 `COMMIT|ROLLBACK` 后，在一个 SQLite 写事务中更新事务行、全部事务消息、删除待决行、插入幂等缓存与查询轨迹、刷新所有分区水位，并完成意图。
6. `force=true` 的重复结论只在测试 `duplicate_*` 注入下使用：相同结论返回 `E_DUPLICATE_ACK`，不同结论写 `txn_conflicts` 并返回 `E_TXN_CONFLICT`，首次结论不变。

## 6. 强制终止与恢复

写路径先使用独立短事务提交 `STARTED` 意图。关键窗口：

- 领取：意图提交后，租约效果先提交；随后才把意图标为 `COMMITTED`。若进程在两步之间退出，会留下 `ACTIVE` 租约和 `STARTED` 意图。
- 事务：意图提交后，协调器可能已经应答；若进程在最终 SQLite 事务前退出，会留下 `STARTED` 意图与 `PENDING` 事务。
- 发布、确认、释放：业务效果与意图完成标记在同一写事务中，故重启后要么全部可见，要么全部不可见。

启动恢复在 `BEGIN IMMEDIATE` 中扫描 `STARTED` 意图：

- 未完成领取：把关联活动租约改为 `RECOVERED`，刷新分区，使其按原序号重投。
- 未完成裁决：事务保持 `PENDING`，成为重新协商候选。
- 未完成发布/确认/释放：不应用未提交效果。

候选事务随后使用同一幂等键重新查询协调器。协调器对幂等键返回首次缓存结果，所以“应答后、落库前终止”可得到一致结论。恢复事件只从 SQLite 行重建；报告明确设置
`complete_exit_report_possible_after_kill=false`，不伪造被终止进程的退出报告。

## 7. 故障注入与业务码

| 注入 `injection` | 实现 | 主要业务码 |
| --- | --- | --- |
| `commit` | 协调器单次返回 COMMIT | `OK`，事务至 `COMMITTED` |
| `rollback` | 协调器单次返回 ROLLBACK | `OK`，事务至 `ROLLED_BACK` |
| `refuse` | 对已关闭的本机回环端口发起真实 HTTP 连接 | `E_TXN_UNREACHABLE` |
| `hang` | 协调器等待并观察客户端断开 | `E_TXN_TIMEOUT`，保持待决 |
| `unknown` | 协调器返回不可判定结论 | `E_TXN_UNKNOWN`，保持待决 |
| `duplicate_same` | 终局事务强制再次查询且结论相同 | `E_DUPLICATE_ACK`，首次结论不变 |
| `duplicate_conflict` | 终局事务强制再次查询且结论相反 | `E_TXN_CONFLICT`，写冲突报告，首次结论不变 |

每次注入把事务当前状态、幂等键、`force` 与观测码写入 `fault_events`。挂起注入还记录协调器是否观察到 `client_disconnected=true`。

其他关键码：`E_PARTITION_INVALID`、`E_LEASE_HELD`、`E_LEASE_EXPIRED`、`E_TXN_FINALIZED`、`E_IDEMPOTENCY_CONFLICT`。

## 8. 已知边界

- 这是本地单机测试组件，不是分布式共识系统；SQLite 是唯一事实源。
- 严格顺序以“可投递头”为准：待决头阻塞后继投递，但后继消息可独立记录自己的已决状态。
- 协调器脚本是测试替身，事务范围由同一 `txn_id` 下的消息集合定义。
- 恢复报告依赖 SQLite 已提交事实；进程被终止时尚未落库的信息不会被恢复报告臆测。

