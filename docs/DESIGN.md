# 一致性设计

## 1. 持久化模型

SQLite 表按职责划分：

- `topics` / `partitions`：主题声明、分区生产序号下界与下一个序号。
- `events`：事件主体、内容指纹、状态、批次、度量和 payload。
- `event_state_trace`：每次状态迁移的来源、目标、原因和投递引用。
- `deliveries`：每次投递尝试、租约令牌、过期时刻与结束原因。部分唯一索引保证每分区最多一个 ACTIVE 租约。
- `confirmations`：确认历史。ACTIVE 状态按事件唯一，回拨后改为 ROLLED_BACK。
- `consumer_offsets`：每组每分区下一个序号，以及重放目标/边界。
- `offset_history`：位点推进、回拨、恢复回退的连续历史链。
- `aggregate_state`：每个事件键的计数、Neumaier 主和与补偿项。
- `intents`：两阶段操作的持久化意图。
- `recovery_reports`：重启后仅基于持久化事实生成的恢复报告。

数据库启用 WAL、`foreign_keys=ON`、`synchronous=FULL` 和 busy timeout。初始化建立普通索引及两个关键部分唯一索引：

```sql
CREATE UNIQUE INDEX uq_active_delivery_partition
ON deliveries(topic_id, group_id, partition) WHERE status = 'ACTIVE';

CREATE UNIQUE INDEX uq_active_confirmation_event
ON confirmations(event_id) WHERE status = 'ACTIVE';
```

## 2. 生产序号与幂等

发布先在单事务内解析整批、比较批内重复事件，然后读取主题与分区。每个新事件在所属分区读取 `next_seq` 并更新，插入事件与初始轨迹。任何冲突或错误都回滚整个事务，因此：

- 同分区序号严格连续；
- 跨分区事务失败不会留下部分批次；
- 事件 ID 首次内容与首次序号保持不变。

幂等指纹不包含 `batch_id` 和 `production_seq`。这两者是接入信封而非事件内容。指纹中的事件时刻先规范化为 UTC 微秒，度量先规范化为十进制文本，保证等价 JSON 数字和时区表示可命中同一事件。

## 3. 两阶段意图

所有关键写路径采用如下顺序：

```text
T1: BEGIN IMMEDIATE; INSERT intent(status=PENDING); COMMIT
        |
        | 进程可在任意点被 kill
        v
T2: BEGIN IMMEDIATE;
    revalidate precondition;
    apply event/delivery/confirmation/offset/aggregate changes;
    UPDATE intent SET status=COMMITTED;
    COMMIT
```

若在 T1 后、T2 前终止，数据库只保留 PENDING 意图，业务状态未变。若在 T2 内终止，SQLite 回滚 T2，仍只保留 PENDING 意图。若 T2 已提交，意图与业务状态在同一提交版本中均为完成态。

## 4. 确认事务

一次 ACK 的 T2 原子执行：

1. 重读事件、当前分区租约、租约令牌、消费者和位点。
2. 检查租约未过期，事件必须为 DISPATCHED，位点必须正好等于事件生产序号。
3. 事件迁移 `DISPATCHED -> ACKED` 并写轨迹。
4. 投递改为 ACKED；插入 ACTIVE 确认。
5. 位点推进到 `seq+1` 并写入 `offset_history`。
6. 从该键的全部 ACTIVE 确认按 `(partition, production_seq)` 重算聚合。
7. 意图改为 COMMITTED。

步骤 3–7 与意图完成标记处于同一 SQLite 提交。任何读事务在提交前只能看到旧的完整版本，在提交后只能看到新的完整版本，因此不会观察到 ACKED 但聚合仍旧、或位点已推进但确认缺失。

重复 ACK 在 T2 中发现事件已 ACKED 且有 ACTIVE 确认时直接完成意图并返回 `idempotent=true`，不改变状态、轨迹、位点或聚合。

## 5. 租约与重投

- 拉取事务先查找同组同分区 ACTIVE 投递。
- 若租约未过期，返回 `E_LEASE_HELD`。
- 若已过期，投递标记 EXPIRED，DISPATCHED 事件标记 REDELIVERED，然后重新读取位点处最小事件。
- 重投时事件 `REDELIVERED -> DISPATCHED`，生成新租约和更大 attempt 编号。

因为位点只在 ACK 时推进，且拉取总是从位点处最小事件开始，慢消费者的未确认事件不能被后续消费者超过。

## 6. 回拨事务

回拨在单个 T2 中执行：

1. 验证目标属于当前保留序号且不是向前跳跃。
2. 释放该分区 ACTIVE 租约。
3. 将目标及之后 ACTIVE 确认标为 ROLLED_BACK。
4. 将目标及之后 ACKED/DISPATCHED 事件迁回 REDELIVERED。
5. 位点设为目标，记录 `replay_end = old_offset`（若已在重放则保留更大边界）。
6. 从剩余 ACTIVE 确认重建全部聚合。
7. 记录 ROLLBACK 位点历史并提交意图。

重放窗口内拉取只考虑 `seq < replay_end`。当确认把位点推进到边界，重放标记在同一 ACK 事务中清除，下一事件才可投递。因此重放不会与回拨后新到达的事件竞争分区顺序。

## 7. 聚合确定性

对每个键，聚合器按规范顺序读取所有 ACTIVE 确认：

```sql
ORDER BY event_key, partition, production_seq
```

Neumaier 算法同时维护 `total` 与 `compensation`：

```text
t = total + x
if |total| >= |x|: compensation += (total - t) + x
else:              compensation += (x - t) + total
total = t
value = total + compensation
```

ACK 时只重算受影响键，回拨时重建全部键。规范顺序使“同一事件集合无论分批确认还是一次确认”得到相同主和与补偿项，同时降低大数淹没小数带来的累计误差。

## 8. 恢复算法

服务启动先持有写者锁，然后在一个写事务内执行：

1. 读取 PENDING 意图并全部标为 ABORTED，记录 ID、类型、scope。
2. 释放所有 ACTIVE 投递；对应 DISPATCHED 事件迁移到 REDELIVERED。
3. 对每个主题/组/分区，从最早保留序号开始扫描持久化事件：
   - ACKED 且有 ACTIVE 确认：完整提交；
   - DEAD_LETTERED：显式终止，也算完整边界；
   - 其他状态：边界停止。
4. 比较持久化位点和扫描边界，取保守值。若位点高于边界，回退；若位点低于边界但存在更高 ACKED 事实，则建立重放窗口。
5. 将边界之后仍 ACTIVE 的确认改成 ROLLED_BACK，将 ACKED 事件迁回 REDELIVERED。
6. 从 ACTIVE 确认重建聚合。
7. 写入报告并提交。

报告中 `source` 固定为 `persisted_sqlite_facts_only`。强制终止不会产生退出报告；恢复报告只说明重启时看到了哪些持久化事实。

## 9. 验证器的不变量

`StateVerifier` 在一个只读快照内检查：

- 每分区事件序号从保留下界开始连续，`partitions.next_seq` 等于最后序号加一；
- 每个事件轨迹从 `NULL -> ACCEPTED` 开始、状态链连续、迁移在合法矩阵内、最终轨迹等于当前状态；
- 所有 ACTIVE 确认对应 ACKED 事件，所有 ACKED 事件恰有一个 ACTIVE 确认；
- 位点前事件只能是 ACKED+确认或显式死信，ACTIVE 确认序号必须小于位点；
- 位点历史链连续，ADVANCE 不后退，ROLLBACK 不前进，尾项等于当前位点；
- 聚合键集合、计数、主和和补偿项可由 ACTIVE 确认逐项精确重建，输出按键稳定排序；
- 每分区最多一个 ACTIVE 租约，租约事件为 DISPATCHED 且不早于位点；
- 没有 PENDING 意图，事件状态均属于五个声明状态。

验证器只读数据库，不自动修复；修复只发生在显式启动恢复流程。
