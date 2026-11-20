# 投递语义、并发与强制终止恢复

## 1. SQLite 事务边界

- 每个请求使用独立连接，启用 WAL、外键、15 秒 busy timeout 和 `synchronous=FULL`。
- 所有写操作使用 `BEGIN IMMEDIATE`，从数据库层串行化写事务。
- 查询快照使用一致读事务；读取方要么看到领取前的 `(pending, delivery_count=n)`，要么看到领取后的 `(leased, delivery_count=n+1)`，不会看到中间值。
- `queue_snapshots` 在每次队列状态变更中写入版本号、状态计数和消息摘要，作为持久化版本快照。

## 2. 两阶段意图协议

入队、领取、确认和导入都遵循：

1. **意图提交**：在独立事务中插入 `intents.state='pending'`。这一步先于任何业务效果落盘。
2. **效果事务**：在同一个 `BEGIN IMMEDIATE` 事务中完成业务变更，并把意图改为 `committed`。业务效果与“已完成”证据原子提交。

因此重启时：

- 看到 `pending` 意图：效果事务未提交，SQLite 已丢弃其变更；恢复器将意图标记为 `rolled_back`。
- 看到 `committed` 意图：业务效果与完成证据已经同时持久化。
- 不可能把“业务效果已提交但意图仍 pending”当作正常状态；恢复器仍保留防御性修复逻辑，用于清理任何与 pending 意图关联的残留效果行。

强杀测试使用 `os._exit(97)` 在效果事务中途终止进程，不复用 Python 异常清理或退出钩子。

## 3. 恢复报告

启动时 `recover_pending_intents` 在单个事务中扫描 pending 意图、修复防御性残留、标记意图并持久化报告。报告字段：

```json
{
  "status": "recovered",
  "intents_scanned": 1,
  "rolled_back_claims": 1,
  "rolled_back_acks": 0,
  "evidence": {
    "source": "sqlite_committed_rows_only",
    "runtime_memory_used": false,
    "uncommitted_transaction_effects_visible": false,
    "crash_exit_report_available": false
  }
}
```

报告不会声称进程在强杀时生成了完整退出报告。其结论只来自已提交的意图行、业务行与 SQLite 事务事实。

## 4. 领取

领取事务完成以下步骤：

1. 选择最早产生且仍可投递的候选：`pending` 或租约已过期的 `leased`。
2. 在同一事务内递增 `delivery_count`，写入租约所有者、随机租约令牌和到期时间。
3. 插入 `deliveries` 投递记录。
4. 更新队列版本并写入版本快照。
5. 标记意图完成并提交。

并发领取依靠 `BEGIN IMMEDIATE` 与单条候选更新，8 路并发实测只有 1 个请求拿到消息，其余得到空结果。消息正文、租约与投递次数在提交后一起可见。

## 5. 至少一次

- 租约到期后，未确认消息再次成为候选。
- 每次重投都插入新的投递记录并递增 `delivery_count`。
- 旧租约令牌在重投后失效，旧消费者确认返回 `E_LEASE_MISMATCH`。

## 6. 恰好一次

- 确认表主键为 `(message_id, consumer_id)`。
- 第一次成功确认写入确认行、将消息置为 `acked` 并清空租约。
- 同一消费者再次确认命中幂等键，返回 `E_DUPLICATE_ACK`，不删除确认、不改变待投递状态。
- 另一消费者确认已确认消息返回 `E_ALREADY_ACKED`。
- 批量确认逐条独立提交；一条失败不撤销已成功的条目。响应含每条索引、成功标记、业务码与汇总计数。

## 7. 语义迁移

队列创建时声明 `at_least_once` 或 `exactly_once`。导入的队列元数据若与现有模式不同，整批拒绝并返回 `E_QUEUE_MODE_MISMATCH`。运行时唯一可改变模式的入口是：

```http
POST /v1/queues/{queue}/migrate-semantics
```

若存在未过期的租约，迁移返回 `E_QUEUE_BUSY`。迁移会增加队列版本并写入版本快照。

## 8. 导入冲突与原子性

冲突裁决顺序：

1. 投递状态优先级：`acked(4) > leased(3) > pending(2) > expired(1)`。
2. 更高级记录版本。
3. 更晚最后写入时间。
4. 三者完全相同：载荷相同则跳过；载荷不同则拒绝。

导入事务开始前已完成格式、版本、容量、时间与批次内重复 ID 检查。事务内任一条拒绝都会抛错并回滚，包括先前已经执行的插入、队列自动创建、覆盖与版本快照。拒绝响应包含 `rejected_index`、业务码与 `state_changed=false`，拒绝报告在事务外单独持久化用于审计。

成功导入的覆盖、跳过、插入数量与每个冲突的三元组比较证据写入 `import_reports` 和响应。

