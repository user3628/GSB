# 实现与一致性说明

## 线性化边界

引擎把以下动作放在同一个 `threading.Condition(threading.RLock())` 临界区中：

- 事务开启计数与身份登记。
- 行写权授予、等待登记、死锁检测与牺牲。
- 行版本写入与旧版本删除标记。
- 保存点创建与回滚。
- 提交序号分配、提交结果落盘、锁释放。

普通读取不进入该互斥区。它们通过 SQLite WAL 的只读事务和固定的提交序号过滤，因此多个页面读取可以并发。提交通过 SQLite 单写事务完成，读者只会看到提交前或提交后的完整状态。

## 可见性算法

对一行按 `row_version DESC, version_id DESC` 扫描版本：

```text
created_visible =
    created_txn == own_txn
    or (created_txn is committed and created_commit_seq <= snapshot_seq)

deleted_visible =
    deleted_txn == own_txn
    or (deleted_txn is committed and deleted_commit_seq <= snapshot_seq)

visible = created_visible and not deleted_visible
```

`READ_COMMITTED` 每条语句生成新的 `snapshot_seq = current_commit_seq`；`REPEATABLE_READ` 与 `SNAPSHOT` 在首次读取时把当前序号持久化到事务行，后续读取复用。自己的未提交版本通过 `created_txn == own_txn` 进入可见集合。

## 编辑保存的检查顺序

1. 事务必须活动，编辑会话必须属于该事务。
2. 检查服务端时间是否超过 `opened_at + edit_budget_ms`。
3. 请求 `row_version` 必须等于会话打开时记录的版本。
4. 取得 `(table,row)` 排他写权；RC 直接冲突，RR/SNAPSHOT 有界等待。
5. 锁等待结束后再次检查预算，防止长等待绕过期限。
6. 以当前完整提交快照读取“最新可编辑版本”，必须仍是会话的 `version_id`。
7. 在 SQLite 单写事务中插入新版本、标记旧版本删除、写 undo 日志并更新会话。

若 3 或 6 失败，返回 `E_EDIT_STALE`，不会执行覆盖。若新取得的锁因失败而不再需要，会被释放并唤醒后继等待者。

## 保存点逆操作

每次行写入记录 `txn_write_ops`：旧版本、旧版本写入前的 `deleted_txn`、新版本。保存点记录当前最大 `op_seq`、最大 `intent_id`、会话全量 JSON 和持锁集合。

回滚时逆序执行 undo：删除本事务在保存点后创建的新版本，恢复旧版本的 `deleted_txn`。之后重建编辑会话快照，释放保存点后新增的锁。目标保存点与更早保存点保留，之后保存点删除。可见性快照不被修改。

## 等待状态机

等待者是 `Waiter(status, enqueued_mono, deadline_mono, holder_txn)`：

```text
waiting --grant--> acquired
waiting --deadline--> timeout
waiting --rollback/cancel--> canceled
waiting --cycle victim--> victim
```

释放锁时只从队首授予仍在活动且未过期的等待者。死锁图边为 `waiter_txn -> holder_txn`；检测到环后按环内最大 `txn_seq` 选择牺牲者。若牺牲者不是当前请求者，牺牲者的锁释放可能立即把锁授予当前请求者。

## 崩溃恢复的不变量

- 已提交事务表是提交可见性的唯一提交事实。
- 行版本的 `commit_seq` 只在对应事务提交时写入；写入和事务提交同属一个 SQLite 事务。
- 活动事务可能留下未提交版本、删除标记、会话、开放意图与开放锁意图，这些都可由恢复清理。
- 恢复报告不引用内存状态，只汇总数据库扫描结果。

