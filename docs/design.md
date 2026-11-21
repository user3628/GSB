# 检查点与 WAL 回放组件 — 技术设计

## 1. 总体结构

```
wckpt/
  config.py        阈值与批量参数配置
  errors.py        业务错误码（E_WAL_CORRUPT / E_CHECKPOINT_INVALID / ...）
  models.py        Page / WalRecord / Checkpoint 数据模型与校验和
  util.py          规范 JSON、CRC32/SHA256、目录 fsync、读写锁 RWLock
  wal.py           段文件 WAL：追加、轮转、校验、范围检索、截断归档
  sqlite_store.py  页表 / 检查点 / 意图 / 请求账本的 SQLite 持久化
  replay.py        自适应回放（精确 / 批量）、可中断续算、numpy 校验
  recovery.py      强制终止后基于持久化事实的恢复
  engine.py        单写者多读者引擎、增量检查点、后台刷页、幂等请求
```

所有状态落在工作区根目录：

- `wal/seg_XXXXXXXXXX.wal`：WAL 段文件；
- `archive/`：截断后归档的段；
- `meta.db`：SQLite，含 `pages / checkpoints / intents / request_ledger / meta`。

## 2. 页与 WAL 记录模型

- 页：`page_no` 主键，保存 `digest`（内容摘要）、`version`（页版本）、`last_lsn`。
- WAL 记录字段：`lsn, page_no, before_digest, after_digest, txn_id, ts, crc`。
  - `crc` 是对除 `crc` 外字段规范 JSON 的 CRC32；
  - 同一页每次成功修改 `version += 1`，跨页互不约束；
  - 新页首条记录 `before_digest = null`，版本从 1 开始。
- 段文件三行结构：段头 `WCKPT-WAL-SEG/1`（含 `seg_id, start_lsn, crc`）、
  若干 `REC` 记录、可选 `END` 页脚（含 `end_lsn, record_count, crc`）。
- 解析任一行失败、CRC 不符、LSN 缺口、段头 `start_lsn` 与首条记录不一致，
  整段标记 `corrupt=true`，遍历时抛出 `E_WAL_CORRUPT`，该段不参与回放。
- 仅含合法段头、零记录的段表示“轮转后、首条记录落盘前被强杀”，
  标记为 `empty` 并在扫描时安全删除，不占用任何 LSN。

## 3. 追加与段轮转

- LSN 由引擎单调分配，跨段连续；追加前断言 `record.lsn == next_lsn`，
  从根本上杜绝两条记录共用 LSN。
- 当前段大小达到 `segment_max_bytes` 且至少含一条记录时：写 `END` 页脚、
  fsync、关闭，再以原子 `open + 段头 + fsync + 目录 fsync` 建立新段。
- 段头 `start_lsn` 即该段第一条记录的 LSN，并在重新扫描时强校验。
- `range_query(from_lsn, to_lsn)` 返回记录及其所属段文件名与 `seg_id`。

## 4. 检查点与整体版本替换

- 检查点字段：`seq`（单调递增）、`start_lsn`（回放起点 = 下一个 LSN）、
  完整页版本集合、SHA256 `checksum`。
- 提交顺序：先在同一 SQLite 事务写 `CHECKPOINT` 意图与 `OPEN` 检查点行并提交，
  再刷页，最后新事务把状态置 `COMMITTED`。
- 加载时重算 SHA256，失败或序号回退返回 `E_CHECKPOINT_INVALID`，旧状态保持可用。
- 加载最新已提交检查点后用 `reset_pages` 整体替换页表，绝不混合新旧页集合。
- 增量检查点只刷自上次检查点以来的脏页，但检查点载荷仍包含完整页集合，
  因此整体替换语义不变。

## 5. WAL 回放与自适应路径

- 从检查点 `start_lsn` 起按 LSN 升序重放，先把页表重建为检查点快照。
- 逐条校验：LSN 连续性、`before_digest` 与当前页摘要一致；
  不一致抛 `E_REPLAY_MISMATCH`，details 含 `expected_lsn / actual_lsn`。
- numpy 对全部 LSN 构建期望矩阵并与实际序列向量化比对。
- 路径选择（`Replayer.decide`）：
  - 记录数 < `replay_record_threshold` 且脏页数 < `replay_dirty_page_threshold`
    走 `precise`：逐条重放、逐条落盘；
  - 否则走 `batched`：按 `replay_batch_size` 分批、按 `flush_group_size` 分组刷页。
- 两条路径共享同一套顺序校验，且内存页视图始终实时更新，
  分组只影响落盘粒度，因此对同一 WAL 前缀产生逐页一致的摘要与版本。
- 幂等：重复回放都从检查点快照重建，最终结果不变。
- 可中断：`interrupt_after` 在应用 N 条后返回 `resume_from_lsn`；
  续算时先重建已应用前缀的内存视图，再从断点继续。

## 6. 增量检查点与 WAL 截断

- 截断安全线 = 当前已提交检查点的 `start_lsn`。
- 段 `end_lsn < safe_lsn` 才可归档；只要存在仍被回放依赖的段，
  即返回 `E_TRUNCATE_UNSAFE`，details 给出每段未覆盖 LSN 区间。
- 安全截断通过 `os.replace` 原子移动到 `archive/`，并 fsync 两个目录。

## 7. 单写者 / 多读者 / 幂等

- `RWLock`：多个读者可并发持有读锁；写者互斥且不饥饿。
- WAL 追加、检查点、截断都在写锁内串行；查询在读锁内只看到完整提交版本，
  页表通过 SQLite 事务整体替换，避免“检查点已更新、页仍旧值”的中间态。
- 幂等请求：进程内 `_RequestGate` 让同 `request_id` 的首个调用执行、
  其余阻塞等待并复用结果；SQLite `request_ledger` 额外持久化结果，
  重启后重复请求直接复用，保证同一请求只生效一次。

## 8. 意图持久化与强杀恢复

- 所有写操作先在 SQLite 事务写意图（`WAL_APPEND / CHECKPOINT / TRUNCATE / REPLAY`）再提交。
- 重启时 `RecoveryManager.recover()`：
  1. 重新扫描 WAL，以磁盘 LSN 高水位为唯一事实，对账 `next_lsn`；
  2. 扫描所有 `OPEN` 意图：
     - `WAL_APPEND`：目标 LSN 已持久化则标记 COMMITTED，否则 ABORTED；
     - `CHECKPOINT`：未 COMMITTED 则把页表与检查点指针回滚到最后一个完整检查点，
       无完整检查点则清空页表， dangling 行置 ABORTED；
     - `REPLAY / TRUNCATE`：标记 ABORTED，REPLAY 额外恢复到最后检查点；
  3. 输出仅基于持久化事实的恢复报告。
- 强杀通过子进程 `os._exit(77)` 模拟，不保证生成优雅退出报告。

## 9. 局限与取舍

- 单进程单写者；多读者为同进程多线程，跨进程读需另行加锁。
- 页内容以摘要表示，不保存原始字节，符合“内容摘要”建模。
- 归档段不再参与在线回放，需要更长保留期时由上层另行管理归档目录。
