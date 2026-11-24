# 设计与持久化顺序

## 1. 组件边界

- `Engine`：统一入口，写路径由 `mu` 串行化；读路径通过 SQLite 只读事务获得一致快照。
- 段文件：事实上的 redo 数据来源。SQLite 保存记录目录、页版本和生命周期元数据。
- 意图表：任何跨“段文件 + SQLite”或跨多步状态的生命周期操作，都在实际修改前写入。
- HTTP/CLI：`serve` 暴露完整本地 HTTP API；`demo` 默认在 `127.0.0.1:0` 启动临时 HTTP 服务，通过真实请求驱动整个确定性工作负载，再重开引擎输出恢复报告。

## 2. 段文件格式

所有整数为大端。

段头固定 128 字节：

| 偏移 | 大小 | 字段 |
|---:|---:|---|
| 0 | 8 | magic `WALSG001` |
| 8 | 4 | format version |
| 12 | 4 | header size |
| 16 | 8 | segment number |
| 24 | 8 | start LSN |
| 32 | 32 | previous segment SHA-256 |
| 64 | 8 | record count |
| 72 | 8 | transaction count |
| 80 | 8 | last LSN |
| 88 | 4 | flags |
| 92 | 32 | reserved |
| 124 | 4 | CRC32C of bytes 0..123 |

记录固定头 76 字节，后接事务 ID 和页数据：

| 偏移 | 大小 | 字段 |
|---:|---:|---|
| 0 | 4 | magic `WLR1` |
| 4 | 4 | total length |
| 8 | 8 | LSN |
| 16 | 8 | page number |
| 24 | 8 | page version |
| 32 | 32 | SHA-256 post-image digest |
| 64 | 4 | transaction ID length |
| 68 | 4 | data length |
| 72 | 4 | CRC32C（计算时该字段置零） |
| 76 | var | transaction ID |
| ... | var | page data |

`ParseSegment` 严格校验 magic、版本、头 CRC、记录 CRC、载荷摘要、段内连续 LSN、头中 `LastLSN` 与记录数。失败统一返回 `E_SEGMENT_CORRUPT`，并带调用方期望的段序号/起始 LSN。

## 3. LSN 规则

- 初始 `next_lsn=1`。
- 每条 WAL 记录恰好占用一个全局 LSN。
- 组提交提交成功后 `next_lsn = end_lsn + 1`。
- 新段头 `start_lsn` 等于该段第一条记录将使用的 LSN。
- 组提交失败时记录被截断，`next_lsn` 回到意图中的组起点；若回滚本身未完整落盘，意图保持 `PENDING`，引擎封锁后续操作并要求重启恢复。
- 重启校验所有幸存段的记录 LSN 连续；SQLite `next_lsn` 必须等于已持久化最大值加一。

## 4. 组提交顺序

```text
preflight fingerprints / versions / segment capacity
  -> SQLite: transaction=GROUPED + GROUP_COMMIT intent=PENDING
  -> segment: records + header counts (not yet durable)
  -> fsync(segment)
  -> SQLite atomic:
       all txns DURABLE
       txn_records + page_versions
       segments counts/digest/byte size
       next_lsn
       intent=DONE
  -> return offsets
```

注：`+ segment` 阶段允许被强杀。由于意图已经持久化且 SQLite 元数据未提交，重启会依据意图的 `old_size/old_counts/old_last_lsn/start_lsn` 截断文件并恢复段头，整组置为 `ABORTED`。

组大小必须适配一个段，这是“一个组共享一个 fsync 屏障”和“事务不拆段”的直接实现约束。单事务或整组过大时在写任何 WAL 字节之前返回 `E_ROTATE_FAILED`。

## 5. 原子轮转

```text
SQLite: ROTATE intent=PENDING
  -> create temp header + fsync + rename
  -> SQLite atomic:
       old OPEN -> SEALED
       insert new OPEN
       current_segment_no / next_segment_no
       intent=DONE
```

- 新段头保存旧段完整文件摘要。
- 文件名使用固定 16 位段号，临时文件不会暴露给目录。
- 创建、重命名或元数据提交失败时删除新文件；旧段保持 `OPEN`。
- 启动时移除未登记临时文件/孤儿文件。

## 6. 检查点

- `AdvanceCheckpoint` 从 `page_versions` 选择每页在 `replay_start_lsn` 下的最大版本。
- 页集合按页号规范化后计算 `pages_digest`，再与序号、回放起点计算整体摘要。
- 写入 `CHECKPOINT` 意图后，在单个 SQLite 事务中停用旧检查点、插入新检查点、页集合和 `intent=DONE`。
- 启动时 `PENDING` 检查点意图直接标记回滚；旧检查点元数据不变。
- `ReplayFromCheckpoint` 只从活动检查点回放起点之后开始，按 LSN 排序，要求页版本递增且 LSN 无缺口。

## 7. 回收与读取

- 回收先验证所有目标段，再写一条 `RECLAIM` 意图。
- 标记 `RECLAIMABLE` 与 `intent=MARKED` 在同一 SQLite 事务完成。
- 所有文件删除后，`RECLAIMED` 与 `intent=DONE` 再原子提交。
- 重启时 `MARKED` 意图续作物理删除；遗留 `RECLAIMABLE` 行也会完成到 `RECLAIMED`。
- `ReadPage` 在一个只读 SQLite 快照中同时读取检查点、检查点页集合、页最新版本，因此不会观察到“检查点已旧、段已可回收”的中间状态。
- 读取选择 `version` 最大且 `lsn <= requested_lsn` 的版本；活动检查点存在时，页全局最新版本必须等于检查点覆盖版本，才允许返回。

## 8. 恢复扫描

启动顺序：

1. 打开/初始化 SQLite schema；
2. 扫描 `PENDING`/`MARKED` 意图：
   - 组提交：恢复段大小与段头，事务回滚为 `ABORTED`；
   - 轮转：删除未登记新段；
   - 检查点：旧元数据不变；
   - 回收：未标记则放弃，已标记则续作删除；
3. 将所有仍为 `APPENDING`/`GROUPED` 的事务置为 `ABORTED`；
4. 清理临时/孤儿/已回收段文件；
5. 严格解析所有幸存段，校验目录计数、字节数、跨段连续 LSN、前段摘要链与 current 指针；
6. 修复被回滚的空当前段起点和 `next_lsn`；
7. 校验活动检查点摘要；
8. 写入 `recovery-report.json`。

## 9. 并发模型

- 单写者：`CommitGroup`、轮转、检查点、回收、显式封存均持 `Engine.mu`。
- 多读者：每次读取使用 SQLite read transaction；SQLite WAL 模式允许并发读。
- 索引/页版本只有组提交的 SQLite 元数据事务提交后才可见，因此不会出现“事务已 DURABLE、段头计数尚未更新”的可见状态。
- 检查点替换与 `RECLAIMABLE` 标记各自处于一致的 SQLite 事务边界。

## 10. 设计取舍

- 页载荷随 WAL 记录保存，摘要字段用于校验 post-image；查询从持久化版本表读取，不从可变内存状态读取。
- 不自动推进检查点；上层事务执行器显式选择安全回放起点。
- 无检查点时允许读取完整且未回收的 WAL 视图；一旦检查点存在，则严格执行检查点覆盖规则。
- 同一事务 ID 的 `DURABLE` 重提只返回首次落盘结果；内容不一致返回冲突。
