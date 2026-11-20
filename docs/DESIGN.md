# 设计与一致性说明

## 1. 状态模型

一个状态版本包含：

- `version`：单调递增提交版本。
- `watermark_ms`：已提交事件最大时间戳。
- `windows`：窗口聚合与状态机状态。
- `session_index`：分组到会话窗口键的索引。
- `next_session_id` / `next_late_seq`：确定性 ID 序列。
- `late_queue`：按事件-窗口分配记录的迟到项。
- `batch_registry`：批次内容哈希与可重放响应。
- `eviction_log`：最近淘汰记录。
- `last_intent_id` / `last_batch_id`：恢复时用于匹配已持久化意图。
- `session_tombstones`：被容量淘汰的已发射会话影响区间，用于继续确定性判定迟到事件；超过全局水位保护范围后自动裁剪。

查询使用发布后的状态对象；写者总是先深拷贝旧状态，在副本上完成整批变更，再原子写入快照并一次性替换引用。因此正常查询不会看到“事件数组已变、聚合未变”的中间态。

## 2. 单批次事务

1. 校验完整请求；任一事件非法则整批返回 `E_EVENT_INVALID`。
2. 规范化事件顺序并计算内容哈希。
3. 检查已提交同 ID 批次；相同哈希重放，不同哈希返回 `E_CONFLICTING_BATCH`。
4. 检查同 ID 正在执行批次；等待其完成并复用结果或错误。
5. SQLite 写入 `pending` 意图并提交事务。
6. 深拷贝当前状态，分配新版本。
7. 按时间戳稳定排序处理事件；迟到判断使用本批次开始前的 watermark。
8. 合并三类窗口；会话窗口可合并相邻 `OPEN` 会话，但若触及 `CLOSED/EMITTED` 会话影响区间则按迟到策略处理。
9. 推进 watermark，执行 `OPEN -> CLOSED -> EMITTED`。
10. 执行 soft limit 淘汰；若仍超过 hard limit，丢弃工作副本并返回 `E_CAPACITY_EXCEEDED`。
11. 构建批次注册表与响应。
12. 写临时快照、`fsync`、原子替换、目录 `fsync`。
13. 发布新内存状态。
14. 将 SQLite 意图标记为 `committed`；崩溃发生在该步之前也可由快照内容证明已提交。

失败时工作副本直接丢弃，SQLite 意图正常路径标记为 `aborted`；硬终止路径保持 `pending`，留给启动恢复扫描。

## 3. 快照格式与校验

快照是单个 JSON 对象，包含：

- `format = DWE_SNAPSHOT_V1`
- `version`
- `created_at`
- `config_fingerprint`
- `checksum_algorithm = sha256`
- `checksum`
- `intent_id` / `batch_id`
- `body`：完整状态

摘要输入是规范化 JSON：

```text
canonical_json({format, version, config_fingerprint, body})
```

`body.version` 必须等于外层 `version`。加载时检查文件名/版本、格式、配置指纹、摘要、状态结构和窗口记录字段。任何失败返回 `E_SNAPSHOT_INVALID`，不触碰当前可用状态。

## 4. 启动恢复

1. 扫描 `runtime/snapshots/`，按版本降序验证。
2. 选择第一个完整合法快照，整体反序列化。
3. 记录更高版本但损坏的快照及 `.tmp` 文件；新提交版本会高于所有观察到的版本，避免覆盖冲突。
4. 扫描 SQLite 中所有 `pending` 意图。
5. 匹配 `last_intent_id` 或 `batch_registry` 内容哈希：匹配则记为已提交，否则记为已中止。
6. 写入 `runtime/recovery/latest.json`，HTTP 报告读取同一事实集。
7. 后续批次的版本、窗口聚合、迟到序列、会话 ID 均从恢复后的单一状态继续，因此后续结果与未中断路径一致。

## 5. 查询可见性与分页

首次分页查询在状态锁内完成过滤、序列化和排序，并创建查询视图。游标只包含 `view_id + offset`。后续请求读取同一视图，不重新读取当前状态，因此：

- 分页期间发生新提交不会改变页内容。
- 不会因新窗口插入导致重复行。
- 不会因旧窗口被淘汰导致遗漏行。
- `as_of_version` 在整个分页链中保持不变。

查询视图有数量上限，过期游标返回 `E_CURSOR_EXPIRED`，不会退化为对当前状态重新分页。

## 6. 并发策略

- 全局单写者：所有状态变更串行，避免跨分组水位与容量淘汰冲突。
- 同 ID 在途去重：请求哈希一致则等待；哈希不同立即冲突。
- 查询并发：查询只在构建视图时短暂获取状态锁，之后读取自己的不可变视图。
- 写入可见性：快照文件原子安装成功后，才替换内存状态引用；查询不会在文件写入中途看到新状态。

## 7. 确定性选择

- 滚动/滑动窗口按整数毫秒和纪元网格计算，不使用本地时区。
- 批次事件排序键：`(timestamp_ms, group, value_is_null, value)`。
- 查询排序键：`(window_start_ms, group, window_type, window_end_ms, session_id)`。
- 淘汰排序键：`(window_end_ms, window_start_ms, group, window_type, window_key)`。
- 分位排序后按最近秩取值。
- 求和采用 Neumaier 补偿，再按固定 Decimal 位数和银行家舍入输出。

## 8. 已知边界

- 精确分位需要保存窗口内非空度量值；本项目面向本地固定样本，容量由窗口和批次硬上限约束。
- 显式快照也会生成新状态版本，但不改变窗口聚合。
- 批次注册表用于跨重启幂等；当前样本规模下不设独立上限。长期大规模运行可增加按保留期归档策略。
- 迟到队列按事件-窗口分配计数，因此一个滑动事件可能生成多条队列记录，这比按事件计数更能准确表达具体受影响窗口。
