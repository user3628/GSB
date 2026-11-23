# 设计与一致性说明

## 1. 组件边界

```text
raw bytes
  -> codec.Decode          完整校验魔数/长度/CRC/摘要，不做部分导入
  -> compat.Decide         显式矩阵三态裁决
  -> normalize.Normalize   bps/ms、舍入、默认值、溢出
  -> engine.Import         单写者状态机、冲突裁决、幂等
  -> store.CommitImport    一个 SQLite 事务写快照/策略/派生/报告/活动指针
```

只读请求走独立的 SQLite 读事务。活动版本指针与对应策略/派生结构在同一事务提交，因此 SQLite MVCC 只会暴露提交前或提交后的完整状态。

## 2. SQLite 表

- `snapshots`：已提交快照元数据、源摘要、内容摘要、降级明细。
- `active_snapshot`：单行活动版本指针。
- `policies`：按 `(snapshot_version, policy_id)` 持久化归一化策略。
- `route_index`：路由匹配索引，引用快照版本。
- `rate_window_stats`：按服务/对齐窗口聚合的限流统计。
- `circuit_state`：按服务的熔断窗口与状态。
- `service_summaries`：依赖上述派生结构的服务汇总视图。
- `snapshot_derivations`：派生重建标记、影响服务集与计数。
- `import_reports` / `import_report_items`：批次与逐条结局审计。
- `intents`：意图日志，状态 `pending|completed|aborted|rejected`。
- `request_registry`：请求 ID、请求摘要、已存响应与状态。
- `recovery_reports`：恢复报告。

所有派生表都携带 `snapshot_version`。旧快照的策略与派生结构保留为不可变版本，显式版本查询可继续绑定读取。

## 3. 导入状态机

1. 校验 `X-Request-ID`，读取请求登记表。
2. 若请求已存在：
   - 请求摘要一致：逐字返回原 HTTP 状态与响应体。
   - 摘要不一致：`E_CONFLICTING_REQUEST`。
3. 完整解码、兼容裁决、归一化；失败时仍先写意图与请求失败记录，保证重试语义。
4. 写入 `pending` 意图，然后写入 `in_progress` 请求记录。
5. 单写者锁内读取活动版本，校验版本严格递增。
6. 计算冲突结局与最终有效策略集合。
7. 在一个事务内写入：新快照、策略、全部派生结构、导入报告、请求响应、意图完成状态，并最后切换 `active_snapshot`。
8. 任一拒绝项存在时不写快照/策略/派生；写入拒绝报告后返回 `E_BATCH_REJECTED`。

## 4. 冲突裁决

同一 `policyId` 的确定规则：

1. `policyRevision` 更高者胜。
2. 版本相同时，`lastWriteMs` 更晚者胜。
3. 两者仍相同：
   - 语义摘要相同：`skip`。
   - 语义摘要不同：按稳定字典序选择摘要较小者。
4. 批内重复 `policyId`：标记 `reject`，整批回滚。
5. 输入中不存在的旧策略：从新活动快照中移除。

报告结局固定为 `overwrite`、`skip`、`reject`，每条包含 `basis` 文本和版本/时间/摘要细节。首次插入也归类为 `overwrite`，依据明确写为“无同标识现有策略”。

## 5. 派生重建与级联一致性

- 快照整体替换会为最终策略集合重建所有派生表。
- `affectedServices` 通过比较新旧服务策略集合及语义摘要计算。
- 路由索引、限流窗口、熔断状态、服务汇总和重建标记全部写入同一事务。
- `active_snapshot` 是该事务的最后写入之一；查询只有在事务提交后才能看到新版本。
- 读请求在单个读事务中确定版本并读取对应派生行，避免不同表跨版本混读。
- 显式旧版本仍可读取旧策略和旧派生结构，它们不被新版本覆盖。

## 6. 并发与幂等

- 进程内 `writeMu` 串行化所有写导入。
- SQLite WAL 允许多个只读事务与单写事务并行。
- 同一请求 ID 的并发调用中，后到者在写锁上等待，随后复用首个结果。
- 请求摘要包含格式提示、截断规则与完整原始字节。
- 写事务暂停或未提交时，其他连接的读事务只看到旧活动版本。

## 7. 意图与恢复

写入顺序：

1. 先提交 `intents(pending)`。
2. 再提交请求 `in_progress` 登记。
3. 业务写入在事务内完成。
4. 同一事务把意图标记完成/拒绝；恢复扫描只处理仍为 `pending` 的意图。

进程被强杀时，SQLite 回滚未提交事务。启动过程：

1. 查询 `status='pending'` 的意图。
2. 查询活动版本、快照行是否存在、策略与派生计数。
3. 先写恢复意图。
4. 在一个事务中把待处理意图改为 `aborted`、把对应 `in_progress` 请求改为中止响应、写入恢复报告、完成恢复意图。

恢复报告字段全部来自 SQLite 已提交行：

- `activeSnapshotVersion`
- `activeSnapshotIntegrity` 的预期/实际计数
- `pendingIntentsObserved` 及其已落盘 facts
- 每个意图的 `snapshotRowPresent`
- `abortedBatches`
- `factsSource = sqlite_committed_rows_only`

不会根据内存计划、日志文本或测试标记推断已提交状态。

## 8. 已知边界

- 设计目标是一个进程独占一个 SQLite 数据库；跨进程写互斥依赖 SQLite 锁，但未额外实现分布式单写者租约。
- 快照版本与策略版本均存入 SQLite 有符号整数列，导入时显式检查不超过 `int64` 上限；bps 使用文本列保存 `uint64`。
- 端点强制回环监听；不连接真实网格控制面。
- 测试暂停头默认关闭，仅在本地强杀测试环境中启用。
