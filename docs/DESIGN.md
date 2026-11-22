# 技术设计：分层迁移、事务边界与强制终止恢复

## 1. 组件边界

服务接收上层编排器提交的完整策略快照、增量调整、批次预览/执行和恢复查询。SQLite 同时保存当前物理事实、策略历史、批次意图、状态轨迹和报告。对象数据本身由上层块存储管理，本组件负责确定分片层归属和容量账本。

层顺序即介质等级：`layers[0]` 最热，索引越大越冷。温度跨层规则如下：

```text
heat >= threshold  -> rank - 1
heat <  threshold  -> rank + 1
rank 越界           -> 该分片不是候选
```

候选还要求 `cooldown_until <= now`。批次构造再应用目标层容量预留与 `batch_limit`。

## 2. 数据模型

| 表 | 作用 |
| --- | --- |
| `policy_snapshots` | 不可变快照历史，保存规范 JSON 与 SHA-256 |
| `shards` | 当前物理事实：大小、热度、所在层、冷却截止 |
| `layer_state` | 当前快照下的层容量、占用、冷热字节、资格与直方图 |
| `policy_runtime` | 当前快照的候选资格聚合与规则标识 |
| `shard_policy_state` | 快照/迁移事务内重建的分片级资格、目标层、冷却与冷热分类 |
| `batches` | 批次整体状态、阶段、请求哈希、计划与结果 |
| `batch_items` | 批次内逐分片源/目标、状态、最后变更时刻与字节账 |
| `transition_events` | 只追加的状态迁移审计轨迹 |
| `migration_intents` | 迁移前持久化的意图事实与最终处置状态 |
| `lifecycle_runs` | RUNNING / SAFE_EXITED / FORCED_RECOVERED 进程生命周期 |
| `reports` | STARTUP_RECOVERY 与 SAFE_SHUTDOWN 两类分离报告 |
| `meta` | 当前快照、级联代数、`state_epoch` |

核心不变量：

```text
layer_state.used_bytes(layer) == SUM(shards.size_bytes WHERE shards.layer_id = layer)
layer_state.shard_count(layer) == COUNT(shards WHERE shards.layer_id = layer)
SUM(layer_state.used_bytes) == SUM(shards.size_bytes)
```

每批次完成、策略应用和启动恢复后都在同一事务内重算并核对上述不变量。核对失败会回滚事务并返回内部一致性错误。

## 3. 写事务与提交边界

### 3.1 策略提交

一个事务内完成：

1. 校验摘要、版本和层拓扑；
2. 以当前分片占用检查所有新容量上限；
3. 插入完整新快照；
4. 更新当前版本与 `cascade_generation`；
5. 删除并重建全部 `layer_state` 与 `policy_runtime`；
6. 增加 `state_epoch`。

任何容量溢出或摘要失败都不会产生部分快照。增量调整先在事务内比较 `base_version` 与当前版本，再基于**完整旧快照副本**修改字段并重算摘要，避免新旧参数混合。

### 3.2 批次准备

准备事务把以下事实一起提交：

- 批次 `MIGRATING / PLANNED`；
- 完整候选计划、截断清单和容量预留依据；
- 每个候选 `PENDING` 状态；
- 每个候选一条 `PREPARED` 意图，包含源层、目标层、大小、快照版本与级联代数；
- 初始 `NONE -> PENDING` 迁移事件。

该提交完成后才可能进入故障注入点 `after_intent`。如果进程此时终止，重启可以仅根据意图和分片当前位置判断迁移尚未发生。

### 3.3 执行阶段标记

准备完成后单独提交 `phase=TRANSFER_STARTED`。它是持久化检查点，不代表任何分片或占用已变化。此阶段的强杀同样会在下次启动被识别为未完成批次。

### 3.4 迁移提交事务

一个 `BEGIN IMMEDIATE` 事务内完成所有选中分片：

1. `PENDING -> MIGRATING`，写迁移事件；
2. 更新 `shards.layer_id`、冷却截止和最后批次；
3. 对源层做减法、对目标层做加法；
4. `MIGRATING -> COMMITTED`，写释放/占用字节；
5. 意图改为 `APPLIED`；
6. 从分片表重建全部层与热度摘要；
7. 校验占用等式；
8. 写批次 `COMMITTED` 结果与 `committed_epoch`；
9. 增加 `state_epoch`。

全部步骤只有一次 `COMMIT`。SQLite WAL 保证读者要么看到事务前版本，要么看到事务后版本。强杀在提交前会丢弃整个事务；强杀在提交后则批次和占用均为完整新版本。

## 4. 崩溃点矩阵

| 中断位置 | 已持久化事实 | 重启处置 |
| --- | --- | --- |
| 意图提交前 | 没有批次或只有回滚后的无效果事务 | 无未完成批次 |
| `after_intent` | 批次 MIGRATING、意图 PREPARED、分片未移动 | 批次和分片 ABORTED，意图 ROLLED_BACK |
| 执行阶段标记后 | 同上，另有 `TRANSFER_STARTED` 检查点 | 同上 |
| 迁移事务提交前 | 事务内状态/占用全部未提交 | 同上；占用从分片事实重建 |
| 迁移事务提交后、响应前 | 批次 COMMITTED、结果、占用全部已提交 | 不中止；重复请求复用结果 |
| 恢复事务提交前 | 恢复变更未提交 | 下次启动重做恢复 |
| 安全退出报告提交后 | SAFE_EXITED 与报告已提交 | 不视为强制终止 |

恢复报告明确使用 `persisted_only=true`、`safe_exit_summary_available=false` 和 caveat，表示它只重建已落盘事实，不承诺崩溃时刻存在完整退出信息。

## 5. 并发与幂等

- 进程内所有写路径使用可重入写锁；SQLite 事务使用 `BEGIN IMMEDIATE`，数据库层也拒绝并行提交。
- 读路径不复用写连接，每个查询独立 `BEGIN`，在一个 WAL 快照中读取版本、占用、批次与状态。
- 批次主键保证同一标识只存在一行。请求哈希由 `batch_id + snapshot_version` 规范化计算；哈希不一致直接 `E_CONFLICTING_BATCH`。
- 同进程并发重复触发同一批次时，第二个调用等待写锁；第一个提交后第二个只读取并返回原结果。跨进程部署仍应限制单执行器。
- 增量调整以 `base_version` 做乐观并发检查；同一基线的第二个提交返回 `E_STALE_BASE`。
- `state_epoch` 每次写事务递增，可用于上层检测是否跨越了提交版本。

## 6. 候选与截断

候选排序固定为：

```text
heat DESC, shard_id ASC
```

目标层容量预留使用：

```text
target_current_used + bytes_already_accepted_for_target
```

不提前抵扣源层迁出释放量，采用保守策略。被截断原因包括：

- `BATCH_LIMIT`：本批已达数量上限；
- `TARGET_CAPACITY`：目标层容量不足，附 `shortfall_bytes`、当前已接受占用与容量。

所有截断项都进入响应，不静默丢弃。

## 7. 状态机

```text
PENDING    -> MIGRATING | ABORTED
MIGRATING  -> COMMITTED | FAILED | ABORTED
FAILED     -> PENDING
COMMITTED  -> terminal
ABORTED    -> terminal
```

执行器内部每次状态改变都先校验迁移表，再追加 `transition_events` 并更新 `batch_items`。校验接口不修改数据库；非法路径返回 `E_INVALID_TRANSITION`，并在上下文标记 `persisted_state_changed=false`。

`COMMITTED` 的资源转移只能由批次提交事务产生，避免通过管理接口绕过容量账本。

## 8. 安全退出与报告

`POST /v1/admin/safe-shutdown` 或 FastAPI lifespan 正常退出时：

1. 中止仍处于 PENDING/MIGRATING 的批次（正常路径通常为空）；
2. 重建并核对层占用；
3. 汇总所有 COMMITTED 批次；
4. 写 `SAFE_SHUTDOWN` 报告；
5. 将当前 lifecycle run 标记为 SAFE_EXITED。

下次启动看到 SAFE_EXITED 时不会生成强制恢复报告。看到 RUNNING 时则执行启动恢复并生成 `STARTUP_RECOVERY`。两类报告都只从 SQLite 已提交行读取。

## 9. 故障注入

`POST /v1/faults/injection` 支持：

- `mode=raise`：在指定持久化边界抛出模拟终止异常，进程仍在，便于单元测试；
- `mode=hard_exit`：调用 `os._exit(73)`，真实模拟进程被强制终止。

注入点：

- `after_intent`；
- `after_transfer_before_commit`；
- `after_commit_before_response`。

该接口仅用于本地验收；服务默认绑定回环地址。

## 10. 已知边界

- SQLite 适合单机单写者；多进程部署需要外部单执行器租约。
- 块数据复制不在本组件范围内；本组件维护逻辑归属与容量账本。
- 冷却截止时刻持久化在分片上；调整策略冷却时长不会自动清除已经写下的截止时刻。
- `os._exit(73)` 是刻意保留的本地故障注入能力，生产打包时应通过路由开关或部署策略移除。
