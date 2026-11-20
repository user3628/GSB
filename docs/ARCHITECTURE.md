# 架构、事务与原子性

## 1. 数据目录

```text
data-dir/
  meta.json
  segments/active-00000001.kvs
  snapshots/snapshot-000000000001.kvs
  snapshots/snapshot-000000000001.manifest.json
  intents/txn-000000000001.intent
  intents/txn-000000000001.commit
  intents/txn-000000000002.abort
  recovery/last_report.json
```

- `meta.json` 是活动段、最后事务号、快照序号的可重建缓存。
- `.intent` 是操作意图，包含事务号、操作类型、目标段、`pre_size`、预期后大小、段标识、批量键等。
- `.commit` 是提交事实。存在提交标记表示段内容达到可应用边界。
- `.abort` 表示进程内已经成功回滚的正常失败事务，启动恢复不得再次截断后续事务。
- 快照 manifest 保存格式版本、快照序号、生成时间、段文件大小与 SHA-256。

## 2. 内存状态与隔离

`StoreState` 包含索引字典、过期最小堆、活动段路径、快照序号和最后事务号。写者从旧状态复制索引字典和堆，完成持久化后通过一次指针交换发布新状态。

读者执行 `acquire -> 读取索引和段 CRC -> release`。读者捕获的是旧状态引用，因此写者追加新键时它仍看到写前索引。快照切换不删除旧文件；状态根退休后，只有最后一个读者释放引用时才清理旧索引和堆。

写锁 `threading.Lock.acquire(blocking=False)` 保证 HTTP 服务内单写者。任何写请求遇到活动写事务立即返回 `E_CONCURRENT_WRITE`。导入批次另外维护 `X-Batch-Id` 的 in-flight/completed 集合，确保同批次只成功一次。读路径不获取写锁。

## 3. 追加事务协议

每个普通写、删除、导入、过期清理执行：

1. 在非阻塞写锁内读取当前状态，计算目标段、`pre_size`、预期 `post_size`。
2. 原子写入并 `fsync` `.intent`。
3. 向段文件追加记录帧并 `fsync`；若发生进程内异常，截断回 `pre_size` 并写 `.abort`。
4. 原子写入 `.commit`，内容包含实际后大小、活动段和段标识。
5. 构造新不可变状态并一次替换状态根。
6. 原子写 `meta.json`。meta 失败不影响已提交事实，重启可通过 `.commit` 修复活动指针。

恢复时：

- 有 `.commit`：验证事务号、段标识、活动段存在且文件长度不小于提交大小，视为已应用。
- 无 `.commit` 且有 `.abort`：正常失败事务，已回滚，不再操作段。
- 无 `.commit` 且无 `.abort`：崩溃遗留事务。追加事务将目标段截断到 `pre_size`；创建的未提交快照删除目标与 manifest，加载型切换不删除外部源文件。

段头提供 `segment_id` 与快照元数据，恢复报告只使用持久化意图、提交/中止标记和段头事实生成。

## 4. 导入冲突规则

先按文件内同键分组：

1. 最高版本获胜；同版本以最大 `written_at_us` 获胜。
2. 若最高版本与最大写入时间仍绑定多个不同 value，整组拒绝，原因 `unresolved_same_version_and_write_time`；其他同键较低优先级记录标为跳过。
3. 文件内低于获胜者的重复项标为 `skip/duplicate_key_lower_priority`。

获胜记录再与当前索引比较：

| 条件 | 结果 | 原因示例 |
|---|---|---|
| 新键 | `overwrite` | `new_key` |
| 导入版本更高 | `overwrite` | `higher_version` |
| 导入版本更低 | `skip` | `lower_version` |
| 同版本、导入写入时间更晚 | `overwrite` | `newer_write_time` |
| 同版本、导入写入时间更早 | `skip` | `older_write_time` |
| 同版本/时间/值/过期完全相同 | `skip` | `duplicate_record` |
| 同版本/时间但值或过期不同 | `reject` | `unresolved_same_version_and_write_time` |

接受记录作为一个追加事务提交。拒绝记录只影响对应键，不导致同批次其他已接受键回滚。

## 5. 快照原子切换

创建快照时：

1. 在写锁内从当前状态读取每条最新记录（含墓碑），写临时快照段并校验其头部与记录 CRC。
2. 构造候选状态与 manifest，但暂不发布。
3. 写入 `.intent`，随后用 `os.replace` 将临时段和 manifest 放入最终路径并 `fsync` 快照目录。
4. 写 `.commit`，然后一次替换内存状态根。旧状态待读者引用归零后释放。

加载外部快照时先完整校验：文件存在、是快照类型、格式版本兼容、快照序号严格递增、可选 manifest 序号与 SHA-256、全部记录 CRC。任何失败都在写 `.intent` 与状态切换之前返回 `E_SNAPSHOT_INVALID`，旧索引、堆和活动段保持不变。

由于索引项只指向一个 `StoreState.segment_path`，快照切换不会产生部分索引指向新段、部分指向旧段的混合状态。

## 6. 保证与限制

- 进程内写事务串行；读事务并发且不受写锁阻塞。
- 恢复边界是完整事务；未提交追加的尾部会被截断。
- 恢复报告不保证硬杀进程生成退出报告，只基于持久化事实。
- 单个 `data-dir` 不支持多个操作系统进程同时写；需要时应由外部进程锁或仅运行一个 HTTP 服务实例保证。
- 追加段和旧意图默认保留用于审计，长期运行可按部署策略离线压缩。
