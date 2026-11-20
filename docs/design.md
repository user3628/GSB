# 设计与一致性说明

## 1. 数据模型

SQLite 是唯一事实来源。主要表：

| 表 | 作用 |
|---|---|
| `versions` | 每次成功摄入产生一个全局提交版本 |
| `write_intents` | 提交前的持久化意图，供强杀恢复扫描 |
| `events` | 每条原始事件及其提交版本 |
| `bucket_versions` | 每个基础桶的追加式版本历史，含 `OPEN/CLOSED` |
| `closed_budget_snapshots` | 桶首次关闭时冻结的预算快照 |
| `service_state_versions` | 每个版本的服务逻辑水位 |
| `alerts_history` | 每个版本、服务和 SLI 的告警判定 |
| `rejected_events` | 迟到拒绝原因与当时水位 |
| `ingest_batches` | `batch_id` 幂等 payload 哈希和原响应 |
| `recovery_reports` | 启动扫描产生的持久化恢复报告 |

`bucket_versions` 的主键是 `(service, sli_type, bucket_start_ms, version)`。读取窗口时按基础桶
取 `version <= snapshot` 的最大版本。写入新计数或把桶从 `OPEN` 变为 `CLOSED` 都追加新行，
不会覆盖历史。

## 2. 提交协议

```text
validate payload + durably commit RUNNING intent
                    |
                    v
compute in memory (events, buckets, closures, alerts)
                    |
                    v
BEGIN IMMEDIATE
  insert events
  insert changed bucket versions and close transitions
  insert closed snapshots
  insert service watermark versions
  insert global version
  insert alert history
  insert idempotency response
  mark intent COMPLETE
COMMIT
```

业务状态只在上面的单事务中出现。intent 需要先独立提交，才能在强杀后留下可扫描的持久化
事实。若计算阶段终止，数据库没有部分业务版本；若最终事务终止，SQLite 原子回滚。

## 3. 窗口不变量

设基础桶为 `B_i = [i*60s, (i+1)*60s)`。任意配置窗口
`W = [as_of - L, as_of)` 是一组连续基础桶的并集。因此：

```text
count(W, good)  = sum(count(B_i, good)  for B_i in W)
count(W, total) = sum(count(B_i, total) for B_i in W)
```

同一版本中，桶数据不变化；窗口结果只依赖版本、窗口长度和 `as_of`。这正是查询一致性和
跨窗口同源性的基础。

滑动窗口有重叠，错误预算比例没有可加性：

```text
budget_ratio(W1 union W2) != budget_ratio(W1) + budget_ratio(W2)
```

规范预算账本因此使用互斥的 1 分钟切片。令 `D` 为对齐后的查询范围，对所有基础桶：

```text
D = disjoint_union(B_i)
bad(D) = sum(bad(B_i))
allowed(D) = sum(allowed(B_i))
budget_ratio(D) = bad(D) / allowed(D)
```

`tumbling_windows` 对每个配置时长把同一 `D` 完整分区。例如 5m 分区 24 个切片、30m 分区 4 个、
1h 分区 2 个；三者的 `sum(bad)` 和 `sum(total)` 必须相同。`consistency_checks` 对这些不变量
做实际比较。接口不声称重叠滚动窗口比例可以直接相加。

## 4. 水位、关闭和迟到

水位是服务已接受事件的最大时间戳，而非服务器墙钟。它使固定的历史样本可回放，并让迟到
判定完全确定。

```text
late if event.timestamp < batch_final_watermark - late_tolerance
closed if bucket_end + late_tolerance <= watermark
```

批次先确定最终水位，再验证该批所有事件，因此 JSON 数组顺序不会改变结果。任何违规都会
使整批不提交事件或桶变更，但拒绝审计会单独持久化。已关闭桶不会被更新；其预算快照在首次
关闭时写入 `closed_budget_snapshots`。

选择拒绝策略而不是“挪到下一可用窗口”，避免把错误归属到时间上不相干的服务窗口，也让
SRE 能通过 `E_LATE_EVENT` 和 `/v1/rejections` 明确处理数据管道问题。

## 5. 采样与估计

事件先满足精确上限时直接使用桶计数。超过上限时，对窗口快照中的事件计算：

```text
priority = blake2b256(event_row_id | timestamp_ms | good)
sample = N 个事件中 priority 最小的 K 个
```

这是固定种子的确定性优先蓄水池，K 为精确上限。事件行 ID 和内容在同一快照内不变，因此采样
选择逐字节稳定。样本率是 `K/N`。SLI 置信区间使用二项比例的 Wilson score 区间；预算按
置信区间端点代入 `(1-SLI)/(1-target)` 的映射区间。该区间是近似统计区间，未对大总体做额外
有限总体修正；接口同时公开样本率，调用方可在告警评审中评估误差。

告警要求稳定性和低查询开销，因此使用桶精确汇总；面向用户的超限窗口查询才进行采样。预算
汇总必须可加，也使用精确账本。

## 6. 并发模型

- 摄入：每进程一个 `RLock`，最终数据库事务 `BEGIN IMMEDIATE`；单 worker 下严格单写者。
- 查询：每个请求独立只读连接和 SQLite read transaction，先锁定 `version`，整个响应不跨版本。
- 同窗口：`CalculationCoordinator` 的 condition + in-flight 集合保证只有一个线程计算，其他线程
  等待后复用结果；缓存键包含 version、服务、SLI、窗口、`as_of`、精确上限和目标。
- 版本缓存：新摄入产生新 version，不会污染旧快照重复查询。

不宣称多 uvicorn worker 下的单写者语义。启动命令固定 `--workers 1`。

## 7. 故障边界

| 终止位置 | 持久化结果 |
|---|---|
| intent 提交前 | 无意图、无业务状态 |
| intent 提交后、最终事务前 | `RUNNING` intent；重启标为 `ABANDONED`，最后完整版本不变 |
| 最终事务执行中 | SQLite 回滚全部业务表和 `COMPLETE` 标记 |
| 最终事务提交后 | 新版本完整可见，intent 为 `COMPLETE` |

恢复报告只重建持久化事实，不会编造退出状态或未提交数据。若未来引入跨进程写者，需要把
版本分配与 single-flight 协调提升为数据库层的租约/唯一约束，而不是依赖进程内锁。

## 8. 当前取舍

- SQLite 适合单机和中等规模可观测性样本；大规模生产部署应替换为事务型分布式存储。
- 原始事件全量持久化以支持确定性重采样；更高吞吐可增加预聚合摘要和采样层。
- Wilson 区间为二项近似，不建模服务内事件相关性。
- SLO 目标是实例级配置；变更目标应作为新的部署配置并重新评审历史窗口。
- 滚动窗口用于告警与诊断，预算审计必须使用账本或平铺分区。
