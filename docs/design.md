# 技术设计：策略迁移、预算重估与恢复

## 1. 不变量

1. 任意可读当前状态都对应唯一 `state.current_version`，且该版本在 `snapshots` 中为 `complete`。
2. 一个快照的策略、配额、派生服务、放置、引用和加权指数只能一起可见；不会出现“新期望副本 + 旧指数”。
3. 写意图先于业务事务提交。业务事务把新快照、当前版本指针和意图 `committed` 状态一起提交。
4. 格式协商与迁移裁决必须在同一 `base_version` 上完成，审计记录绑定最终快照版本。
5. 权重/配额/上下界/目标并发变化时，重建整个派生结果集，而不是原地修改局部行。

## 2. SQLite 表

| 表 | 用途 |
|---|---|
| `state` | 单行当前快照指针 |
| `snapshots` | 版本、类型、来源格式、状态 |
| `snapshot_state` | 快照阈值与相对容差 |
| `strategies` | 每版本完整分层策略及 `source_json/source_format/source_version` |
| `quota_groups` | 每版本配额组 |
| `derived_services` | 期望副本、并发需求、资源容量、放置 JSON |
| `derived_index` | 完整加权指数契约 JSON |
| `references_v1` | 服务到期望副本、放置、加权指数、配额组的引用 |
| `intents` | 写意图状态机 |
| `migration_batches` | 旧版迁移批次裁决 |
| `import_audits` | 格式协商、迁移结论、载荷 SHA-256 |
| `rebuild_events` | 每次级联重建的影响服务与原因 |
| `recovery_reports` | 启动恢复报告 JSON |

历史快照不可变；新版本复制未变字段，保证旧版本可用于恢复和离线核对。

## 3. 策略校验

- 服务标识非空、长度不超过 128 字节、不包含控制字符、空白、`/`、`\`。
- 副本上下界均为 `[1,1000000]`，且下界不大于上界。
- 目标并发 `[1,10000000000]`；延迟预算 `[1,3600000]` 毫秒；权重 `[1,1000000]` 千分比。
- 显存、算力、配额字段不得为 0，且不得超过 `int64` 最大值，避免 SQLite 有符号整数溢出。
- 原生新版策略的配额必须存在，且配额容量至少能支撑最小副本数。
- 批量服务标识唯一；失败整批不写快照。

## 4. 迁移裁决

迁移计划逐服务生成：

- `mapping_basis`：固定副本到上下界、废弃字段映射或安全丢弃依据；
- `degradations`：权重默认 1000、目标并发与延迟预算补齐；
- `equivalence`：旧固定副本、新期望副本、差值、允许差值、配额容量、是否自洽；
- `reasons`：未映射废弃字段、缺失配额、预算不一致。

`expected_replicas` 使用新版推导规则计算，但不把结果抬升到 `min_replicas`，以便容量不足时暴露真实缺口。例如 `q-tight` 只能承载 2 个副本而旧策略要求 6 个，则输出 `expected=2, fixed=6, difference=4, allowed=1, within_tolerance=false`，HTTP 返回 409 `E_MIGRATION_AMBIGUOUS`。

批次级还计算 `weighted_index_equivalence`：用默认权重分别合成旧固定副本与新期望副本，比较两个定点指数，并对旧指数应用同样的相对/绝对容差。这样可防止“每个服务单独看似接近、整体预算却漂移”的静默通过。

## 5. 加权指数数值契约

设服务按 ID 字典序为 `i=0..n-1`：

```text
scale = 1_000_000
W = sum(weight_i)
N_i = desired_i * weight_i * scale
index = half_up(sum(N_i) / W)
```

每一步计算 `q_i, r_i = divmod(N_i, W)`；`r_i=0` 为 `exact`，否则默认 `floor`，当 `2*r_i >= W` 时加一并标记 `half_up`。该步误差分子：

```text
E_i = rounded_i * W - N_i
```

累计误差为 `sum(E_i)`，分母为 `W`；补偿量为最终精确半入结果减逐步结果之和。阈值容差按阈值而非指数计算，避免阈值接近 0 时产生无意义的相对容差。所有中间大整数使用 `math/big.Int`，最终指数必须落在 `int64` 可表示范围。

## 6. 事务状态机

```text
无意图
  -> createIntent: pending（独立提交）
  -> after_intent 故障点（可选进程退出）
  -> BEGIN
       base_version 检查
       读取基快照
       应用变更
       全量校验与派生重建
       写新快照及所有派生行
       mid_rebuild 故障点（可选进程退出）
       更新 current_version
       intent -> committed
       migration_batch -> accepted/aborted
     COMMIT
```

失败路径在事务回滚完成后执行 `intent -> aborted`，避免 SQLite 单写连接自锁。启动恢复：

1. 读取当前版本与最大 `complete` 版本；不一致则校正。
2. 扫描 `pending/applying` 意图。
3. 意图改为 `recovered_aborted`，错误码 `E_PROCESS_TERMINATED`。
4. 关联迁移批次改为 `aborted`。
5. 写入恢复报告；查询接口只读取该持久化报告。

## 7. 并发基版本校验

SQLite 连接池限制为 1，事务串行化。两个同基线增量都可能先留下 `pending` 意图，但只有先进入并成功提交的事务能推进 `current_version`；后一个事务在基版本检查处失败并收到 `E_STALE_BASE`，其意图被标记中止，策略与派生状态不变。

## 8. 格式损坏定位

二进制读取严格按记录边界推进，记录序号从 1 开始。以下情况均返回 `E_RECORD_CORRUPT` 和序号：

- 未知记录类型；
- 长度前缀超出剩余字节或超过 16 MiB；
- CRC32 摘要不匹配；
- payload 内部整数/字符串截断；
- 声明记录数之后存在尾随字节（序号为 `record_count+1`）。

格式版本或端序标记非法返回 `E_FORMAT_UNSUPPORTED`；高版本在解码前由兼容矩阵拒绝，防止未知必填字段被静默忽略。
