# FlashBlock 技术设计

## 1. 组件边界

`flashblock` 是纯 Python + NumPy 的本地库，核心入口如下：

- `AttentionEngine`：绑定一个分片的 Q/K/V、配置和工作区，提供全量运行、断点续算、前缀增量更新、配置失效、状态查询、失效查询与预算查询。
- `full_attention`：无检查点的分块前向核心，返回输出、每行最大值、归一化因子和行块状态。
- `reference_attention`：逐行全量参考实现，仅用于容差验证。
- `CheckpointStore`：运行代际、原子记录、文件锁、恢复扫描、隔离损坏/重复记录。
- `AttentionConfig`：YAML 配置及其语义版本指纹。

输入数组统一复制为 C 连续 `float64`。组件不会将完整的 `[batch, heads, seq, seq]` 注意力矩阵写出或持久化。

## 2. 分块在线 softmax

对每个行块和列块，保存局部状态：

```text
m_local   [B, H, q_rows]       列块内行最大值
l_local   [B, H, q_rows]       相对 m_local 的指数和
o_local   [B, H, q_rows, D]    相对 m_local 的加权 V 和
```

列块内先做 `exp(score - m_local)`，不会直接指数化原始 logits。多个列块按 key 起点和列块序号稳定排序后合并：

```text
m_new = max(m_run, m_local)
alpha = exp(m_run - m_new)
beta  = exp(m_local - m_new)
l_new = l_run * alpha + l_local * beta
o_new = o_run * alpha + o_local * beta
```

合并完成后才做 `o / l`。因此行块最大值和归一化因子在持久化、恢复、增量路径中都是同一中间量。

### 因果掩码与边界

合法 score 为 `key_index <= query_index`。非整除行数、列数、头维度均通过切片和 `min(end, length)` 处理。全掩码的局部行使用 `-inf` 最大值和零指数和，不参与有效合并。

### 数值保护

- 输入 NaN/Inf 在进入计算前返回 `E_NUMERIC`，带 shard、数组名、batch/head/sequence/head_dim 与近似 row/col block。
- logits 超出 `score_clamp` 被钳制；超过 `extreme_threshold` 被计数，聚合在 `NumericStats` 中。
- 归一化因子为零、合并出现 NaN/Inf 时返回 `E_NUMERIC`，带当前 row block、col block 和 stage。

## 3. 字节级增量一致性

全量块计算和增量重算都调用相同的局部状态计算与合并函数。增量更新流程：

1. 比较旧/新 K/V 的逐 key 内容，得到真实变化的 key 区间。
2. 以列块粒度比较内容摘要，得到受影响列块集合。
3. 因果模式下，最早受影响行块为 `min_changed_key // row_block_size`；非因果模式为 0。
4. 最早受影响行块之前的行块整体复用。
5. 受影响行块内，只有内容依赖摘要变化的列块重算；其余局部状态直接复用。
6. 以固定列块顺序重新合并所有局部状态，得到新行块状态。
7. 默认将增量结果与从零分块计算逐字节比较（输出、行最大值、归一化因子）。不一致时抛出 `E_INCREMENTAL_MISMATCH`，给出 shard、行块区间、字段和最大偏差。
8. 验证成功后，将新运行代际目录整体原子发布。

“逐字节一致”的对比对象是同一实现、同一机器、同一 NumPy 构建下的从零分块计算。逐行参考实现使用不同的归约顺序，因此按 YAML 中的 `atol`/`rtol` 做数值容差验证。

配置级失效（行/列块大小、因果开关、在线策略、数值钳制策略）会将最早受影响行块和列块都定位为 0，并清除复用清单。前缀级失效则保留因果模式下不可能看到变化 key 的行块。

## 4. 检查点格式与恢复

工作区结构：

```text
artifacts/runtime/
  shard-<name>-<hash>/
    .publish.lock
    runs/
      run-0001/
        run.json
        schedule.jsonl
        r000000-s0000000001.ckpt.npz
        ...
      run-0002/
        run.json
        invalidation.json
        schedule.jsonl
        ...
    quarantine/
```

每个 `.ckpt.npz` 同时包含 JSON 元数据和数组。关键字段：

- shard id、run id、行块序号、全局单调序号；
- 已覆盖 key 区间；
- 行级运行最大值、指数和、未归一化部分输出；
- 每个列块的局部 m/l/o 与依赖指纹；
- 输入、Q、K/V、配置指纹；
- 各数组 SHA-256 与完整 payload SHA-256。

写入使用同目录临时文件、`fsync`、`os.replace` 和目录 `fsync`。因此最终文件名只会在完整写入后出现。恢复扫描只接受连续序号和连续行块；损坏 payload 或重复序号会把有歧义的后缀移入 `quarantine/`，再从事务前缀继续。

## 5. 并发发布

`publish_next` 在跨平台文件锁（POSIX `fcntl.flock`，Windows `msvcrt.locking`）内执行：

1. 重新扫描当前连续检查点前缀。
2. 仅允许发布 `row_block == committed_count`。
3. 仅允许发布 `sequence == base_seq + committed_count`。
4. 拒绝不同 run、不同输入/配置指纹、重复行块、落后或跨越行块。

失败返回 `E_STALE_CHECKPOINT` 并附带提交序号、期望序号、当前可用序号。缺失前置状态或非法续算跳转返回 `E_INVALID_STAGE` 和当前可用序号。

## 6. 内存预算调度

`plan_row_block` 按行块估算：

- Q 行块切片；
- 当前 K/V 列块切片；
- score/weight 临时矩阵；
- softmax 合并缓冲；
- 当前行块所有列块的持久局部状态缓冲；
- `workspace_factor` 安全系数。

若预算不足，先按 batch 拆分并记录 `batch_split_required` 与原因；即使单 batch 也无法满足时返回 `E_MEMORY_BUDGET`，附带逐项 breakdown、最低需求和建议动作。每次运行的调度决策写入 `schedule.jsonl`，可通过 `budget_report()` 或 CLI `budget` 查询。

## 7. 错误码

| 业务码 | 含义 |
| --- | --- |
| `E_CONFIG` | YAML/配置字段或取值非法 |
| `E_SHAPE` | Q/K/V 或增量切片形状非法 |
| `E_NUMERIC` | 非有限输入/状态、零归一化因子、数值累加失败 |
| `E_INCREMENTAL_MISMATCH` | 复用与重算拼接结果未通过逐字节验证 |
| `E_STALE_CHECKPOINT` | 落后、重复、跨行块或 run 指纹不一致的发布 |
| `E_INVALID_STAGE` | 缺失前置检查点、非法续算/更新阶段 |
| `E_MEMORY_BUDGET` | 估算峰值超过预算，无法继续拆分 |

## 8. 已知边界

- 没有 GPU、反向传播和多进程任务队列；并发保护针对同一工作区中的发布协议。
- 当前配置只有一个真实的分块在线合并实现；`logsumexp_block_merge` 与 `stable_running_max` 使用相同的稳定分块语义，策略名变化会触发完整失效。
- 默认增量验证会额外执行一次从零分块计算。生产环境可在配置中设置 `verify_incremental: false`，此时性能路径只计算受影响块，但不会执行字节级不一致检测。
- 检查点保存局部列块状态，空间复杂度为 `O(seq^2 * D / col_block_size * batch * heads)` 量级，但不会保存 score 矩阵。

