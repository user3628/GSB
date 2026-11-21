# BlockLab 技术设计

## 1. 执行模型

每个作业绑定一个 `.npz` 张量分片、一个 YAML 配置和形状清单中的声明目标形状。作业目录内按 `activation → reorder → reduce` 推进，阶段不可跳跃。每个阶段被拆成确定性块序列：

- `activation`：将张量视为 `(prod(shape[:-1]), shape[-1])` 的二维矩阵，按 `row_block × col_block` 切块；4-D 布局为 `[B,H,S,D]`。
- `reorder`：`swap_head_seq` 在 4-D 上交换轴 1/2，在 2-D 上做矩阵转置；`tile` 以块为瓦片进行重排。
- `reduce`：与 `reorder` 使用相同的目标块序列，对重排结果块做稳定归约并写入块摘要。

块产物、块摘要和检查点记录均为不可变文件。只有连续序号前缀中的记录会被视为可用。

## 2. 激活稳定性

每个激活块先求有限元素的块内参考极值 `r`（按绝对值最大元素选取），计算 `shifted = x - r`，再在浮点域恢复 `shifted + r` 后求值。异常块的输入 `NaN/Inf` 在输出中保留原哨兵值，不做零替换。

- GELU：Abramowitz & Stegun 7.1.26 误差函数近似，避免外部 SciPy 依赖。
- SiLU：使用按符号分支的稳定 sigmoid。
- Sigmoid：`exp(-abs(x))` 公式避免正负极端值指数溢出。

有限输出检查合法值域：GELU `[-0.171, +∞)`、SiLU `[-0.279, +∞)`、Sigmoid `[0,1]`。NaN、Inf 或全零行任一出现时，块状态为 `error`，诊断包含分片、块键、行列区间、首值样本、计数和异常值。

## 3. 重排语义

### swap_head_seq

- 4-D：目标形状 `[B,S,H,D]`；每个目标块记录源 `[b,h-range,s-range,:]` 与目标 `[b,s-range,h-range,:]`。
- 2-D：目标形状 `[C,R]`；源/目标块互为转置。

### tile

- 4-D 在每个 `(batch, head)` 平面按 `S × D` 切瓦片；2-D 在整个矩阵上切瓦片。
- 完全位于内部的整瓦片按“源行优先顺序 → 目标列优先顺序”做双射重排。
- 最后一行/列的尾块保持原位，从而在行数、列数非整除时仍保持声明形状并覆盖每个元素一次。
- 每个块记录 `source_region`、`target_region` 与源/目标瓦片坐标。

重排前先按模式推导实际目标形状，并同时对比形状清单声明与 YAML `target_shape`。不一致返回 `E_SHAPE_MISMATCH`，详情同时包含 `declared_shape` 与 `actual_shape`。

## 4. 稳定归约

- `sum`：`math.fsum`。
- `mean`：`math.fsum / finite_count`。
- `l2`：迭代 `math.hypot(norm, value)`，不做平方连乘，避免大值溢出。

NaN/Inf 不计入有限聚合值，但绝不修改产物；诊断记录非有限元素数量与样本值。全零行同样触发 `E_NUMERIC`。

## 5. 检查点与恢复

目录布局：

```text
work/jobs/<job_id>/
  checkpoints/<stage>/e000001/
    seq-00000001-<block-key>.json
    blocks/00000001-<block-key>.npy|json
  locks/
  archive/
  quarantine/
  outputs/
```

记录关键字段：

```json
{
  "schema": "blocklab.checkpoint.v1",
  "sequence": 1,
  "epoch": 1,
  "job_id": "demo",
  "shard_id": "demo",
  "stage": "activation",
  "input_fingerprint": "sha256 of stage input",
  "stage_fingerprint": "sha256 of stage plan",
  "config_version": "sha256 of stage config",
  "workspace_config_version": "sha256 of full config",
  "block": {"index": 0, "key": "...", "coords": {}, "row_range": [0, 3], "col_range": [0, 4]},
  "product_digest": "sha256 of artifact bytes",
  "product_kind": "npy",
  "artifact": "blocks/...",
  "status": "ok",
  "metadata": {"shape": [3, 4], "diagnostics": null}
}
```

写块顺序：先原子写产物，再原子写记录。两者都使用同目录临时文件、`fsync`、`os.replace` 和目录 `fsync`。进程在任意点退出时，最多留下可忽略的临时文件或未引用产物，不会出现半条记录。

恢复时按文件名/内部序号检查：

1. JSON 可解析且字段完整；
2. schema、job、stage、epoch、序号一致；
3. 阶段计划、阶段输入指纹和阶段配置版本一致；
4. 块键与块索引等于该序号的预定块；
5. 产物存在且 SHA-256 等于记录值。

首个不通过项之前的前缀被复用；损坏、不完整、重复序号和其后的记录被忽略，并在下次发布时隔离到 `quarantine/`。序号写入必须恰好等于当前有效前缀长度 + 1。

## 6. 锁与并发

- 元数据/失效事件：`locks/meta.lock`。
- 阶段发布：`locks/<stage>.lock`，使用 Linux `fcntl.flock`。
- 作业执行：`locks/run.lock`，非阻塞，避免同机重复执行器。

发布在阶段锁内重新扫描有效前缀，要求 `requested_sequence == expected_sequence` 且块键等于预定序列位置。落后、重复、跳号或跨块错位均返回 `E_STALE_CHECKPOINT`。

## 7. 版本指纹与失效传播

计划指纹链：

```text
activation_plan = H(input_digest, activation_type, block_sizes, activation_config_version)
reorder_plan    = H(activation_plan, reorder_mode, declared/actual target, block sizes, reorder_config_version)
reduce_plan     = H(reorder_plan, sum_strategy, tolerances, reduce_config_version)
```

各阶段完成后：

```text
stage_content = H(stage plan, ordered[(sequence, block_key, product_digest, status)])
version       = H(input_digest, config_version, all plans, all stage contents)
```

失效只从最早变化的阶段开始：

| 变化 | 最早阶段 | 下游失效 | 可复用 |
|---|---|---|---|
| 输入内容、激活类型、块大小 | activation | reorder, reduce | 无 |
| 重排模式、目标形状 | reorder | reduce | activation |
| 求和策略、容差 | reduce | — | activation, reorder |

失效事件在 `invalidations.jsonl` 中追加记录，包含原因、旧/新计划指纹、旧/新输入摘要、受影响阶段、块数量与区间。阶段目录整体归档到 `archive/`，下一纪元从序号 1 重新连续发布；历史纪元和当前纪元均可审计。

## 8. 停止与恢复

- HTTP 停止写入作业目录下 `STOP` 文件。
- CLI 的 `SIGINT/SIGTERM` 只设置停止标志。
- 执行器仅在当前块“计算、产物原子写、记录原子写”全部完成后检查停止标志。
- 恢复先清除 `STOP`，扫描连续前缀，从下一块继续；已完成块不再计算。
- 最终 `activation.npy`、`reordered.npy` 和 `summary.json` 与完整运行逐字节一致。

## 9. 确定性边界

- 所有输入转为 C 连续 `float64`。
- 块序列、块形状、聚合顺序、JSON 序列化键顺序固定。
- `np.lib.format.write_array` 生成固定 NPY 头；样本 NPZ 使用固定 ZIP 时间戳。
- 运行状态文件中的过程性统计不进入结果摘要；结果摘要不包含时间戳。
- 同一 Python/NumPy 运行环境、同一输入和配置下，块产物、输出张量和摘要逐字节一致。

