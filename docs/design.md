# density-trace 技术说明

## 1. 组件边界

`density_trace` 将滚动密度状态拆为五层：

| 层 | 模块 | 职责 |
| --- | --- | --- |
| 输入契约 | `manifest.py` | 校验 NPZ/JSON、有效列、缺失行、标签和内容指纹 |
| 数值核心 | `numerics.py` | 稳健带宽、分块高斯核、补偿求和、定点量化、误差界 |
| 状态演进 | `state.py` | 整数密度状态、版本摘要、逐批/一次性精确合并 |
| 漂移裁决 | `reference.py` | 参考表校验、边际分位比较、两档阈值裁决 |
| 持久化与编排 | `workspace.py`, `engine.py` | 文件锁、原子检查点、停止恢复、版本失效、查询 API |

## 2. 批内 KDE

对第 b 批的二维有效样本 `x_k=(x_k,y_k)`，稳健带宽为：

```text
h_j = max(1.4826 * median(|x_j - median(x_j)|),
          IQR(x_j) / 1.349,
          bandwidth_floor)
```

网格点 `g_i` 上的单批 KDE 为：

```text
p_i^(b) = 1 / (2*pi*h_x*h_y*n_b) * sum_k exp(-0.5*((g_ix-x_k)/h_x)^2
                                                   -0.5*((g_iy-y_k)/h_y)^2)
```

实现用对数域计算 `log_kernel`，再乘显式归一化系数。禁止直接累计未缩放概率密度。

## 3. 定点域与补偿求和

配置 `density_scale=S`，每个样本核贡献先转换为定点域：

```text
c_ki = exp(log_kernel_ki) * S / (2*pi*h_x*h_y*n_b)
```

批次按 `chunk_size` 切样本块。每块对网格向量执行 Kahan 或 Neumaier 补偿求和。块结束后的补偿和量化为整数 `q_i^(block)`，再以整数加法并入该批结果。检查点记录每个块的：

- `block_index`
- `sample_start`, `sample_end`
- `partial_sum_units`
- `partial_sum_sha256`

同一输入与配置下，块顺序、浮点运算和量化均固定，因此密度曲线字节固定。不同块大小只改变量化次数和浮点抵消，曲线差由配置容差约束。

## 4. 误差界

每批误差报告由高精度参考累加与最终整数结果比较得到。设 `n_blocks` 为块数，`eps` 为 float64 机器 epsilon，`A` 为所有缩放贡献绝对值之和，则报告使用：

```text
quantization_bound_units = 0.5 * n_blocks
floating_bound_units    = 16 * eps * A
error_bound_units       = quantization_bound_units + floating_bound_units + 1
```

同时记录 `scale`、块顺序、每块部分和、观测误差、误差上界、相对上界和有效位数。Linux 上参考累加使用 NumPy `longdouble`；若平台 `longdouble` 不高于 float64，则回退到 `decimal.Decimal` 精确累加 float64 贡献。观测误差超过显式上界时返回 `E_NUMERIC`。

## 5. 精确状态合并

第 b 批的有效样本数为 `n_b`，批次定点密度为 `q_i^(b)`。累计状态为：

```text
U_i = sum_b n_b * q_i^(b)
W_i = sum_b n_b
curve_i = U_i / (S * W_i)
```

`U_i`、`W_i` 是整数，合并满足结合律和交换律。逐批合并和一次性合并产生相同整数数组、相同曲线字节和相同摘要。每个状态包含：

- `state_version`：覆盖批数，从 1 开始。
- `version_fingerprint`：状态内容、链摘要和当前配置指纹的 SHA-256。
- `covered_batches`, `last_batch_id`, `effective_samples`。
- `curve_sha256`, `payload_sha256`, `chain_digest`。

状态摘要中的 `version_fingerprint` 同时写入对应裁决，因此裁决可追溯到确定的密度状态版本。

## 6. 参考表与漂移得分

参考表存储二维查询网格、参考曲线和每个分位索引的 `probability`、`x_value`、`y_value`。正式比对前必须验证：

- 参考网格与当前配置完全一致。
- 曲线长度、有限性和非负性正确。
- 所需索引均存在且无重复。
- 概率严格递增。
- x/y 边际分位值分别严格递增。

缺失所需索引时报告第一个缺失索引；非单调时报告破坏单调性的当前索引。错误详情包含 `batch_id` 和 `quantile_index`。

当前二维密度曲线按固定网格积分得到 x/y 边际权重，再在配置概率点求边际分位。参考值同样转换为定点域。设 `q` 为参与范围内的分位点：

```text
score = sqrt(mean([
  ((q_current_x - q_reference_x) / span_reference_x)^2,
  ((q_current_y - q_reference_y) / span_reference_y)^2
]))
```

`span_reference` 使用参与范围首个到最后一个参考分位的差。得分与两档阈值比较：

- `score >= drift_threshold`：`drift`，`is_drift=True`，高置信度。
- `warning_threshold <= score < drift_threshold`：`warning`，`is_drift=False`。
- 否则：`stable`。

输出包含得分、阈值、两档置信水平、分位索引范围、定点分位值、裁决依据和参考表/曲线摘要。

## 7. 原子检查点

每批完成密度合并和漂移裁决后构造规范 JSON 记录，至少包含：

- `sequence`：全局单调序号，`batch_id + 1`。
- `batch_id`、`generation`。
- `input_digest`：NPZ 字节与 manifest 批声明的组合摘要。
- `config_version`、`config_fingerprint`。
- `stage_fingerprints`：density 和 adjudication 阶段指纹。
- `state_summary`、完整整数 `state`。
- 批密度元数据、逐块部分和、误差报告。
- `adjudication`，其中包含状态版本及其指纹。

写入使用同目录临时文件、`fsync`、`os.replace` 和目录同步。临时文件不会被历史加载器视为记录，因此任意写入中断不会暴露半写检查点。

历史加载器要求 `sequence == batch_id + 1`，并且有效记录从序号 1 连续开始。当前活动 generation 由失效元数据决定。

## 8. 并发发布

工作区使用 `.workspace.lock`：

- POSIX：`fcntl.flock(LOCK_EX)`。
- Windows：`msvcrt.locking(LK_NBLCK)` 重试直到获得独占字节锁。

发布在锁内重新加载有效历史，只接受：

```text
provided.sequence == head.sequence + 1
provided.batch_id == head.batch_id + 1
provided.generation == workspace.generation
provided.state_summary.state_version == batch_id + 1
```

落后、重复、序号跳跃、批次错位或 generation 过旧均返回 `E_STALE_CHECKPOINT`，且不会触碰已有检查点文件。

## 9. 版本变化与增量失效

配置被拆成阶段指纹：

- density 阶段：网格、带宽规则、分块大小、求和策略、缩放和容差。
- adjudication 阶段：阈值、置信水平、分位索引范围和参考表摘要。
- 批次输入：NPZ 文件内容 + manifest 批声明。

`inspect_versions()` 对每个已提交记录比较当前指纹，选择最早不匹配批次。内容变化通常只影响该批及后续批次；全局 density/adjudication 配置变化从批次 0 开始影响。

应用失效时：

1. 在文件锁内原子写入最新 `invalidations.json`，generation 加一。
2. 事件记录原因、受影响区间、旧/新版本指纹和新 generation。
3. 删除受影响检查点；即使删除中断，加载器也会按 generation 忽略旧记录。
4. 未受影响前缀保留原字节并继续作为重建父状态。

查询 `/api/version` 或 CLI `version` 可看到失效原因、区间、新指纹、最新事件和当前 generation。

## 10. 停止与恢复

`request-stop` 原子写入 `.stop.request`。执行器只在批次边界消费请求：如果停止到达时正在计算/裁决/发布，当前整批完成后再退出。消费请求时以原子 `os.replace` 改为 `.stop.ack`。

恢复时从连续有效检查点的最后一个完整状态继续，不重算已提交前缀。相同配置与输入下，恢复路径和连续路径的检查点规范 JSON 字节完全相同；记录序号、裁决和误差报告也因此一致。
