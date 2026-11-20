# spmm-ops 技术说明

## 1. CSR 数据模型与校验

输入为 `A.shape=(m,n)` 的 CSR 三元组：

- `indptr.shape=(m+1,)`、`indices.shape=(nnz,)`、`data.shape=(nnz,)`。
- `indptr[0]=0`、`indptr[-1]=nnz`、差分非负。
- 所有列索引满足 `0 <= indices[pos] < n`。
- `data` 与右乘矩阵 `B.shape=(n,p)` 同为 float32 或 float64。

`CSRMatrix.from_arrays` 会复制并只读化输入。校验失败时：

- 维度非法：`E_SHAPE_INVALID`。
- `indptr` 长度/首尾/单调性、`indices/data` 长度或列越界：`E_BAD_CSR`，附 array、position、row。
- 重复索引额外次数超过容差：`E_DUP_INDEX`，附 row、column、所有 nnz 位置。

重复索引容差定义为同一个 `(row, column)` 对允许的最大额外出现次数。`tolerance=0` 拒绝任何重复；样本中的重复为额外 1 次。

## 2. SpMM 内核

逻辑计算为 `C[i,j] = Σ_{pos in row i} A.data[pos] * B[A.indices[pos], j]`。内核：

1. 按 `row_chunks × col_chunks` 组织。`col_chunks` 切分 A 的列/B 的行（K 维），不是输出列。
2. 每行块预取并稳定排序该块所有行位置。
3. 每个行块 × 列块只处理该列范围内的已存非零项，乘以 `B[k, :]` 的整行。
4. 显式零值也执行乘加，不省略位置。
5. 重复列索引按稳定排序后的出现次序累加。

累加顺序：

- `order="ascending"`：行内按 `(column_index, original_nnz_position)` 稳定升序，再按列块推进。列块只控制切片和遍历，不改变全局升序。
- `order="block"`：位置按列块编号稳定分组，列块升序推进；同一列块内保持 CSR 原始出现次序。

第 4 条使两种顺序在乱序 CSR 上可能产生不同的浮点舍入结果。固定输入、顺序、方案和策略时，执行不使用并行归约或 BLAS，输出可逐字节复现。

### 补偿和

`compensated` 使用 Neumaier 更新：

```text
t = s + x
if |s| >= |x|: c += (s - t) + x
else:          c += (x - t) + s
s = t
final = s + c
```

该更新在有限值域执行。每个输出元素一旦遇到非有限累加器或非有限乘积，就停止补偿并把该元素交给 IEEE 累加语义，防止 `Inf-Inf` 在补偿项中错误制造 `NaN`。若输出行含非有限值，内核二次扫描该行已存非零项，记录 `A_NONFINITE`、`B_NONFINITE`、`PRODUCT_OVERFLOW` 或 `ACCUMULATION_OVERFLOW`，并带 row、nnz position、column index 与输出列。

## 3. 自动分块

候选来自两维的 1、2、4、…、矩阵维度及若干整除尺寸。每个候选计算：

- `work_set = B_column_slice + accumulator(+compensation) + row_order_workspace + 4096`
  - `B_column_slice = col_block × p × itemsize`
  - `accumulator = row_block × p × itemsize`，补偿和再乘 2
  - `row_order_workspace = max_nnz_in_row_block × 48 + row_block × 256 + p × itemsize × 8`
- `traffic = repeated_B_reads + sparse_reads + dense_term_reads + output_writes + tile_spin`
- 行块 nnz 的 `max/mean-1` 与列块 nnz 的同类不均衡度。
- 空行块比例。
- `tile_count / nnz` 空转比例。

只保留 `work_set <= cache_budget` 的候选，然后以固定权重组合：

```text
0.45 * traffic_excess
+ 0.25 * load_imbalance
+ 0.20 * empty_block_ratio
+ 0.08 * spin_excess
+ 0.02 * cache_pressure
```

按 `(score, row_block, col_block)` 确定性排序。高稀疏率/高空行率若切成大量小行块，会增加空块比例与自旋项，因此被惩罚。报告保存所有可行候选的分数分量、预测访存、工作集与最终理由；若没有候选满足预算则返回 `E_MEMORY_BUDGET` 和最小候选明细。

## 4. 误差界与审计

### 4.1 理论界

令机器单位舍入 `u = eps/2`，一行有 `n` 个已存项（含重复与显式零），`S_abs = max_j Σ_pos |a_pos B[pos,j]|`。`S_abs` 的计算本身再乘 `(1+gamma_n+16u²)` 做上界膨胀。

- 顺序求和：`bound = gamma_(n+1) * S_abs`，其中 `gamma_n = n u / (1-nu)`。
- 补偿求和：`bound = (4u + (n+2)u² + 16u³) * S_abs`。

上界按输出行计算，块界取块内最大行界。参考值转换为 float64 时另加最多一个 ulp 的参考舍入余量。块内所有有限元素的实际绝对误差都必须不超过该余量；否则 `E_NUMERIC_MISMATCH`，错误详情指出块行列区间、最大绝对误差、最大相对误差和位置。

### 4.2 显式高精度参考

参考文件是本地显式 `.ref.npz`，不是从结果反推：

- 生成时使用 80 位 Decimal 上下文逐项计算。
- 输出 40 位有效数字字符串；特殊值按 IEEE 类别写为 `NAN`、`INF`、`-INF`。
- 审计读取参考字符串、验证形状，并在随机抽样位置计算 `|Decimal(output_float) - Decimal(reference_string)|`。

`sampling_seed` 与 `sampling_count` 固定时，随机位置可复现；报告同时给出参考摘要、抽样位置、十进制误差和行理论上界。非有限参考不伪造有限误差，而是严格比较 NaN/±Inf 类别。

### 4.3 自校验

独立 `naive_spmm` 按 Python 循环逐非零元累加。按 `self_check_blocks` 随机抽取逻辑行块，与内核输出比较：

- 有限值：`|actual-naive| <= atol + rtol*max(|actual|,|naive|)`。
- 非有限值：NaN/±Inf 类别必须一致。

任一失败返回 `E_NUMERIC_MISMATCH`，附具体行列区间、最大绝对/相对偏差和首个失配坐标。

## 5. 峰值内存与拆分

受管峰值公式：

```text
output(m × p)
+ B_column_slice(col_block × p)
+ accumulator(row_block × p, 补偿策略乘 2)
+ row_order_workspace(max_block_nnz × 48 + row_block × 256 + p × itemsize × 8)
+ 4096 metadata
```

若逻辑方案超预算，按行块和列块在逻辑块内部继续拆分，枚举确定性候选尺寸，优先选择执行 tile 数最少、再选择较大有效 tile 的方案。诊断记录：

- 受管峰值、预算、输出/切片/累加器/行工作区明细；
- 逻辑 tile 数与执行 tile 数；
- `split_count = executed_tiles - logical_tiles`；
- 行/列 chunk 边界。

若即使 1×1 工作 tile 仍超预算，立即返回 `E_MEMORY_BUDGET`，附 breakdown、shortfall 和建议最小预算，不依赖运行时内存溢出。调用方持有的完整 B 是常驻输入，不重复计入受管临时峰值。

## 6. 版本与缓存协议

### 6.1 状态

`VersionStore` 目录包含：

- `state.json`：原子替换的完整版本状态，含记录数组、head、active scheme、state digest。
- `anchor.json`：高水位锚点，含已提交记录摘要前缀、head 和 state digest。
- `.lock`：跨进程排他锁。
- `cache/<cache_key>.npy`：结果缓存。

每个记录：

```text
sequence, kind, prev_digest, payload, digest
digest = SHA256(canonical(sequence, kind, prev_digest, payload))
```

记录从 1 连续递增，`prev_digest` 形成摘要链。`state_digest` 覆盖状态其余字段。写入流程在文件锁内加载、校验、追加、原子替换 JSON；缓存先原子写 `.npy` 再提交状态。

### 6.2 并发与整体替换

- 发布可带 `expected_sequence` 和 `expected_digest`。序号落后、重复或同序号摘要不同均返回 `E_STALE_PUBLISH`。
- 每次加载都比较 `anchor.json`：序号回退、已提交摘要链前缀分叉、同序号 head/state digest 变化均拒绝。
- 缓存发布必须匹配当前 active scheme 的 sequence 与 digest；跨方案错位返回 `E_STALE_PUBLISH`。
- 相同 active scheme 的 `ensure_scheme` 幂等；相同缓存 key 且文件摘要有效时返回已有版本。否则追加连续新版本。
- Windows 使用 `msvcrt.locking`，Unix 使用 `fcntl.flock`，均支持超时。

### 6.3 缓存键与失效

缓存键是规范化 JSON 的 SHA-256，包含：

```text
shape_fingerprint
structure_fingerprint (indptr + indices + shape)
sparse_value_fingerprint (data)
dense_fingerprint
dense_shape, dtype
block_scheme (row_block, col_block, scheme_id)
sum_strategy
accumulation_order
memory_limit_bytes
execution_schedule (effective row/col blocks and chunk boundaries)
```

任一变化产生新键。即使旧键文件仍存在，缓存查找还要求其版本记录属于当前 active scheme；方案变更后旧结果不会复用。缓存命中时仍重新读取显式参考并执行审计，缓存内容还要通过输出摘要校验。

## 7. 确定性

- 样本使用固定 PCG64 种子；Decimal 参考与 manifest 摘要固定。
- 分块候选排序有完整 tie-break，不含时间或进程相关量。
- 内核顺序遍历、稳定排序，无并行归约。
- 输出摘要对 little-endian C-order 原始字节和 dtype/shape 计算 SHA-256。
- 版本记录不含时间戳；干净工作区重复运行的版本序号与摘要一致。
