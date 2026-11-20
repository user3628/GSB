# 技术说明

## 1. 不变量

1. 参数校验先于输出分配。形状、批次、转置和 dtype 失败时没有可返回的部分结果。
2. 每个寄存器微块拥有独立 `MR x NR` 累加器；跨缓存块、跨 K 面板、跨批次都不复用累加器。
3. BMM 的批次是最外层循环，批次内调用与 GEMM 相同的 `gemm_2d_into`。
4. 自校验失败时输出只在内部存在，API 抛出 `E_NUMERIC_MISMATCH` 而不返回它。
5. 只有通过完整配置链校验且序号连续推进的记录才能被采纳。
6. 缓存键包含形状指纹、分块、求和策略和输入摘要；配置版本不匹配必须失效重建。
7. 内存预算在执行前规划，超预算不得冒险分配。

## 2. 分块内核

逻辑形状先统一为：

```text
A_logical: M x K
B_logical: K x N
C:         M x N
```

转置只改变视图：

```text
A T: (K, M) -> swap last two axes
B T: (N, K) -> swap last two axes
```

分块循环：

```text
for cache_i in range(0, M, MC):
  for cache_j in range(0, N, NC):
    for reg_i in cache_i .. cache_i+MC step MR:
      for reg_j in cache_j .. cache_j+NC step NR:
        acc = zeros(MR_actual, NR_actual)
        partials = []
        for k0 in range(0, K, KC):
          partials.append(A[reg_i, k0:k1] @ B[k0:k1, reg_j])
        C[reg_i, reg_j] = sum(partials, strategy)
```

`scalar` 模式的 `partials` 是每个 K 项的向量外积；`panel` 模式是每个 K 面板的 BLAS 结果。`scalar` 默认用于很小的矩阵，使策略差异直接作用于全部 K 项；`panel` 用于可扩展执行。两种模式都报告在 `KernelInfo.accumulation_mode`。

边界块使用实际长度，不做隐式补零。因此任意正 M/N/K 都合法。

## 3. 求和策略与误差界

设浮点单位舍入 `u = eps/2`，某输出元素的绝对项和为 `S = sum_k |a_ik*b_kj|`。

- 顺序求和：`|error| <= gamma_K S`，`gamma_n = n*u/(1-n*u)`。
- 二次幂树：深度约为 `ceil(log2 K)+1`，使用对应深度的 `gamma`。
- 补偿求和：报告 `2uS + 2u^2 K S`。

实现逐元素计算 `S` 的样本来自高精度基准函数。`theoretical_abs_bound_max` 是抽样中的最大界，`theoretical_bound_violations` 统计实际偏差超过界的样本。理论界是保守上界，极小样本可能远小于它。

二次幂参考与内核不使用相同代码路径：

```text
reference(rows, k0, k1):
  if interval size is 1: return outer product
  p = largest power of two <= size (for exact powers use size/2)
  return reference(rows, k0, k0+p) + reference(rows, k0+p, k1)
```

高精度基准对每个抽样元素执行：

```text
products = float64(a_ik) * float64(b_kj)
reference = math.fsum(products)
```

非有限输入不进入 `fsum` 的有限求和语义，而是按顺序执行 IEEE 加法，使 NaN/Inf 可见传播。结果摘要统计 NaN、正 Inf、负 Inf 的总数、抽样位置和按行列块的计数。

## 4. 自动分块评分

候选生成范围由硬上限、L1 寄存器工作区和 L2 缓存工作区过滤：

```text
register_workspace = (MR*KC + KC*NR + MR*NR) * itemsize
cache_workspace    = (MC*KC + KC*NC + MC*NC) * itemsize
```

对每个候选计算：

```text
m_tiles, n_tiles, k_tiles
microkernel_calls = ceil(M/MR)*ceil(N/NR)*ceil(K/KC)
traffic           = K*M*ceil(N/NC) + K*N*ceil(M/MC) + M*N
edge_waste        = padded_product/(M*N*K) - 1
parallel_tiles    = ceil(M/MC)*ceil(N/NC)
```

访存、调用次数和缓存压力使用 `log1p` 归一化（越低越好）；并行维度用“并行块 / 可用工作进程”截断到 1；边界浪费越低越好。工作负载按 `M*N*K*batch` 分为 small/medium/large 并使用不同权重。最终分数是五者的加权和，记录在 `PlanSelection`。选择器不承诺启发式结果一定是实际最快方案，`plan_benchmark.json` 专门记录实测差异。

显式方案必须满足：

```text
MR, NR, MC, KC, NC > 0
MR <= 32, NR <= 64, MC <= 2048, KC <= 1024, NC <= 4096
MC % MR == 0
NC % NR == 0
```

## 5. 内存调度

```text
base            = output_bytes + cache_workspace + register_workspace + fixed
per_batch       = (M*K + K*N) * itemsize
full_batch_peak = base + batch*per_batch
slice_size      = max(1, floor((budget-base)/per_batch))
shard_peak      = base + shard_batches*per_batch
```

调用方原已持有的完整输入不重复计入；`per_batch` 为可能物化的批次操作数切片。输出缓冲按完整形状计入。每个分片记录 `estimated_peak_bytes` 与 `budget_remaining_bytes`。若 `base+per_batch > budget`，不分配输出，直接返回 `E_MEMORY_BUDGET` 和分解明细。

## 6. 配置存储格式

```text
config_store/
  records/000000000001.json
  records/000000000002.json
  HEAD.json
  .lock
../.config_store.anchor.json
```

单条记录：

```json
{
  "schema_version": 1,
  "seq": 2,
  "created_utc": "...",
  "prev_digest": "sha256(previous record without record_digest)",
  "payload": {
    "fingerprint": "shape fingerprint",
    "plan": {"mr": 16, "nr": 32, "mc": 256, "kc": 64, "nc": 256},
    "sum_strategy": "pairwise",
    "cache_budget": {"l1_bytes": 32768, "l2_bytes": 1048576},
    "selection_source": "large",
    "metadata": {}
  },
  "payload_digest": "sha256(payload)",
  "record_digest": "sha256(record without record_digest)"
}
```

校验顺序：JSON 结构 -> 文件名/序号 -> 载荷摘要 -> 记录摘要 -> 前序链 -> HEAD/外部锚 -> 调用方最小序号/指纹。分类码：

```text
正常：ConfigState.status = "valid"
回退：E_STALE_CONFIG / rollback
篡改：E_STALE_CONFIG / tampered_record 或 tampered_store 等
发布冲突：E_STALE_PUBLISH / sequence_conflict 或 duplicate_sequence
```

外部锚只能防止“只替换配置目录”的旧快照回放；若攻击者同时控制外部锚所在目录，需要更高层可信存储。这是本项目的明确安全边界。

## 7. 结果缓存格式

缓存键：

```text
sha256({
  schema: 1,
  shape_fingerprint,
  plan: {mr,nr,mc,kc,nc,signature},
  sum_strategy,
  accumulation_mode,
  input_digest: sha256(dtype + shape + contiguous bytes of A and B)
})
```

条目同时保存 `config_seq`、`cache_seq`、NPY 结果 SHA-256、元数据摘要和计算结果报告。索引 `index.json` 维护全局单调发布序号。查找时：

1. 无条目：`miss`。
2. 条目的 `config_seq != current_config_seq`：删除条目和结果文件，返回 `invalidated/config_version_mismatch`。
3. 元数据摘要或 NPY 文件摘要不匹配：删除并返回 `invalidated`。
4. 形状或 dtype 不匹配：删除并返回 `invalidated`。
5. 全部通过：`hit`。

发布在文件锁内读取当前序号，只允许 `current+1`；重复键、指定旧序号或跳过序号返回 `E_STALE_PUBLISH`。所有索引、条目和配置记录使用原子替换写入。

## 8. 并发与原子性

Windows 锁：`msvcrt.locking(fd, LK_NBLCK, 1)` 锁住锁文件第一字节。POSIX 锁：`fcntl.flock(LOCK_EX|LOCK_NB)`。锁文件与数据文件分离，避免 Windows 无法替换被锁文件。超时返回 `E_STALE_PUBLISH/lock_timeout`。

每次文件写入流程：

```text
mkstemp in target directory -> write -> flush -> fsync -> os.replace
```

POSIX 上额外 `fsync` 目标目录。配置写入顺序为 record -> HEAD -> 外部 anchor；中途崩溃会留下可检测的不一致状态，而不会静默采纳半版本。

## 9. 测试策略

- 数值：转置/非方阵、三种策略、BMM 与独立 GEMM 的 `array_equal`。
- 异常：K 冲突、批次冲突、GEMM/BMM 误用、NaN/Inf 传播。
- 自校验：零容差强制触发 `E_NUMERIC_MISMATCH`，检查行块/列区间。
- 分块：小规模不过度切块、大规模满足 L2、显式整除/上限/边界块。
- 配置：正常推进、指纹不匹配、整库回退、记录篡改、过期发布。
- 缓存：命中、版本失效、文件篡改、重复/落后发布。
- 并发：多进程自动续号、精确序号竞争只允许一个成功、缓存并发发布连续。
- 内存：切片逐元素一致、预算明细、恢复一致性。

