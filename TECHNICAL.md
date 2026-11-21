# 技术说明

## 1. 组件划分

| 模块 | 职责 |
|---|---|
| `chunkops/ops.py` | 分块 Softmax/LayerNorm、显式求和、非有限值扫描、内存调度 |
| `strategies.py` | 自动候选生成、访存/误差/缓存评分、方案与淘汰记录 |
| `validation.py` | 逐向量 `math.fsum` 朴素参考、抽样块比较、每输出块误差界 |
| `data.py` | 固定种子 `.npz`、异常注入、形状/范围清单 |
| `storage.py` | 原子写入、`fcntl.flock`、检查点、版本链、结果缓存 |
| `pipeline.py` | 四阶段执行、停止/恢复、摘要与错误导出 |
| `http_server.py` / `cli.py` | 本地接口 |
| `demo.py` | 三档、双轴、异常、恢复、HTTP/CLI 验收演示 |

## 2. 归约核心

### 2.1 轴统一

物理轴为 1 时直接处理 `x`；物理轴为 0 时对 `x.T` / `out.T` 视图执行同一套“沿有效矩阵列归约”的核心。输出始终写回原形状，因此不存在额外转置副本。

### 2.2 Softmax

对每个输出行块：

1. 按归约列块扫描，维护 `m`（running max）与 `l`（shifted exp sum）。
2. 块内先做 `block - max(block, running_max)`，再在指数域求和。
3. 合并公式：
   `l_new = l_old * exp(m_old - m_new) + sum(exp(block - m_new))`
4. 第二个扫描将 `exp(x - m) / l` 写入输出块。

全程不使用概率连乘；极大值（如 ±1e4）不会直接触发 `exp` 溢出。NaN/Inf 由 NumPy IEEE 语义传播。

### 2.3 LayerNorm

对每个输出行块按归约列块执行在线 Welford 合并：

- 块均值使用所选求和策略。
- 块二阶矩使用 `sum((block - block_mean)^2)`。
- 合并：
  - `delta = mean_b - mean_a`
  - `mean = mean_a + delta * n_b / n`
  - `M2 = M2_a + M2_b + delta^2 * n_a * n_b / n`
- 输出：`(x - mean) / sqrt(M2/n + eps) * gamma + beta`。

`gamma`/`beta` 长度必须匹配被归一化轴长度。

### 2.4 求和策略

- `sequential`：严格按归约索引从左到右更新。
- `pairwise`：相邻配对树形归约，奇数末项按明确顺序进位。
- `compensated`：Neumaier 补偿求和，同时保留“大数 + 小数”分支丢失的低阶项。

三种策略都只依赖显式归约顺序，因此相同输入、方案、内存预算产生相同字节输出。

## 3. 自动分块

候选行块/列块以 `sqrt(dimension)` 为尺度中心，扩展为 1/4、1/2、1x、2x、4x，并加入规模上限候选。评分分量：

- 预测访存量：两次输入扫描 + 输出写入 + 归约块累加器读写 + 块启动开销。
- 计算代价分量：三种求和策略的向量化/补偿开销。
- 解析误差上界：`gamma(strategy, reduction_length) * magnitude * anomaly_factor + output quantization`。
- 缓存适配惩罚：工作区超过 cache budget 时显著加罚。
- 规模上限惩罚：小张量禁止使用大张量专用块。
- 异常稳定惩罚：异常比例提高时降低补偿策略惩罚。

小、中、大基础 Softmax 的典型选择：

| 规模 | 行块 | 列块 | 策略 |
|---|---:|---:|---|
| 128 x 512 | 32 | 64 | sequential |
| 2048 x 1024 | 128 | 256 | pairwise |
| 8192 x 4096 | 64 | 1024 | pairwise |

异常分片会切换为 `compensated`。每个被淘汰候选都记录分数及原因，如 `higher_weighted_score`、`row_block_exceeds_scale_cap`、`workspace_exceeds_cache_budget`。

## 4. 误差控制与校验

每个输出块生成一个误差估计：

- 记录归约块顺序 `reduction_order`。
- 使用固定种子随机抽样归约位置 `sample_positions`。
- 用抽样位置的幅值缩放一阶误差项。
- 依据实际求和策略计算 `gamma`。
- 输出 dtype 量化项单独计入。
- 抽样含非有限值时估计界记为 `Infinity`。

校验阶段只对确定性抽样输出块使用逐向量 `math.fsum` 朴素定义参考，返回：

- 块行列区间。
- 最大绝对偏差、最大相对偏差。
- NaN 对 NaN 的传播匹配数。
- 对应误差估计索引与估计界。
- `bound_covers_observed`。

若超出 `atol/rtol`，返回 `E_NUMERIC_MISMATCH`，详情包含失败块区间与偏差。

## 5. 内存调度

内存估计显式包含：

- 完整输入缓冲。
- 完整输出缓冲。
- 输入/指数或中心化工作块。
- 输出工作块。
- 在线累加器/`M2`。
- 分配器与元数据余量。

若工作区导致峰值超预算，按物理行块或列块持续对半拆分，记录每一步拆分前后块大小与字节估计。每步调度也记录 `estimated_peak_bytes`。若输入 + 输出 + 最小工作区仍超预算，返回带 `input_bytes`、`output_bytes`、`minimum_workspace_bytes`、`required_bytes` 的 `E_MEMORY_BUDGET`，不通过捕获 OOM 处理。

## 6. 版本、锁与缓存

### 6.1 检查点

`checkpoints/000N_stage.json` 包含：

- 单调阶段序号。
- `prev_digest`。
- 阶段 payload。
- 整个记录的 SHA256 digest。

恢复时只接受连续、摘要和前置摘要均匹配的链；损坏点之后从最后一致阶段重算。

### 6.2 版本链

`versioned/<kind>/records/<seq>.json` 保存完整 payload 与摘要；`HEAD.json` 指向最新记录。外部守卫 `versioned/.guards/<kind>.json` 独立保存：

- `max_seq`
- `head_digest`
- `payload_digest`
- `shape_fingerprint`
- `scope`

因此仅替换 `versioned/plans` 目录时：

- HEAD 序号小于守卫序号：`rollback`
- 摘要不一致：`tamper`
- 形状指纹不一致：`shape_fingerprint_changed`
- 正常 `seq+1`：接受

均通过 `E_STALE_VERSION` 的 `stale_reason` 区分。

### 6.3 并发发布

发布在 `fcntl.flock` 排他锁内比较 `expected_seq`：

- 必须是当前序号 + 1。
- 落后、重复、越级均返回 `E_STALE_PUBLISH`。
- 同形状但不同算子/轴 scope 返回 `E_STALE_PUBLISH`。
- 缓存同 key 不同内容也返回 `E_STALE_PUBLISH`。

### 6.4 结果缓存

缓存键至少包含：

- shape fingerprint
- value fingerprint
- op / axis
- sum strategy
- row block / col block
- cache budget
- output dtype / eps / gamma / beta 指纹
- memory budget 与实际生效块大小

读取时校验元数据、形状和数组 SHA256；任何不一致视为 miss，绝不返回过期数组。

## 7. 四阶段管线

1. `plan`：加载并校验分片指纹，自动/用户方案评分，静态内存调度，发布 plan 版本，写检查点。
2. `reduce`：查结果缓存；miss 时执行分块算子；原子保存 `.npy`；发布 result 版本；写检查点。
3. `validate`：验证输出文件摘要、抽样参考、误差报告；失败写报告并返回 `E_NUMERIC_MISMATCH`；成功写报告与检查点。
4. `summary`：汇总阶段链、方案理由/淘汰项、内存调度、缓存键、非有限值、误差报告与版本指纹；写 `summary.json` 和最终检查点。

每个阶段完成后才写检查点。停止请求是持久化 `stop.json`；运行在输出块边界或阶段边界观察它。恢复清除停止标记，从最后一致检查点继续。缓存命中或重算都保持确定性输出。

## 8. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/api/datasets/generate` | 生成固定种子样本 |
| GET | `/api/fragments/manifest?root=data` | 形状/范围清单 |
| POST | `/api/fragments/import` | 导入并校验分片 |
| POST | `/api/runs/stage` | 执行下一阶段 |
| POST | `/api/runs/run` | 执行到指定阶段/步数，支持 stop-after |
| POST | `/api/runs/stop` | 持久化优雅停止请求 |
| POST | `/api/runs/resume` | 清除停止请求并恢复 |
| GET | `/api/runs/status` | 阶段状态、检查点、版本指纹、停止状态 |
| GET | `/api/runs/plan` | 方案、评分、淘汰原因 |
| GET | `/api/runs/checkpoints` | 检查点链 |
| GET | `/api/runs/versions` | 版本链与守卫状态 |
| GET | `/api/runs/errors` | 误差报告 |
| GET | `/api/runs/summary` | 摘要 |

HTTP 仅绑定回环地址；JSON 中非有限数值统一编码为字符串 `"NaN"` / `"Infinity"` / `"-Infinity"`。

## 9. 本次实测

- Python 3.14.4，NumPy 2.5.5。
- `python -m pytest`：60 passed。
- 大规模 8192 x 4096：
  - Softmax：约 6.7 秒。
  - 列方向 LayerNorm：约 5.5 秒。
- 大规模峰值静态估计：
  - Softmax：271,336,448 bytes。
  - LayerNorm：271,862,784 bytes。
- 全量/恢复输出 SHA256 相同。
- 6 进程并发发布只有 1 个成功推进。
- 完整证据见 `artifacts/verification.json` 与 `artifacts/verification.md`。
