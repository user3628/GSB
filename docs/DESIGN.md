# 设计说明

## 1. 总览

每个材质是不可拆分的工作单元，内部顺序推进四个阶段：

1. `control`：描述词规范化、分词、固定哈希嵌入、32 维单位向量。
2. `noise`：确定性值噪声逐八度合成、振幅归一化、固定顺序归约。
3. `palette`：控制调制、双线性重采样、最近 RGB 调色、RGBA8 量化、材质参数映射。
4. `diagnostics`：Decimal 高精度参考、跨阶段误差传播、预算判定、归因交叉校验。

阶段产物先写入内容寻址摘要，再以“pending 事务 → 哈希链账本 → 原子检查点”的协议提交。恢复时只补完已进入账本但尚未替换到目标路径的完整记录；其余中断只会留下无引用的临时/暂存文件，不会形成半写 JSON。

## 2. 描述词与控制向量

### 规范化

处理顺序为 `Unicode NFKC`、Unicode 空白折叠、首尾裁剪、`str.lower()`。NFKC 同时完成全角/半角统一。分词使用 Unicode 字母/数字规则 `[^\W_]+`，因此中英混排无需词典。

空描述或没有字母/数字 token 时返回：

```json
{"error": {"code": "E_EMPTY_PROMPT", "material_id": "..."}}
```

### 固定哈希嵌入

每个 token 的整数权重为 `出现次数 × Unicode 字符数`。对每个 token、每个维度计算：

```text
SHA256("matgen-control-v1" 0x1f hash_seed 0x1f token 0x1f dimension)
u = first_64_bits / 2^64
signed = 2u - 1
```

累加 `weight × signed` 后做 L2 单位化。参考路径用 Decimal(60) 重做哈希、加权、平方和与平方根，输出：

- `control_vector`：32 个 IEEE-754 float64。
- `control_vector_bytes_sha256`：32 个小端 float64 打包后的摘要。
- `reference_vector_decimal`、`error_bound`、`relative_error_bound`、`significant_digits`。

同一配方、配置和实现版本产生相同字节。

## 3. 值噪声与八度归约

### 格点值

每个格点使用 SplitMix64：

```text
key = seed XOR ((octave+1)·G) XOR (x·Kx) XOR (y·Ky)
value = splitmix64(key) >> 11
value_f64 = value / 2^53
```

格点值是二进有理数，进入 float64 和 Decimal 参考时来源一致。

### 插值

像素中心归一化到 `[0,1]`，再乘八度频率。使用 smoothstep：

```text
s(t) = t²(3 - 2t)
```

在 x、y 方向做双线性插值。最右/最下边界使用实数值频率坐标的自然分数部分，因此边缘仍执行同一插值公式，而不是产生未定义索引。

### 振幅归一化与线程确定性

```text
a_norm[i] = a_raw[i] / sum(a_raw)
field = sum_i a_norm[i] · octave_i
```

每个八度独立计算，写入固定结果槽；随后始终按八度索引顺序执行 float64 加法。线程只并行“单层计算”，不参与归约，因此单线程和多线程逐元素完全相等。

### 重采样

基础噪声场用角点对齐双线性插值重采样到目标分辨率。参考路径用 Decimal 精确表示源坐标、分数权重和插值，而实际路径使用 float64，从而可度量重采样自身的浮点误差。

## 4. 调色、量化与材质参数

控制向量派生：

```text
energy = mean(|v|)
tint = clip(base_color + 0.04·v[0:3], 0, 1)
mod = clip(0.5 + (gray-0.5)·(0.75+0.35·energy) + 0.10·v[0], 0, 1)
target_rgb = tint · (0.30 + 0.70·mod)
```

目标 RGB 与排序后的色卡唯一颜色做最近欧氏距离匹配，距离并列时取排序后的第一个索引，保证确定性。输出 `RGBA8`，alpha 固定 255。粗糙度、金属度按控制向量分量和能量映射并裁剪到 `[0,1]`。

色卡错误：

- 空分片、显式空卡、未知分片、零颜色、非法量化通道：`E_PALETTE_INVALID`。
- PNG 高度/宽度、分片 box、`declared_size` 不一致：`E_DIM_MISMATCH`。

## 5. 误差模型

### 5.1 逐八度累计

设第 `i` 个八度在高精度参考中的贡献为 `r_i`，实际 float64 贡献为 `f_i`，归一化振幅为 `a_i`。对每个固定随机抽样位置 `p`：

```text
Δ_i = |f_i(p) - r_i(p)|
R_i(p) = EPS · (|F_i(p)| + |f_i(p)| + 1) · 8
B_i(p) = max(B_{i-1}(p) + Δ_i + R_i(p),
             |F_i(p) - R_i(p)| + 8·EPS)
```

其中 `F_i` 是 float64 累计值，`R_i` 是 Decimal(70) 累计参考，`EPS = 2^-52`。`B_i` 是第 i 层结束时的保守上界，并与该样本的实际误差取最大，避免声明的界小于已观测误差。

每层记录：

- `log_contribution = log10(mean(|r_i|))`，即该八度在抽样点上的对数绝对贡献。
- `log_amplitude_contribution = log2(a_norm)`。
- `contribution_bound = max(B_i - B_{i-1})`。
- `accumulation_error_bound = max(B_i)`。
- `relative_error_bound = B_i / max(|R_i|, tiny)`。
- 每个抽样的 `float_value`、`reference_decimal`、`absolute_error`、`error_bound`。
- `effective_digits = clamp(floor(-log10(2·B)), 0, 15)`。

### 5.2 重采样、调色和量化

- 重采样：把基础场累计界按插值线性敏感度传播，并与目标抽样位置的实际 Decimal 误差取最大。
- 调色：传播灰度界和控向量界；若 float 路径与 Decimal 参考选出不同调色板索引，上界额外加入两色最大通道差。
- 量化：加入不可避免的 `0.5/255` 半 LSB。
- 材质参数：逐项记录值、Decimal 参考、实际误差、误差上界和有效位数；纹理均值使用整数通道总和的高精度除法作参考。

出现 NaN、Inf、负误差界或缺失参考时抛出 `E_NUMERIC`。适用时附带零基 `octave_index`。

### 5.3 预算与归因

逐八度占用率：

```text
occupancy_i = contribution_bound_i / octave_error_budget
```

报告超预算八度、最接近预算层的余量、端到端使用量、总预算占用和超预算标志。端到端使用量为目标灰度界与量化界之和，包含不可避免的 8 位半 LSB。

归因有两种独立排序：

1. 误差贡献界排序；
2. 对每个八度施加与贡献界相关的 `+Δ` 扰动，重放调色和量化后的颜色变化排序。

排序不一致的八度标记 `review_required=true`，并给出 `deviation_source`，通常是非线性最近邻调色或 8 位量化平台抑制了微小扰动。

## 6. 检查点与原子提交

每个记录都包含：

- `stage` / `stage_index`
- 原料内容指纹 `recipe_fingerprint`
- 阶段输入指纹 `stage_input_fingerprint`
- 配置版本 `config_version`
- 材料级连续 `sequence`、阶段级 `stage_revision` 与工作区全局 `global_sequence`
- 上游记录摘要
- 产物文件列表、大小、SHA-256 与聚合摘要
- 记录自身 SHA-256

原子性依赖同目录临时文件、`fsync`、`os.replace` 和目录 `fsync`（POSIX）。JSON 规范编码无时间戳、无绝对路径。

`pending` 事务保证“账本已提交、目标记录尚未替换”时可在恢复阶段补完完整记录。账本为逐行哈希链：每行包含前一行摘要，外部锚点保存账本摘要、条目数和最后全局序号。整个 `.matgen` 被旧快照替换时，外部 `.matgen.anchor.json` 会识别回退。

发布流程在跨进程排他文件锁内执行：

1. 校验当前账本、锚点和已有记录。
2. 校验前置阶段、上游记录摘要和材料身份。
3. 材料级序号必须等于该材料已有提交最大序号 + 1；阶段修订号必须等于该阶段已有提交数 + 1。
4. 写产物、pending、账本、原子检查点，删除 pending。

分类结果：

- `normal_advance`：序号连续、指纹一致、摘要一致。
- `rollback`：记录缺失、序号后退或快照整体回退。
- `tamper`：指纹、摘要、记录摘要或账本哈希链不一致。

仅 `normal_advance` 被接受；其余返回 `E_STALE_CHECKPOINT`。

## 7. 增量失效

阶段输入指纹仅覆盖该阶段实际依赖：

- 控制：描述词、材料 id、哈希种子、控制配置。
- 噪声：噪声配方、基础/目标分辨率、控制记录摘要、噪声配置。
- 调色：基础色、粗糙度、金属度、分辨率、色卡来源摘要、控制/噪声记录摘要。
- 诊断：控制/噪声/调色记录摘要、预算和抽样配置。

因此修改预算只失效诊断；修改基础色只失效调色与诊断；修改描述词从控制阶段开始失效。失效通过账本事件先记录，再删除该阶段及下游产物；中断后的删除会在完整性校验中完成。

## 8. 停止与确定性

停止请求在材质边界检查。当前材质四阶段全部完成并生成输出后才退出，因此不会留下半个材质单元。恢复复用完整检查点，最终输出和完整运行一致。

任何持久化 JSON 都使用排序键、紧凑分隔符、UTF-8 和 `allow_nan=False`。纹理使用固定行主序 RGBA8；噪声使用固定小端 float64。报告按 `material_id` 排序，`run_digest` 只由输出相关配置、阶段记录摘要和输出文件摘要组成，不包含线程数、时间或路径。

