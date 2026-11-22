# 设计说明

## 1. 目标与边界

本组件解决四个相互耦合的问题：

1. 在纯 NumPy 环境中实现可复现的多尺度自顶向下融合。
2. 为每层权重提供一个可重放的端到端敏感度诊断。
3. 将阶段结果、量化误差和敏感性报告变为可恢复、可校验的本地状态。
4. 在配置变化时只失效必要的最早层及下游，并证明增量结果等于全量重建。

组件不负责训练、自动微分、GPU 加速或远程存储。横向卷积权重由清单参数和固定种子派生，确保示例无需额外二进制权重。

## 2. 层序与算子

清单中的 `layers` 按细分辨率到粗分辨率排列，融合按反向顺序执行。示例为：

```text
manifest order:  p3(finest) -> p4 -> p5(coarsest)
processing:       p5 -> p4 -> p3
```

每一层包含一个横向连接和一次通道注意力：

```text
L_i = ConvNCHW(X_i; W_i, b_i, kernel, stride, padding)
A_i = normalize(w_i)
H_i = L_i * A_i[c]
```

其中 `A_i[c]` 广播到 `HxW`。注意力长度必须等于该层 `lateral.out_channels`。

深层 `p5`：

```text
Y_5 = c_5,lateral * H_5
```

其余层：

```text
U_i = resize(Y_{i+1}, target=H_i.shape[2:], scale=2, mode=mode)
Y_i = c_i,lateral * H_i + c_i,topdown * U_i
```

最近邻使用声明的整数倍率映射 `floor(out_index / scale)`；双线性使用半像素坐标 `((out+0.5)/scale - 0.5)`，并夹到源图范围。若目标尺寸与 `source*scale` 的差在每个空间维不超过 `max_tail`，则尾部通过边缘复制或裁切显式处理；否则返回 `E_SHAPE_MISMATCH`。示例中 `p4 -> p3` 的 `16x20 -> 17x21` 正是尾行/尾列路径。

卷积权重由以下材料派生：

```text
sha256(task.seed | layer | kernel_seed | in_channels | out_channels
       | kernel_h x kernel_w | stride | padding)
```

再使用 PCG64 正态分布和 fan-in 缩放生成。材料、权重摘要和偏置摘要均进入阶段指纹。

## 3. 注意力与融合校验

支持归一化规则：

- `sum_to_one`：`w / sum(w)`
- `l1`：`w / sum(abs(w))`

即使原始和已经接近 1，报告也始终记录 `raw_sum`、`abs_sum` 和是否应用归一化。以下情况返回 `E_INVALID_PARAM`：

- 注意力向量长度与通道数不符：`index = actual_length`。
- 任一权重为 NaN/Inf：`index = first_invalid`。
- 归一化分母为零。
- 融合系数包含非有限值或全为零。

每层输出还会检查清单声明的形状和 `[low, high]` 区间。区间越界使用 `E_NUMERIC`，携带越界数量、首元素索引和观测范围。

## 4. 敏感性诊断

### 4.1 扰动模型

对一层 `i` 的原始注意力向量使用交替符号方向，对融合系数使用 `lateral:+1, topdown:-1` 方向，同时做加性扰动：

```text
w'_i = w_i + amplitude * attention_pattern_i
c'_i = c_i + amplitude * fusion_pattern_i
```

单层重放时只修改该层，其他层保持原值。全层重放时每层同时使用各自方向。

### 4.2 两个排序

端到端相对偏差：

```text
r_i = ||Y_i' - Y||_2 / max(||Y||_2, eps)
```

其中 `Y` 是最终 `p3` 输出。一阶估计使用中心差分：

```text
J_i   = (Y_i(+h) - Y_i(-h)) / (2h)
s_i   = ||J_i||_2 * amplitude / max(||Y||_2, eps)
```

`h` 为 `first_order_probe`。分别按 `r_i` 和 `s_i` 降序排序；同一层在两个排序中的名次不同则标记 `review_required=true`。

### 4.3 加性约束

报告同时给出：

```text
sum_single = sum_i r_i
all_layers = ||Y_all' - Y||_2 / ||Y||_2
gap        = |sum_single - all_layers|
```

若 `gap > tolerance`，运行返回 `E_NUMERIC`，原因是 `sensitivity_additivity_exceeds_tolerance`。这防止非线性交互较大时静默给出误导性排序。

## 5. 量化与误差界

对每层输出和最终输出使用对称定点量化：

```text
qmax     = 2^(bits-1) - 1
scale    = max(abs(x)) / qmax        # 或清单 scale_override
q        = clip(round(x/scale), -qmax, qmax)
dequant  = q * scale
```

逐元素误差界：

```text
bound = 0.5 * scale + 1e-12 * max(1, max_abs(x), max_abs(reference))
```

报告中同时记录：

- `max_quantization_error`：反量化相对主输出的误差峰值。
- `max_reference_error`：反量化相对高精度参考的误差峰值。
- `reference_error_within_bound` 与首个越界索引。
- `effective_bits = log2(max_abs / (0.5*scale) + 1)`。
- 饱和元素数、零码数量、参考摘要和误差数组摘要。

`scale` 为零或全部量化码饱和时返回 `E_NUMERIC`。参考不匹配不是量化算法错误，因此报告会标记而不阻断重建；这使权重变更后的增量测试可以继续，同时暴露“参考尚未同步更新”的诊断。

每一层会写出以下内容寻址数组：

```text
quantized.npy
dequantized.npy
error_bound.npy
abs_reference_error.npy
```

## 6. 阶段指纹

每个阶段的 `component_fingerprints` 包括：

- `input`：特征分片文件字节 SHA-256。
- `attention`：原始注意力配置规范化摘要。
- `fusion`：融合模式和系数摘要。
- `lateral`：横向参数、派生权重摘要和偏置摘要。
- `upsample`：倍率、模式、对齐方式。
- `quantization`：位宽和缩放覆盖。
- `reference`：高精度参考文件 SHA-256。
- `output_contract`：期望形状和允许区间。
- `config`：任务种子、完整层配置、算法版本和阶段 schema。

自指纹：

```text
own = sha256(component_fingerprints)
```

阶段版本包含自指纹和紧邻的上游阶段版本：

```text
stage_version = sha256({own, dependency_version})
```

因此 `p5` 变化会传递到 `p4`、`p3`；`p3` 自身变化只影响 `p3`。敏感性版本额外包含扰动幅度、容差、中心差分步长和所有阶段版本。

## 7. 检查点协议

### 7.1 写入顺序

每个阶段完成后：

1. 在内容寻址目录 `artifacts/<stage_version>` 的临时目录中写入全部文件并 `fsync`。
2. 原子重命名临时目录为最终目录。
3. 将记录写入 `checkpoints/.tmp-*`，`fsync`。
4. 原子 `os.replace` 为 `NNNNNNNN.json`。
5. 最后原子写 `HEAD.json`，记录最高序号和记录摘要。

如果在 1-4 之间崩溃，最终记录不存在，临时记录被扫描忽略；如果在 4-5 之间崩溃，记录已完整，恢复时自动修复 `HEAD`。

### 7.2 记录字段

```json
{
  "schema_version": 1,
  "task_id": "demo_fpn",
  "sequence": 2,
  "layer": "p4",
  "stage_index": 1,
  "stage_version": "...",
  "own_fingerprint": "...",
  "dependency_version": "...",
  "component_fingerprints": {"input": "...", "attention": "..."},
  "input_fingerprint": "...",
  "attention_fingerprint": "...",
  "config_version": "...",
  "artifact_summary": {"files": {"raw.npy": "...", "metadata.json": "..."}, "primary": "raw.npy"},
  "status": "committed",
  "previous_record_digest": "..."
}
```

记录自身摘要为记录对象规范化 JSON 的 SHA-256；下一条记录保存前一条摘要，形成链。

### 7.3 校验分类

- `normal`：序号从 1 连续、链摘要一致、HEAD 一致、所有产物摘要一致。
- `rollback`：HEAD 序号大于记录数、序号缺口或发布序号落后/跳跃。
- `tamper`：记录 JSON、HEAD、链摘要或产物摘要不一致。
- 支持 `HEAD` 滞后记录时的恢复；不把合法的崩溃窗口误判为篡改。

### 7.4 并发

所有扫描修复、发布和报告写入共享 `state/.lock`，使用 Linux `flock(2)`：

- 发布前在独占锁内重新扫描。
- 新记录序号必须是当前 HEAD + 1。
- 如果同层同 `stage_version` 已提交，则视为幂等复用，不重复追加。
- 严格 API 传入 `expected_sequence` 时，落后、重复或跳跃都返回 `E_STALE_CHECKPOINT`。
- 跨任务声明返回 `E_STALE_CHECKPOINT`；缺少前置阶段返回 `E_INVALID_STAGE`。

## 8. 停止与恢复

停止请求写入 `STOP.json`。引擎只在“开始下一个层”和“当前层提交后”检查：

```text
if stop_requested: return stopped
```

因此当前不可拆分层的计算与检查点提交一定完成。`resume` 清除停止文件并从当前阶段指纹扫描已提交记录。

恢复一致性依赖以下事实：

- 产物是确定性的 `float64` `.npy` 字节。
- 检查点记录按内容寻址，旧产物不会被覆盖。
- 敏感性从完整输出和输入重算，排序与 JSON 序列化稳定。
- 导出到 `outputs/` 的字节与内容寻址产物一致。

## 9. 增量失效与全量校验

增量流程：

1. 重新计算所有当前阶段版本。
2. 读取检查点链中每层最新记录。
3. `stage_version` 相同的阶段直接加载产物。
4. 最早不匹配阶段及其处理顺序下游重建。
5. 写入 `reports/incremental.json`，记录原因和指纹。
6. 若敏感性版本变化，重算排序。

`verify_incremental()` 不信任复用结果，直接从输入执行一次完整融合，然后逐字节比较：

- 每层 `raw.npy` 内容。
- 每层量化元数据。
- 最终量化报告。
- 敏感性报告。

不一致时返回 `E_INCREMENTAL_MISMATCH`，内容包括层名、首个差异元素索引、期望摘要与实际摘要。若攻击者同时修改产物、记录、HEAD 使检查点链保持自洽，这项校验仍能定位差异。

## 10. 输出约定

每个成功任务至少生成：

```text
outputs/<task>/p5.npy
outputs/<task>/p4.npy
outputs/<task>/p3.npy
outputs/<task>/final.npy
outputs/<task>/quantization/*.npy
outputs/<task>/reports/quantization.json
outputs/<task>/reports/sensitivity.json
state/tasks/<task>/reports/run.json
```

`final.npy` 等于最细层输出；其量化使用最细层声明的位宽，但作为独立条目记录参考摘要与误差数组。
