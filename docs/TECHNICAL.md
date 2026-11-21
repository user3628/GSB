# qpipe 技术说明

## 1. 模块边界

| 模块 | 职责 |
|---|---|
| `qpipe/samples.py` | 固定种子生成确定性 `.npz`、算子清单、形状声明和异常夹具 |
| `qpipe/config.py` | 版本化配置快照、阶段/流水线指纹、增量失效证据 |
| `qpipe/calibration.py` | 对称/非对称、逐张量/逐通道标定和量化 |
| `qpipe/forward.py` | 定点整数累加、溢出钳制计数、缩放链反量化 |
| `qpipe/attribution.py` | float64 参考前向、逐层误差、敏感度与扰动重放归因 |
| `qpipe/validation.py` | 层/端到端容差、越界样本位置和结构化错误报告 |
| `qpipe/checkpoints.py` | 原子检查点、发布账本、真实文件锁、不可变阶段缓存 |
| `qpipe/pipeline.py` | 四阶段编排、逐阶段执行、停止恢复、状态查询 |
| `qpipe/verifier.py` | 独立结构、摘要、归因和字节一致性验证 |
| `qpipe/http_api.py` | 本地回环 HTTP 与后台任务 |
| `qpipe/cli.py` | 整数退出码 CLI 与确定性演示 |

## 2. 输入格式

`operator_manifest.json` 的关键字段：

```json
{
  "schema_version": 1,
  "operator_chain": ["l0", "l1"],
  "layers": [
    {
      "id": "l0",
      "op": "linear",
      "weight": {
        "shard": "weights/shard_00.npz",
        "key": "weight",
        "quantization": {
          "bits": 8,
          "mode": "symmetric",
          "granularity": "per_channel",
          "channel_axis": 0
        }
      },
      "activation": {
        "shard": "calibration/shard_00.npz",
        "key": "activation",
        "quantization": {
          "bits": 8,
          "mode": "symmetric",
          "granularity": "per_tensor",
          "channel_axis": 0
        }
      },
      "accumulator_bits": 32,
      "tolerance": {"max_abs": 2.0, "max_rel": 0.35}
    }
  ],
  "end_to_end_tolerance": {"max_abs": 4.0, "max_rel": 0.25},
  "attribution_sum_tolerance": 1e-9
}
```

`shape_declarations.json` 为每层声明 `input_shape`、`weight_shape` 和 `output_shape`。导入时复制清单、形状声明和所有分片到不可变配置快照，并为每个文件记录 SHA-256。

线性算子使用行向量约定：`y = x @ W.T`。权重通道轴为输出行；支持任意合法权重通道轴用于标定，但固定整数累加前向要求权重与激活的通道轴都是可因式分解的 `0`，或使用逐张量参数。非前导激活通道轴会走显式 `integer_products_with_mixed_scale_reduction` 路径，避免把混合缩放伪装成单一整数累加。

## 3. 标定

对每个通道子集或整张量取观测极值 `min/max`。

对称模式：

```text
qmax = 2^(bits-1) - 1
qmin = -qmax
scale = max(abs(min), abs(max)) / qmax
zero_point = 0
```

非对称模式：

```text
qmin = 0
qmax = 2^bits - 1
denominator = max(qmax - qmin - 1, 1)
scale = max((max-min)/denominator, abs(min)/qmax, abs(max)/qmax)
zero_point = clip(ceil(-min/scale), qmin, qmax)
```

预留一个码位并考虑零点钳制，确保量化器可表示区间覆盖两端极值。量化使用 `round_half_up_then_clip`：

```text
q = clip(floor(x/scale + 0.5) + zero_point, qmin, qmax)
```

标定结果同时记录 `observed_min/max`、`representable_min/max`、`saturation_count` 和量化后极值。缩放因子为零、全零通道、非法位宽/模式/粒度/形状均抛出带 `layer` 与 `parameter` 的 `E_INVALID_PARAM`。

## 4. 定点前向与反量化

可因式分解路径：

```text
acc[b,o] = sum_k clip_signed_bits((xq[b,k]-zpx) * (wq[o,k]-zpw))
y[b,o]   = acc[b,o] * scale_w[o] * scale_x[b]
```

每个累加步骤都执行有符号 `accumulator_bits` 钳制。层诊断记录：

- `accumulator_bits`、模式、最小/最大值；
- `accumulator_sha256`、元素和、最大绝对值；
- `overflow_clamped` 与 `overflow_clamp_count`；
- 前 16 个钳制位置；
- 权重/激活饱和计数。

钳制结果不会被静默忽略：计数和位置随层发布，并参与阶段摘要。`l2` 演示故意使用 11 位累加器，产生 14 次显式溢出钳制。

## 5. 误差归因

float64 参考链为显式真值：

```text
x_ref[l+1] = x_ref[l] @ W_ref[l].T
```

实际量化链以反量化输出作为下一层输入。端到端偏差使用 L1：

```text
D = sum(abs(y_quant_final - y_ref_final))
```

对每层在参考输入上做局部分量化，得到 `delta_l`，并构造下游参考 Jacobian `G_l`：

```text
first_order_raw[l] = sum(abs(delta_l) @ abs(G_l))
replay_raw[l]      = sum(abs(delta_l @ G_l))
```

两种原始依据分别归一化到总和 `D`，浮点残差分配给原始贡献最大的层。阶段记录原始值、归一化贡献、占比、排名、两种 basis 和归一化细节。排名不同的层设置 `review_required=true`，但仍保留两种依据。

演示中 `l2/l3` 因敏感度界与带符号重放的排序互换被标记待复核。

## 6. 容差与错误

每层同时检查 `max_abs_error <= tolerance.max_abs` 与 `relative_l1_error <= tolerance.max_rel`。另检查端到端最大绝对偏差和相对 L1 偏差。

失败报告包含：

- 超界层、指标、观测值、限值和超界量；
- 超界样本索引与具体 `[sample, feature]` 位置；
- 距容差最近的层/指标及余量；
- 端到端容差；
- `E_TOLERANCE_EXCEEDED` 与最大偏差层。

NaN/Inf 在标定前全局扫描，定位首个非有限值并返回 `E_NUMERIC_INPUT`，不会进入量化或前向。

## 7. 检查点与发布

运行目录结构：

```text
.qpipe/
  configs/<config_id>/
  cache/<stage>/<fingerprint>/
  runs/<run_id>/
    checkpoints/00000001.json ...
    publish_ledger.json
    stage_layers/<stage>/00000001.json
    output/{result,error_report,layer_errors}.json
  state/active.json
  control/stop.request
  locks/publication.lock
```

检查点字段包括 `stage`、`layer`、`unit`、`input_content_fingerprint`、`config_version`、`artifact_digest`、`stage_fingerprint` 和全局单调 `sequence`。写入流程为：同目录临时文件 -> 写入 -> `fsync` -> `os.replace` -> 目录 `fsync`。

发布使用真实 OS 文件锁（POSIX `fcntl.flock`，Windows `msvcrt.locking`）。每阶段维护 `next_layer_seq`：

- 只接受期望序号；
- 只接受该序号对应的层 ID；
- 完成当前阶段前必须已连续发布全部层；
- 前置阶段未完成返回 `E_INVALID_STAGE`；
- 重复/落后/跳号/跨层返回 `E_STALE_CHECKPOINT`。

阶段缓存目录按 `stage + fingerprint` 内容寻址，只允许首次原子目录发布；已有不同摘要时拒绝覆盖。

## 8. 增量失效

阶段指纹链：

```text
calibration = H(算子链, 层量化配置, 输入分片摘要, 形状声明, 标定算法版本)
forward     = H(calibration, accumulator_bits, 定点实现版本)
attribution = H(forward, 归因算法版本, 归因求和容差)
validation  = H(attribution, 层容差, 端到端容差)
```

与上一次活动配置比较后，找到第一个指纹变化（或缓存缺失）的阶段。该阶段和全部下游重建；上游缓存继续复用。配置 diff 同时给出 `shard_content_changed`、`bitwidth_changed`、`quantization_mode_changed`、`granularity_changed`、`operator_chain_changed`、`accumulator_bits_changed`、`layer_tolerance_changed` 等可观测原因。

仅修改 `l3` 层容差时，实测只有 `validation` 重建，前三阶段均命中缓存。修改权重分片内容时，最早失效点回到 `calibration`。

## 9. HTTP 与并发

HTTP 服务只接受 `127.0.0.1`、`localhost` 或 `::1`。长任务通过后台线程执行，返回 job ID；查询 `/api/jobs/{id}` 获取状态。业务异常按其 HTTP 映射返回，后台任务同时保留稳定业务码。

`/api/publish-layer` 暴露底层发布探针，便于多进程验证锁与序号。4 进程同时发布同一层时，只有 1 个成功，其余返回 `E_STALE_CHECKPOINT`。

## 10. 确定性边界

- 同一 Python/NumPy 环境下，相同配置的标定、前向、归因、报告逐字节一致。
- 演示在 Python 3.11.9/NumPy 2.4.6 与 Python 3.14.4/NumPy 2.5.5 上得到相同摘要。
- 浮点归因使用固定顺序归约；不以时间戳、绝对运行路径或进程 ID 生成最终业务产物。
- 运行状态/HTTP 任务元数据可包含运行标识，但不进入最终三份业务 JSON。
