# 多尺度特征金字塔融合与敏感性诊断组件

这是一个纯本地、纯 NumPy 的可集成领域组件，面向检测头提供多尺度特征自上而下融合、通道注意力、定点量化误差分析、权重敏感性诊断、原子检查点、优雅停止/恢复、并发发布保护与增量失效重建。

组件不依赖 GPU、深度学习框架、容器或外部数据集。所有随机样本由固定种子生成，所有产物使用确定性序列化，因此同一配置和同一输入下可逐字节复现。

## 交付内容

- `src/multiscale_fusion/`：可安装的库、CLI 与本地 HTTP API。
- `examples/demo/`：固定种子样本、融合清单、标量高精度参考、完整运行产物与验证指标。
- `tests/`：42 项确定性测试，覆盖融合、量化、敏感性、检查点、并发、停止恢复和增量重建。
- `docs/DESIGN.md`：数学定义、指纹和检查点协议细节。
- `docs/VALIDATION.md`：实际执行结果与逐层误差数据。

## 快速开始

要求 Python 3.10+、NumPy 1.24+、pytest。

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'

# 运行全部测试
pytest -q

# 生成示例、导入工作区、完整融合并做增量对全量校验
msfp demo --dir examples/demo --force --verify-full
```

不安装包时也可直接运行：

```bash
PYTHONPATH=src python -m multiscale_fusion demo --dir /tmp/msfp-demo --force --verify-full
PYTHONPATH=src pytest -q
```

## 示例数据

示例包含三个 NCHW 层级，按“细分辨率到粗分辨率”声明，融合时反向执行 `p5 -> p4 -> p3`：

| 层 | 输入形状 | 输出形状 | 输入通道 | 输出通道 | 上采样 | 量化位宽 |
|---|---:|---:|---:|---:|---|---:|
| `p3` | `(2, 5, 17, 21)` | `(2, 7, 17, 21)` | 5 | 7 | bilinear x2 + 1 尾元素 | 10 |
| `p4` | `(2, 3, 8, 10)` | `(2, 7, 8, 10)` | 3 | 7 | nearest x2 | 12 |
| `p5` | `(2, 9, 4, 5)` | `(2, 7, 4, 5)` | 9 | 7 | 无 | 8 |

样本包含奇数通道、奇数空间尺寸、固定极端激活值（例如 `24000`、`-31000`）以及参考真值。`p4` 到 `p3` 的 `8x10 -> 16x20 -> 17x21` 路径显式覆盖奇数尺寸尾行/尾列对齐。

## 数据流

对每个处理层 `i`：

```text
lateral_i = conv2d_nchw(feature_i)
attention_i = normalize(attention_weights_i)
weighted_i = lateral_i * attention_i[channel]

deepest:
    fused_i = fusion.lateral * weighted_i

other:
    topdown_i = resize(fused_{i+1}, target=weighted_i.shape[2:])
    fused_i = fusion.lateral * weighted_i
            + fusion.topdown * topdown_i
```

`p5` 为最深/最粗层，`p3` 为最终融合输出。融合结果还会校验清单声明的 NCHW 形状和允许数值区间。

## 清单格式

完整示例见 `examples/demo/bundle/manifest.json`。核心字段：

```json
{
  "schema_version": 1,
  "tasks": [{
    "id": "demo_fpn",
    "seed": 20261123,
    "layers": [{
      "name": "p4",
      "input": "shards/p4.npy",
      "reference": "reference/p4.npy",
      "expected_output_shape": [2, 7, 8, 10],
      "allowed_range": [-20000.0, 20000.0],
      "lateral": {
        "kernel_size": [3, 5],
        "stride": [1, 1],
        "out_channels": 7,
        "padding": "same",
        "weight_scale": 0.68,
        "kernel_seed": 3102
      },
      "upsample": {
        "scale": 2,
        "mode": "nearest",
        "align_corners": false,
        "max_tail": 1
      },
      "attention": {
        "weights": [0.10, 0.20, 0.15, 0.10, 0.25, 0.10, 0.10],
        "normalization": "sum_to_one"
      },
      "fusion": {
        "mode": "weighted_sum",
        "coeffs": {"lateral": 0.62, "topdown": 0.38}
      },
      "quantization": {"bits": 12}
    }],
    "final_reference": "reference/final.npy",
    "sensitivity": {
      "amplitude": 0.001,
      "tolerance": 0.25,
      "first_order_probe": 0.00001
    }
  }]
}
```

横向卷积权重不依赖外部权重文件，而由 `task.seed + layer + kernel_seed + 形状` 派生；权重摘要会进入阶段指纹。若需要接入真实训练权重，可在后续版本增加权重导入字段而不改变阶段协议。

## 库集成

```python
from multiscale_fusion import FusionEngine
from multiscale_fusion.workspace import import_bundle

import_bundle("examples/demo/bundle", "my_workspace", force=True)
engine = FusionEngine("my_workspace")

# 完整运行；已有且未失效的阶段会复用
result = engine.run("demo_fpn")

# 查询状态和敏感性排序
status = engine.status("demo_fpn")
sensitivity = engine.sensitivity("demo_fpn")

# 权重/清单变化后再次运行：只重建最早受影响层及其下游
result2 = engine.run("demo_fpn")

# 将当前复用结果与从零全量重算逐字节比较
engine.verify_incremental("demo_fpn")
```

主要返回内容包括：

- 最终输出 SHA-256、逐层产物、检查点头序号。
- `invalidation`：最早受影响层/阶段、失效原因、复用层、当前阶段指纹。
- `sensitivity`：逐层重放偏差、一阶敏感度、两种排序、待复核层、加性误差。
- `quantization`：逐层和最终输出的缩放因子、误差界、实际参考误差、有效位数、饱和计数。

## CLI

```bash
# 生成样本、清单和标量高精度参考
msfp generate /tmp/bundle --seed 20261123 --force

# 导入本地分片和清单
msfp import --workspace /tmp/ws --bundle /tmp/bundle --force

# 分阶段执行到 p4
msfp stage --workspace /tmp/ws --task demo_fpn --layer p4

# 完整/增量执行
msfp run --workspace /tmp/ws --task demo_fpn --verify-full

# 请求优雅停止，随后恢复
msfp stop --workspace /tmp/ws --task demo_fpn
msfp resume --workspace /tmp/ws --task demo_fpn --verify-full

# 查询
msfp status --workspace /tmp/ws --task demo_fpn
msfp sensitivity --workspace /tmp/ws --task demo_fpn
msfp verify-checkpoints --workspace /tmp/ws --task demo_fpn
msfp verify-incremental --workspace /tmp/ws --task demo_fpn

# 本地 HTTP 服务
msfp serve --workspace /tmp/ws --host 127.0.0.1 --port 8765
```

`run --stage-delay SECONDS` 用于稳定复现“运行中收到停止请求”；`--crash-after p4` 模拟进程在 p4 检查点提交后强制终止，用于验证恢复只依赖已写记录。

## HTTP API

服务默认只绑定 `127.0.0.1`，不访问外部目标。

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 健康检查 |
| `POST` | `/v1/import` | 从本地 `bundle_dir` 导入清单和分片 |
| `POST` | `/v1/run` | 完整/增量融合 |
| `POST` | `/v1/stage` | 执行到指定层 |
| `POST` | `/v1/stop` | 写入优雅停止请求 |
| `POST` | `/v1/resume` | 清除停止请求并恢复 |
| `POST` | `/v1/verify-incremental` | 增量对全量逐字节校验 |
| `GET` | `/v1/status?task_id=...` | 状态查询 |
| `GET` | `/v1/sensitivity?task_id=...` | 敏感性查询 |
| `GET` | `/v1/checkpoints?task_id=...` | 检查点链校验 |
| `GET` | `/v1/invalidation?task_id=...` | 增量失效原因 |
| `GET` | `/v1/quantization?task_id=...` | 逐层误差报告 |

示例：

```bash
curl -s -X POST http://127.0.0.1:8765/v1/import \
  -H 'Content-Type: application/json' \
  -d '{"bundle_dir":"/abs/path/examples/demo/bundle","force":true}'

curl -s -X POST http://127.0.0.1:8765/v1/run \
  -H 'Content-Type: application/json' \
  -d '{"task_id":"demo_fpn","verify_full":true}'
```

## 确定性检查点

工作区结构：

```text
workspace/
  input/manifest.json
  input/shards/*.npy
  input/reference/*.npy
  state/.lock                              # flock(2) 文件锁
  state/tasks/<task>/checkpoints/NNNNNNNN.json
  state/tasks/<task>/artifacts/<stage_version>/
  state/tasks/<task>/HEAD.json
  state/tasks/<task>/STOP.json
  state/tasks/<task>/reports/*.json
  outputs/<task>/*.npy
```

每个阶段记录包含任务标识、层名、输入分片指纹、注意力指纹、配置版本、阶段版本、产物摘要、前序记录摘要和单调序号。记录先写临时文件、`fsync`，再 `os.replace`，最后原子更新 `HEAD`。产物目录按 `stage_version` 内容寻址，历史记录不会被新产物覆盖。

校验会区分：

- `normal`：序号连续、链摘要一致、HEAD 一致、产物摘要一致。
- `rollback`：HEAD 领先记录、序号缺口或回退。
- `tamper`：记录、链摘要、HEAD 或产物摘要不匹配。

真实文件锁保证多工作进程只会产生一条连续序号链；重复、落后、跳跃或跨任务发布返回 `E_STALE_CHECKPOINT`，缺失前置阶段返回 `E_INVALID_STAGE`。

## 增量失效

阶段指纹由以下内容构成：

- 特征分片字节摘要。
- 注意力原始权重向量。
- 融合系数。
- 横向卷积参数及派生权重摘要。
- 上采样配置。
- 量化位宽/缩放覆盖。
- 高精度参考分片、输出形状和允许区间。
- 算法版本与上游阶段版本。

变化后，最早受影响阶段及其依赖链上的全部下游阶段重建；未受影响阶段直接复用内容寻址产物。扰动幅度只影响敏感性版本，因此不会重建张量，但会使敏感性排序失效并重算。`state/tasks/<task>/reports/incremental.json` 和 `/v1/invalidation` 可查询原因、最早层和当前指纹。

`verify_incremental()` 会从输入重新执行一次完整融合，并将逐层原始张量、量化元数据、量化报告和敏感性报告逐字节比较。不一致时返回 `E_INCREMENTAL_MISMATCH`，携带层名、首个差异元素索引和期望/实际摘要。

## 敏感性定义

对每层同时注入固定方向的注意力权重和融合系数扰动，重放完整融合链，计算最终输出相对 L2 偏差：

```text
relative_deviation = ||y_perturbed - y_baseline||_2 / ||y_baseline||_2
```

一阶估计使用同一方向上的中心差分：

```text
first_order = ||(y(+h) - y(-h)) / (2h)||_2 * amplitude / ||y_baseline||_2
```

两种排序不一致的层标记 `review_required=true`。报告还比较“逐层偏差之和”与“全层同时扰动偏差”；差值必须不超过清单容差，否则返回数值诊断错误而不是静默通过。

## 量化

每层和最终输出使用对称定点量化：

```text
qmax  = 2^(bits-1) - 1
scale = max_abs(output) / qmax
q     = clip(round(x / scale), -qmax, qmax)
```

输出记录显式 `scale`、每元素误差界 `0.5*scale + 数值参考松弛`、参考分片摘要、实际逐元素参考误差、有效位数、饱和计数和零码数量。`scale <= 0` 或全部量化码饱和返回 `E_NUMERIC`。

若权重变化后没有同步更新高精度参考，运行仍会完成，但报告会设置 `reference_error_within_bound=false` 并给出 `first_reference_mismatch_index`，便于区分“量化本身”与“参考已过期”。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_SHAPE_MISMATCH` | 声明形状、卷积/重排形状或参考形状不一致，附层名与期望/实际形状 |
| `E_INVALID_PARAM` | 参数非法，附层名、索引、字段和原因 |
| `E_NUMERIC` | 非有限值、允许区间越界、零缩放、全饱和或敏感性加性超差 |
| `E_STALE_CHECKPOINT` | 检查点回退、篡改、跨任务/重复/跳跃发布 |
| `E_INVALID_STAGE` | 清单/前置阶段/产物不可用 |
| `E_INCREMENTAL_MISMATCH` | 增量复用结果与全量重算不一致，附层名和差异位置 |
| `E_LOCK_TIMEOUT` | 等待工作区文件锁超时 |

所有异常继承 `FusionError`，`to_dict()` 可被 CLI/HTTP 直接序列化。

## 验证结果

当前环境执行 `PYTHONPATH=src pytest -q`：**42 passed**。

示例完整运行的最终 `p3/final` SHA-256 为：

```text
23370c4feccabbfc40d357685bd805707d46b3b0b34f38866f4e8de1da6e7ead
```

逐层主算子与独立标量参考的最大绝对误差约 `4.55e-13`，相对 L2 误差低于 `1.8e-16`。敏感性排序为 `p3 > p4 > p5`，一阶排序一致，待复核层为空；逐层偏差和为 `0.00758853`，全层同时扰动偏差为 `0.00533771`，差值 `0.00225082`，低于容差 `0.25`。更多数据见 `docs/VALIDATION.md` 和 `examples/demo/validation.json`。

## 设计文档

- `docs/DESIGN.md`：算法、阶段协议、恢复与并发细节。
- `docs/VALIDATION.md`：测试与示例验证记录。
