# Convolution / Interpolation Workbench

一个完全本地运行的 NHWC 卷积与插值算子工作台。核心计算只使用 NumPy，状态持久化、文件锁和 HTTP 服务只使用 Python 标准库；不依赖 GPU、模型、容器或外部服务。

工作台把一个分片的算子清单拆成逐阶段执行链。每完成一个阶段就原子发布检查点，进程在阶段边界优雅停止后可以从最近一致检查点继续。恢复运行与一次完整运行的最终 `.npy`、形状轨迹、检查点日志逐字节一致。

## 功能概览

- 2D/1D 卷积：NHWC、任意正卷积核、步长、`same`/`valid` 填充、膨胀率、零填充、输出空间分块和输出通道分块。
- 插值：最近邻、双线性、双三次；任意缩放因子（实现限定为 `[1/64, 64]` 的有限正数）；`half_pixel`/`align_corners`；全程 float64 坐标计算与边界裁剪。
- 全链预校验：非法算子名、参数缺失、通道/形状不兼容会在执行任何阶段前返回 `E_INVALID_STAGE`，并带上分片和算子位置。
- 期望声明核对：每阶段输出形状和数值区间与 `expectations.json` 核对，不符返回 `E_SHAPE_MISMATCH` 和块位置。
- 原子检查点：产物先临时写、`fsync`、`os.replace`，再原子替换连续 JSONL 日志；损坏、半写、重复或乱序记录不会进入有效前缀。
- 优雅停止：HTTP/CLI 停止请求、`SIGINT`/`SIGTERM` 都只设置停止意图；当前不可拆分阶段完成后退出并写停止报告。
- 版本失效传播：张量内容、卷积/插值参数、对齐模式或 `config_version` 变化时，定位最早失效阶段，删除该阶段及下游检查点并重建；上游继续复用。
- 并发安全发布：每个分片链使用跨平台标准库文件锁；非连续、落后或重复发布返回 `E_STALE_CHECKPOINT`。
- 交互方式：完整 CLI、本地 HTTP JSON API 和浏览器面板。

## 环境与启动

项目目标环境是 **Python 3.11 + Windows 本地虚拟环境**。文件锁在 Windows 使用 `msvcrt.locking`，在 POSIX 使用 `fcntl.flock`；原子替换使用 `os.replace`。

Windows PowerShell 示例：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

生成或重建确定性演示数据：

```powershell
python -m conv_workbench init --root demo_workspace --force
```

运行全部三个分片的完整算子链：

```powershell
python -m conv_workbench --workspace demo_workspace --run-dir runs\demo run
```

`demo_workspace` 已随仓库提供，无需先生成也可直接运行。所有命令也可以把 `python -m conv_workbench` 换成安装后的 `conv-workbench`。

## 演示数据

固定随机种子为 `20261120`，输入均为 float64、C 连续、NHWC 的 `.npy` 分片。

| 分片 | 输入形状 | 算子链 | 形状轨迹 |
| --- | --- | --- | --- |
| `small_odd_pad` | `[2, 5, 6, 3]` | conv2d(same, stride `[1,2]`) → bilinear(up, half_pixel) | `[2,5,3,5]` → `[2,8,4,5]` |
| `medium_odd_dilated` | `[1, 9, 11, 5]` | conv2d(valid, dilation `[1,2]`) → bicubic(down, align_corners) → conv1d(same, stride 2) | `[1,4,7,4]` → `[1,2,3,4]` → `[1,2,2,3]` |
| `boundary_chain` | `[1, 6, 5, 1]` | conv1d(same, dilation 2) → nearest(up) → conv2d(same, dilation 2) → bicubic(down) | `[1,6,3,3]` → `[1,12,6,3]` → `[1,12,6,2]` → `[1,6,3,2]` |

覆盖奇数通道、小/中尺寸、1D/2D、same/valid、可见边界值、上采样与下采样、三种插值和两种坐标模式。

`demo_workspace/invalid_cases/` 另附可注入的错误样例：

- `invalid_conv_missing_weights.json`：卷积缺少 `weights`。
- `invalid_scale_out_of_bounds.json`：缩放因子 `129` 越界。

## 清单与期望声明

`manifest.json` 顶层字段：

```json
{
  "format_version": 1,
  "config_version": "demo-v1-20261120",
  "shards": [
    {
      "id": "small_odd_pad",
      "tensor": "tensors/small_odd_pad.npy",
      "operators": [
        {
          "name": "conv2d",
          "params": {
            "weights": [[[[0.1]]]],
            "bias": [0.0],
            "stride": [1, 1],
            "padding": "same",
            "dilation": [1, 1]
          }
        }
      ]
    }
  ]
}
```

卷积权重形状：

- `conv2d`: `[kh, kw, cin, cout]`
- `conv1d`: `[kernel, cin, cout]`，沿 NHWC 的 `W` 轴卷积，`H` 保持不变

1D 卷积还接受 `kernel_size` 声明，但必须与权重第一维一致。`bias` 缺省为零向量。`same` 的总填充量按
`max((ceil(size/stride)-1)*stride + (kernel-1)*dilation + 1 - size, 0)` 计算，左侧取一半、右侧取余数。

插值参数：

```json
{
  "name": "interpolate",
  "params": {
    "mode": "bicubic",
    "scale": [0.65, 0.5],
    "align_corners": true
  }
}
```

插值输出尺寸为 `floor(input_size * scale)`；若任一维小于 1 或 scale 不在 `[1/64, 64]`，返回 `E_INVALID_STAGE`。坐标公式：

- `align_corners=true`: `source = i * (in - 1) / (out - 1)`，`out == 1` 时取 0。
- `align_corners=false`（half_pixel）: 使用实际尺寸比 `ratio = in/out`，`source = (i + 0.5) * ratio - 0.5`。
- 所有坐标先裁剪到 `[0, in-1]`；三次插值使用 `a=-0.75`，邻域索引再裁剪，绝不回绕。

`expectations.json` 按分片和阶段声明真值观测区间：

```json
{
  "format_version": 1,
  "shards": {
    "small_odd_pad": {
      "stages": [
        {
          "operator_index": 0,
          "operator_name": "conv2d",
          "shape": [2, 5, 3, 5],
          "range": [-3.5, 3.0],
          "tolerance": 1e-6
        }
      ]
    }
  }
}
```

## 计算确定性

- 卷积按输出行、输出列和输出通道分块；越界位置由 `np.pad(..., mode="constant")` 零填充。
- 卷积的输入通道归约使用固定顺序的逐通道累加，不依赖 BLAS 的分块策略；更换输出块或通道块不会改变发布字节。
- 插值和插值邻域计算固定使用 float64，产物用 `np.save(..., allow_pickle=False)` 序列化；同一输入和参数重复计算结果逐字节相同。
- 产物摘要、阶段前缀指纹均对规范化 JSON 或产物字节计算 SHA-256。

## 检查点与恢复

每个分片有独立的单调检查点序列。阶段 `i` 的检查点序号为 `i+1`，因此有效前缀始终是 `1..k`、无跳号、无重复。

运行目录布局：

```text
run_dir/
  checkpoints/<shard>/checkpoints.jsonl
  checkpoints/<shard>/invalidations.json
  artifacts/<shard>/stage-0001-<digest>.npy
  outputs/<shard>.npy
  outputs/trace.json
  reports/run.json
  reports/stop.json
  locks/<shard>.lock
  STOP_REQUEST.json            # 仅在存在停止请求时
```

检查点记录包含：

```text
operator_index, checkpoint_seq, op_name, op_fingerprint,
input_fingerprint, config_version, chain_prefix,
artifact_digest, artifact_path, shape, dtype,
previous_checkpoint_sha256, checkpoint_sha256
```

发布顺序：

1. 在分片锁内重新加载连续检查点前缀。
2. 要求新阶段索引等于当前前缀长度，否则返回 `E_STALE_CHECKPOINT`。
3. 临时写阶段 `.npy`，`fsync` 后 `os.replace` 到最终路径。
4. 重新生成完整 JSONL 日志的临时文件，`fsync` 后原子替换。

因此任意写入点中断最多留下未被日志引用的临时/孤立产物，不会留下半写检查点。恢复时只接受校验和正确、前序哈希连接正确、产物摘要匹配且序号连续的记录；损坏、不完整、重复或跳号记录被忽略。若缺失立即前序产物，下游计算返回 `E_INVALID_STAGE`。

## 停止与恢复

停止是“阶段边界”语义：停止请求到达时，当前阶段继续计算和发布，下一阶段开始前退出。

```powershell
# 另开一个终端，或调用 HTTP POST /api/stop
python -m conv_workbench --run-dir runs\demo stop --reason "boundary check"

# 恢复
python -m conv_workbench --workspace demo_workspace --run-dir runs\demo resume
```

演示和测试也可使用确定性的 `--stop-after N`。`reports/stop.json` 记录停止原因、最近检查点序号、已完成/待处理分片和原始停止请求；强制终止不承诺生成该报告。

## 版本变化与失效传播

阶段前缀指纹链为：

```text
prefix[0] = H(config_version, shard_id, tensor_fingerprint, op_fingerprint[0])
prefix[i] = H(prefix[i-1], op_fingerprint[i])
```

`op_fingerprint` 覆盖算子名和规范化参数，包括权重摘要、步长、填充、膨胀、插值模式、缩放与对齐模式。运行开始时逐阶段比较当前前缀与活动检查点：

- 张量内容变化：阶段 0 及全部下游失效，原因 `source_tensor_changed`。
- 某阶段卷积参数变化：该阶段及下游失效，原因 `convolution_parameters_changed`。
- 插值模式/缩放变化：原因 `interpolation_parameters_changed`。
- 对齐模式变化：原因 `alignment_mode_changed`。
- 上游失效导致后续输入摘要变化：原因 `upstream_invalidated`。
- `config_version` 变化：全部阶段失效。

失效事件写入 `invalidations.json`，包含最早失效阶段、受影响阶段、原因、旧/新算子指纹、旧/新链路前缀、旧/新输入指纹和旧产物摘要。通过 `status` 和 `fingerprints` 命令可查询；失效及其下游产物不会被当作当前结果复用。

## 并发发布

同一分片的发布由 `<run_dir>/locks/<shard>.lock` 串行化。发布者必须提供下一个连续序号且输入摘要必须等于当前上游产物摘要；落后或重复发布返回稳定业务码 `E_STALE_CHECKPOINT`（HTTP 409）。不同分片拥有独立检查点流，可并行推进。

## CLI

```text
python -m conv_workbench [--workspace DIR] [--run-dir DIR] <command>

init                生成确定性演示工作区
self-check          全链预校验，不执行
list-shards         列出分片
run                 运行/继续完整算子链
resume              从最近一致检查点恢复（run 的别名语义）
stage               请求单个阶段；缺失前序产物返回 E_INVALID_STAGE
status              查看阶段状态、检查点、失效记录
fingerprints        查看规划指纹和活动指纹
stop                写入优雅停止请求
export              导出最终张量和 trace.json
import-shard        复制本地 .npy 并追加算子链
serve               启动本地 HTTP + 浏览器应用
```

常用选项：`--shard ID`、`--stop-after N`、`--delay-between-stages SEC`、`--block-h`、`--block-w`、`--out-channel-block`。

## HTTP API 与面板

```powershell
python -m conv_workbench --workspace demo_workspace --run-dir runs\web serve --host 127.0.0.1 --port 8766
```

浏览器打开 `http://127.0.0.1:8766/`。本次环境检查时 `8766` 可用；若占用可改端口或使用 `--port 0` 让系统分配并查看启动输出。服务仅绑定回环地址。

| 方法 | 路径 | 功能 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查、工作区、运行目录和端口 |
| GET | `/api/shards` | 分片清单 |
| POST | `/api/import` | 导入 `.npy` 与算子链，失败时回滚 |
| GET | `/api/status?shard=...` | 阶段状态与失效记录 |
| GET | `/api/fingerprints?shard=...` | 规划/活动指纹 |
| POST | `/api/run` | 完整运行，可传 `shard_ids`、`stop_after` |
| POST | `/api/resume` | 恢复运行 |
| POST | `/api/stage` | 请求单阶段 |
| POST | `/api/stop` | 写优雅停止请求 |
| POST | `/api/export` | 导出结果 |
| GET | `/api/trace` | 形状轨迹和阶段摘要 |
| GET | `/api/outputs/<shard>` | 下载最终 `.npy` |

## 业务错误码

| 错误码 | 含义 | 关键上下文 |
| --- | --- | --- |
| `E_INVALID_STAGE` | 非法算子、参数缺失、形状不兼容、缺失前序检查点、链路未完成 | `shard_id`, `operator_index`, `reason` |
| `E_SHAPE_MISMATCH` | 输出形状或数值区间不符合声明 | `shard_id`, `operator_index`, `block`, `output_index` |
| `E_STALE_CHECKPOINT` | 并发发布落后、重复或输入不是当前上游产物 | 提供的/期望的序号和输入摘要 |
| `E_INVALID_REQUEST` | HTTP/CLI 请求格式或状态不允许 | 请求字段 |
| `E_NOT_FOUND` | 分片、端点或产物不存在 | 路径/分片 |

绝不会因插值坐标越界而回绕；越界只裁剪。非法链在预校验失败时不会执行或发布任何阶段。

## 测试与验收

```powershell
python -m pytest
python scripts\demo_validation.py
```

测试覆盖：

- 1D/2D 卷积与朴素循环参考对比，覆盖 same/valid、stride、dilation、奇数通道、边界零填充。
- 改变输出块/输出通道块后卷积发布字节不变。
- 三种插值 × 两种对齐模式 × 上/下采样与标量参考对比，边界裁剪和重复运行字节一致。
- 全量运行、阶段停止、外部停止请求、恢复逐字节比对。
- 参数/张量变化的最早阶段失效、上游复用、下游摘要变化。
- 损坏、半写、重复检查点忽略；并发发布陈旧码。
- 形状/区间错误、非法卷积、越界缩放、缺失前序、部分链路导出。
- HTTP 健康、运行、状态、指纹、导出、下载、错误码和导入回滚；CLI 全流程。

本机实测：CPython 3.11.9、NumPy 2.4.6、pytest 9.2.0，`40 passed`。完整实测数据与固定摘要见 `VALIDATION.md`。

> 备注：`.npy` 文件的字节表示依赖 NumPy 版本。仓库记录的摘要由上述环境生成；验收比较在同一环境下完成。

