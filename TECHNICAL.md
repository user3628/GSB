# AudioQ 技术说明

## 1. 模块与数据流

```text
WAV + manifest ──> frame ──> metrics ──> fusion ──> diagnostics ──> report
                      │          │           │            │
                      └──── per-shard atomic checkpoints ───┘
```

核心模块：

- `audioq/models.py`：统一语义模型、v1/v2 编解码、降级与回补。
- `audioq/audio.py`：WAV 校验、Hann 分帧、SNR/频谱失真/削顶指标。
- `audioq/fusion.py`：权重归一、对数/归一化融合、Decimal 高精度参考与误差界。
- `audioq/checkpoints.py`：文件锁、原子写、序号连续性、损坏/重复隔离。
- `audioq/pipeline.py`：四阶段执行、指纹链、复用/失效、停止恢复、报告。
- `audioq/web.py` + `audioq/templates/dashboard.html`：本地 HTTP 与诊断界面。
- `audioq/cli.py`：命令行工作台。
- `audioq/demo.py`：固定种子样本、完整/停止恢复/错误注入演示。

## 2. 统一内部模型与配置互转

`EvaluationConfig` 保存语义配置及其结构版本；`EvaluationModel` 保存一次分片评估所需的 `shard_id`、样本数、采样率、三项指标得分、融合分、结构版本与版本指纹。内部权重统一为和为 1 的浮点数。

### v2

```json
{
  "schema_version": 2,
  "evaluation_id": "demo-quality-v2",
  "frame": {"length": 400, "hop": 160, "window": "hann"},
  "metrics": {"snr_threshold_db": 8.0, "spectral_epsilon": 1e-8, "spectral_scale_db": 20.0,
              "clipping_level": 0.98, "clipping_ratio_limit": 0.001, "clipping_scale_ratio": 0.08},
  "weights": {"snr": 0.4, "spectral": 0.35, "clipping": 0.25},
  "fusion": {"domain": "log", "epsilon": 1e-6, "decimals": 12, "high_precision": 50},
  "default_reference": {"min_mos": 0.0, "max_mos": 5.0, "target_mos": 4.0}
}
```

### v1 兼容规则

- 权重字段为 `weights_percent`，值必须是整数百分比。
- 导入策略可选 `normalize`（默认，按百分比总和归一）、`strict_100`、`divide_100`。
- 缺少 `snr_threshold_db` 时，使用 `fixed`（默认 8 dB，可配置）或 `weight_scaled`：`4 + 10 * normalized_snr_weight`。
- 导出 v1 时通过最大余数法生成总和为 100 的整数百分比。
- 导出结果显式包含 `downgrade_items` 与 `downgrade_restore`；缺失门限与可能损失的权重精度都可回补。回补后的内部语义字段与原模型一致。

## 3. 分帧与指标

帧起点为 `0, hop, 2*hop, ...`，只保留完整帧。窗口为 `numpy.hanning(frame_length)`，PCM 先转为 `int16 / 32768.0`。

- 帧能量：`sum((frame * hann)^2)`。
- SNR 估计：以帧能量 90 分位的信号能量减去 10 分位噪声底，计算 `10*log10(signal/noise)`，限制在 `[-20, 60] dB`；同时输出逐帧 SNR。
- SNR 得分：`clip(2.5 + 0.25*(snr_db - snr_threshold_db), 0, 5)`。
- 频谱失真：对每帧归一化幅度谱与活跃帧中位谱计算对数谱距离，并叠加频谱平坦度惩罚：`20*log10(1 + 100*flatness)`。
- 频谱得分：`clip(5*exp(-distortion_db / spectral_scale_db), 0, 5)`。
- 削顶：分别输出逐帧削顶比例与分片样本削顶比例；超过 `clipping_ratio_limit` 产生 `E_CLIPPED_AUDIO`，但保留指标和融合结果供诊断。
- 得分：`clip(5*(1 - clipping_ratio/clipping_scale_ratio), 0, 5)`。

空音频、全静音、无完整帧或帧长大于样本数均返回 `E_EMPTY_AUDIO`，不输出伪造的零分。所有浮点结果按配置位数舍入，JSON 使用排序键和固定分隔符，因此同输入同配置逐字节一致。

## 4. 融合、分级与精度

可用指标集合为 `A`，配置权重为 `w_i`。先按 `sum(w_i, i in A)` 重归一化；被跳过指标及其原权重显式记录。

- `log` 域：`exp(sum(normalized_w_i * ln(max(score_i, epsilon))))`。
- `normalized` 域：`sum(normalized_w_i * score_i)`。

输出包含配置权重、重归一化权重、对数贡献、加权贡献、跳过原因以及是否发生重归一化。全部指标不可用或可用权重和为零时返回 `E_INVALID_WEIGHT`。

分级规则：低于参考区间下界为 `poor`；高于上界为 `excellent`；区间内达到 `target_mos` 为 `excellent`，否则为 `good`。越界时输出绝对偏差与有符号偏差。

融合使用 Python `Decimal` 在 `high_precision` 位精度下重新计算参考值。`error_bound` 是“序列化融合分与实际高精度参考值之差”的实测绝对值，不使用缺失信息推断。报告同时给出配置小数位、有效数字和高精度参考值。

## 5. 链式版本指纹与增量失效

每阶段指纹为：

```text
frame_fp       = H({algorithm, implementation_sha256, source_sha256, manifest metadata, frame params})
metrics_fp     = H({algorithm, implementation_sha256, frame_fp, metric params, score decimals})
fusion_fp      = H({algorithm, implementation_sha256, metrics_fp, weights, reference, fusion params, schema_version})
diagnostics_fp = H({algorithm, implementation_sha256, fusion_fp, language, order, evaluation_id, report schema})
```

其中 `implementation_sha256` 来自对应实现源文件内容，避免代码已变化但旧检查点被误复用。

因此：

- 样本内容或帧参数变化：最早失效阶段为 `frame`。
- SNR 门限等指标参数变化：`metrics` 及其下游失效。
- 权重、参考区间、融合参数或结构版本变化：`fusion` 及其下游失效。
- 语言、顺序或评估 ID 变化：`diagnostics` 失效。

状态查询同时返回当前失效事件与最近一次持久化失效记录。未通过当前指纹校验时，`export-report` 与报告导出接口拒绝读取旧诊断，返回 `E_STALE_CHECKPOINT`。改回旧配置不会幂等返回历史下游检查点，而是生成新的序号与产物。

## 6. 原子检查点、锁与恢复

每个分片拥有独立单调序号，记录结构如下：

```json
{
  "record_version": 1,
  "seq": 3,
  "shard_id": "clean",
  "stage": "fusion",
  "input_fingerprint": "sha256:...",
  "config_fingerprint": "sha256:...",
  "artifact_digest": "...",
  "artifact": {"...": "..."},
  "version_components": {"...": "..."},
  "prerequisites": {"frame": "...", "metrics": "..."},
  "record_digest": "..."
}
```

发布协议：

1. 获取进程间 `fcntl.flock` 与进程内线程锁；异常和正常路径均在 `finally` 释放。
2. 扫描该分片全部记录，校验文件名、分片、阶段、产物摘要、记录摘要和换行完整性。
3. 损坏、重复序号、断档之后的记录移入 `quarantine/`，不参与恢复。
4. 校验全部前置阶段产物摘要；缺失返回 `E_INVALID_STAGE`，被更新版本超越返回 `E_STALE_CHECKPOINT`。
5. 只允许 `seq = last_consistent_seq + 1`；重复当前阶段指纹、指定过期序号或跨分片产物均返回 `E_STALE_CHECKPOINT`。
6. 写临时文件、`fsync`、`os.replace`、再 `fsync` 目录。任意时刻中断最多留下不参与扫描的临时文件。

运行循环仅在分片边界检查停止请求；当前分片四阶段都完成后才退出。恢复时忽略损坏/重复/断档记录并从最近一致前缀继续。报告不含时间戳、绝对路径或复用计数，因此完整运行与中断恢复后的诊断逐字节一致。

## 7. 稳定错误码

| 错误码 | 触发条件 |
|---|---|
| `E_EMPTY_AUDIO` | 空文件、全静音、无完整帧、帧长大于样本数 |
| `E_CLIPPED_AUDIO` | 削顶比例超过配置限制（保留指标与融合供诊断） |
| `E_LENGTH_MISMATCH` | 清单样本数与 WAV 实际样本数不同 |
| `E_SAMPLE_RATE` | 清单采样率与 WAV 实际采样率不同 |
| `E_NUMERIC` | 指标/融合分出现 NaN、Inf 或越界数值 |
| `E_INVALID_WEIGHT` | 权重非法、权重和为零、全部指标不可用 |
| `E_STALE_CHECKPOINT` | 序号落后/重复、前置产物被超越、跨分片错位发布 |
| `E_INVALID_STAGE` | 缺失前置阶段或未知阶段 |
| `E_AUDIO_FORMAT` | 非单声道 16-bit PCM WAV |
| `E_CONFIG` / `E_MANIFEST` / `E_IO` | 配置、清单或本地 I/O 错误 |

所有错误对象包含 `ok:false`、`code`、`message`，并在可得时附带 `shard_id`、`stage` 和 `observed`。

## 8. 实际验证记录

环境：Python 3.14.4、NumPy 2.2.1、Flask 3.1.3、pytest 8.3.4（代码使用 Python 3.10+ 语法）。

```text
$ pytest -q
37 passed in 3.00s
```

最终演示：

```text
acceptance_ok: true
full report sha256: fba5d1be55d01791aef551ae6ca02cea38ae24b76f3543cfafc54cf57b40ec8f
full report fingerprint: sha256:b32ebdafc82db40e2e6d0617be85e46ce9b2f001d763cba1b47057f87391afa6
stop/resume byte_identical: true
clean MOS-like score: 4.441443508803 (excellent)
low SNR score: 1.865633734388 (good, inside declared low-quality reference range)
clipped score: 0.095817737322 (poor, E_CLIPPED_AUDIO)
silent score: null (E_EMPTY_AUDIO + E_INVALID_WEIGHT)
v1 -> v1 roundtrip semantic equality: true
v1 -> v2 semantic equality: true
invalid-weight check: E_INVALID_WEIGHT
length-mismatch check: E_LENGTH_MISMATCH
```

完整机器可读结果见 `demo/demo_summary.json` 与 `artifacts/verification.json`。

## 9. 范围与限制

- 指标是本地确定性信号处理启发式，不宣称替代主观 MOS 实验。
- 文件在单次评估期间被假定为不可变；若运行中内容变化，下一次版本链会检测并重建。
- 单进程 CLI 顺序执行分片；多个进程可安全发布同一工作区的检查点，版本冲突以结构化错误暴露。
- 强制 `SIGKILL` 不保证生成退出报告；已发布的原子检查点仍可恢复。优雅停止只在分片边界生效。
