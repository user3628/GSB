# ToneLab 技术设计

## 1. 设计目标

ToneLab 把三种声音数据外部形态收敛为一个内部模型，并围绕该模型提供确定性、
可复用、可审计的分阶段处理。核心约束是：

- 输入是 16 kHz、单声道、16-bit PCM WAV 分片。
- 不使用真实克隆模型、外部语音服务、GPU、容器或非本地依赖。
- 同一输入与配置产生逐字节一致的阶段产物。
- 版本 1 与版本 2 可导入，升级后可导出并稳定往返。
- 增量构建必须与独立从零全量构建逐字节一致。

## 2. 模块边界

| 模块 | 职责 |
|---|---|
| `tonelab/models.py` | `Project/Shard/PronUnit` 统一模型、PCM base64、内部 JSON |
| `tonelab/config.py` | 默认配置、深合并覆盖、配置版本 |
| `tonelab/audio.py` | WAV 读写信封、采样率/声道/位深保持与错配诊断 |
| `tonelab/formats.py` | v2/v1 清单、声音工程 JSON、WAV 侧车、单元标注 |
| `tonelab/validation.py` | 清单总量、时间线、音高/共振峰数值校验 |
| `tonelab/features.py` | 分帧、Hann 窗、频谱包络、谐波结构统计 |
| `tonelab/modeling.py` | 单元权重、音高插值、F0 与共振峰轨迹合成 |
| `tonelab/quantization.py` | 定点量化、位宽约束、误差界 |
| `tonelab/fingerprints.py` | 阶段输入指纹、失效起点与版本指纹 |
| `tonelab/checkpoints.py` | 原子检查点、文件锁、单调序号、停止标志 |
| `tonelab/engine.py` | 构建、复用、增量/全量比对、编辑、往返、查询 |
| `tonelab/workspace.py` | 工作区持久化与导入操作 |
| `tonelab/cli.py` / `tonelab/server.py` | CLI 与本地 HTTP/浏览器入口 |

## 3. 内部模型

```text
Project
  project_id, description, sample_rate, channels, bit_depth
  declared_total_samples
  shards[]
    Shard
      shard_id, timbre_id, sample_rate, channels, bit_depth
      int16 pcm[], pcm_sha256
      norm_pitch_curve[(time 0..1, value)]
      units[]
        unit_id, symbol, start_ms, end_ms
        formants_hz[F1..Fn], formant_ranges_hz[[low,high]...]
      structure_version, source_structure_version
      origin, quantization
```

内部工程 JSON 使用 `format=tonelab.internal-project`，PCM 编码为
`int16le-base64`。项目级导出 `format=tonelab.sound-project` 也支持相同 PCM 编码，
并可保留来源版本与量化元数据。

## 4. 外部形态契约

### 4.1 声音工程 JSON v2

```json
{
  "format": "tonelab.sound-project",
  "structure_version": 2,
  "audio": {"sample_rate": 16000, "channels": 1, "bit_depth": 16},
  "declared_total_samples": 78240,
  "shards": [{
    "shard_id": "shard_001",
    "timbre_id": "speaker_aurora",
    "sample_count": 19360,
    "source_structure_version": 2,
    "pcm_encoding": "int16le-base64",
    "pcm_b64": "...",
    "pitch_curve": [[0.0, 0.97], [0.25, 1.0]],
    "units": [{
      "unit_id": "shard_001:u0000",
      "symbol": "你",
      "start_ms": 0.0,
      "end_ms": 330.0,
      "formants_hz": [420.0, 2100.0, 2900.0],
      "formant_ranges_hz": [[386.4, 453.6], [1932.0, 2268.0], [2668.0, 3132.0]]
    }]
  }]
}
```

导入也接受 `wav` 路径引用。路径相对清单所在目录解析。

### 4.2 WAV + 音高侧车

WAV 只保存 PCM。侧车命名为 `<shard>.pitch.json`：

```json
{
  "format": "tonelab.wav-sidecar",
  "sidecar_version": 1,
  "shard": {
    "shard_id": "shard_001",
    "sample_count": 19360,
    "sample_rate": 16000,
    "channels": 1,
    "bit_depth": 16,
    "pitch_curve": [[0.0, 0.97]],
    "units": [],
    "wav": "shard_001.wav"
  }
}
```

WAV 导出后立即读回并校验 `PCM_16/16000/mono`。导入时采样率、声道或位深不符
返回 `E_FORMAT_MISMATCH`，`declared` 与 `actual` 均完整给出。

### 4.3 单元标注

`<shard>.units.json` 只携带 `unit_id/symbol/start_ms/end_ms`。重新导入时按
`unit_id` 优先、随后符号匹配已有单元以继承共振峰目标；未知单元使用缺省规则。

### 4.4 版本 1 清单

```json
{
  "format": "tonelab.manifest.v1",
  "structure_version": 1,
  "declared_total_samples": 20480,
  "shards": [{
    "wav": "shard_004_legacy.wav",
    "timbre_id": "speaker_aurora",
    "reference_f0_mean": 178.0,
    "sample_count": 20480,
    "pitch_offsets_percent": [-2, 0, 3, 1, -1],
    "units": "旧|版|legacy|停顿"
  }]
}
```

配置项：

- `v1_compat.pitch_percent_mode`：`reference_ratio`、`cents`、`absolute_offset_hz`。
- `v1_compat.pitch_percent_scale`：百分比换算比例。
- `v1_compat.unit_separator`：整串单元分隔符。
- `v1_compat.stable_id_prefix/stable_id_length`：稳定 ID 形态。
- `v1_compat.default_formants`：按 `vowel/front_vowel/consonant/pause/fallback` 补齐。

`reference_ratio` 的曲线值为 `1 + percent * scale`；绝对 F0 为
`reference_f0_mean * normalized_value`。`cents` 模式下每个百分点映射为
`scale * 100` cents。

## 5. 分帧与特征

默认参数：帧长 512、帧移 256、FFT 1024、64 个对数间隔包络点、12 个谐波。

每帧执行：

1. 截取 `[start, min(start+frame_length, sample_count))`，不足帧记录为截断。
2. 对实际长度应用 Hann 窗，零填充到 FFT 大小。
3. 计算幅度谱并做固定宽度移动平均，插值到包络频率网格。
4. 在参考 F0 的整数倍频率处采样，形成归一化谐波幅度。
5. 计算频谱质心、85% rolloff、谐波能量比、奇偶谐波比与 RMS。

所有浮点值按配置 `round_digits` 舍入，JSON 使用排序键与紧凑分隔符生成摘要。

## 6. 发音建模

每个特征帧中心时刻：

1. 在归一化音高曲线上线性插值，范围受 `pitch_min/pitch_max` 约束。
2. `f0 = reference_f0_mean * normalized_pitch`，范围受 `f0_min_hz/f0_max_hz` 约束。
3. 对发音单元计算内区间权重与边界过渡权重，归一化后混合 F1..Fn。
4. 输出帧时间、归一化音高、F0、共振峰轨迹、增益与单元权重。

过渡宽度由 `model.transition_ms` 配置。整个过程不含随机数。

## 7. 定点量化

配置同时声明目标位宽与目标缩放因子。运行时会依据该分片实际最大幅值降低有效缩放
因子，使所有整数落在 signed `bits` 范围内，并记录：

```json
{
  "per_shard_scales": {
    "pitch_scale": 8192,
    "formant_scale": 16,
    "f0_scale": 16,
    "configured": {"pitch_scale": 8192, "formant_scale": 16, "f0_scale": 16}
  },
  "pitch_curve": {"error_bound": 0.00006103515625, "max_observed_error": 0.00005},
  "formant_trajectory": {"error_bound": 0.03125, "max_observed_error": 0.02}
}
```

误差界严格为 `0.5 / effective_scale`; 若实际误差超过界，量化阶段直接失败。

## 8. 阶段指纹

阶段输入摘要如下：

```text
feature  = PCM sha256 + 音色 + 参考 F0 + 音频/特征配置
model    = feature 指纹 + 音高曲线 + 单元/共振峰 + 结构版本 + 建模配置
quantize = model 指纹 + 结构版本 + 量化配置
export   = quantize 指纹 + 结构版本 + 音频格式 + 导出配置
version  = 项目全部阶段指纹 + 配置版本 + 结构版本指纹
```

每个阶段产物内嵌 `artifact_sha256`，检查点额外记录文件 SHA256。构建时先比较阶段
输入指纹，再验证已存产物文件摘要；指纹匹配且文件完整才允许复用。

## 9. 检查点与恢复

检查点目录结构：

```text
workspace/checkpoints/
  .publish.lock
  records/000000000001.json
  records/000000000002.json
```

发布流程：

1. 获取 `flock` 排他锁。
2. 校验既有记录文件名连续为 `1..N`。
3. 校验前置阶段检查点存在且其产物文件仍存在。
4. 拒绝重复阶段指纹发布与落后/跳号序号。
5. 写入同目录临时文件、`fsync`、`os.replace`、目录 `fsync`。

构建循环按分片执行四个阶段。停止标志只在当前分片四阶段全部结束后检查；恢复时从
阶段指纹与检查点继续。强杀不会产生半条记录，因为记录文件只在完整序列化并落盘后替换。

## 10. 增量一致性证明

`consistency_check` 的步骤：

1. 在临时目录复制基准工作区的阶段、检查点与导出。
2. 应用同一种变更到增量与全量项目副本。
3. 增量工作区执行 `incremental`，全量工作区执行 `full`。
4. 对每个分片、每个阶段比较阶段指纹、产物 SHA256 与原始字节。
5. 递归比较全部项目级导出文件。
6. 比较不包含运行模式/复用统计的确定性项目指纹。

任何差异都返回 `E_INCREMENTAL_MISMATCH`，上下文列出分片、阶段与左右摘要；不做
容差比较。

## 11. 已知边界

- 当前确定性 WAV 路径固定为 16-bit PCM、单声道；这是任务输入约束。
- 版本 1 schema 不表达音高时间点与任意单元边界，降级导出按 schema 固有规则损失这些时间线信息。
- 实时预览、真实克隆模型、MOS 评价与外部标注平台不在本项目范围内。

