# audiofp-audit

本地、确定性、仅依赖 NumPy 的音频指纹检索特征压缩与量化误差审计组件。组件面向 16 kHz、单声道、16 位 PCM WAV 分片，提供从谱峰检测、指纹对生成、定点量化/桶压缩到参考真值审计、检查点恢复、增量失效和并发发布保护的完整链路。

不依赖音频解码库、GPU、容器或外部服务。WAV 文件由组件直接解析 RIFF/PCM 数据。

## 快速开始

Windows PowerShell：

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python scripts\run_verification.py
```

Linux/macOS：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python scripts/run_verification.py
```

也可安装为本地 wheel/可编辑包，并提供 `audiofp` 命令：

```bash
python -m pip install -e .
audiofp --help
```

## 交付内容

```text
audiofp/
  dsp.py          汉宁窗分帧、幅度谱、二维局部极大谱峰检测
  pairing.py      锚点到目标点的确定性指纹对生成
  quantize.py     定点量化、FNV-1a 桶哈希、碰撞审计
  audit.py        参考真值命中/误配/时移审计、定点可信度、语料汇总
  checkpoints.py  原子检查点、分片连续序号、阶段失效
  locking.py      fcntl / msvcrt 真实文件锁
  pipeline.py     四阶段可恢复流水线、停止请求、增量重建
  api.py          导入、状态/版本查询、报告与阶段产物读取
  samples.py      固定种子 WAV 与参考指纹对生成
  cli.py          库命令行入口
samples/          已生成并提交的确定性样本、配置和参考真值
scripts/          样本生成与验收演示脚本
tests/            10 项自动化测试
artifacts/verification.json|txt  实际验收结果
```

## 数据合同

### WAV

- RIFF/WAVE，未压缩 PCM
- 单声道、16 kHz、16 位小端
- 分片长度必须至少覆盖一个分析帧

不满足条件返回 `E_INVALID_WAV`。静音或全零分片在谱峰阶段返回 `E_NO_PEAK`，错误对象包含 `shard_id`，不会静默返回空峰表。

### 配置清单

示例见 `samples/fingerprint_config.json`。每个分片可覆盖 `defaults` 中的字段：

| 字段 | 含义 |
| --- | --- |
| `shard_id` | 分片稳定标识 |
| `wav` | 相对清单目录的 WAV 路径 |
| `frame_length`, `hop_length` | 分帧长度与帧移（采样点） |
| `neighborhood` | 时间-频率二维局部极大检测半径；同时作用于相邻帧和相邻频格 |
| `time_delta_min/max` | 指纹对时间差范围，单位为分析帧 |
| `freq_delta_min/max` | 频率差绝对值范围，单位为 FFT 频率格 |
| `quantization_bits` | 频率格和时间差的定点量化位宽 |
| `max_peaks_per_frame` | 每帧按幅度截断后的最大谱峰数 |
| `max_pairs` | 每分片指纹对上限 |
| `targets_per_anchor` | 每个锚点保留的高幅度目标峰数 |
| `bucket_width` | 确定性哈希后的桶数量 |
| `expected_false_match_rate` | 允许的实测误配率上限，`[0,1]` |
| `confidence_threshold` | 可疑分片可信度下限，`[0,1]` |
| `max_collision_rate` | 桶碰撞率上限，`[0,1]` |
| `peak_amplitude_floor` | 幅度谱绝对谱峰下限 |
| `hash_seed` | 桶哈希种子 |
| `pairing_policy` | 当前支持 `bounded_no_wrap` |

### 参考真值

`samples/reference_pairs.json` 的结构为：

```json
{
  "schema": "audiofp.reference.v1",
  "seed": 20261123,
  "shards": {
    "music_a": [
      {
        "anchor_frame": 3,
        "anchor_freq": 14,
        "target_frame": 7,
        "target_freq": 28,
        "dt": 4
      }
    ]
  }
}
```

审计只使用该清单作为检索真值，不把当前生成的指纹对反向写入真值。样本生成器从“仅保留稳定音调与节奏、去除噪声”的参考音频抽取真值，因此混音中由噪声产生的额外指纹会真实计入误配。

## 算法与审计口径

### 1. 分帧与谱峰

1. 按 `frame_length`、`hop_length` 取样，乘汉宁窗。
2. 通过实数 FFT 计算幅度谱。
3. 在每帧时间-频率平面内比较 Chebyshev 邻域 `|Δt| <= neighborhood`、`|Δf| <= neighborhood`。
4. 严格局部极大；等幅邻峰通过更早的 `(frame, freq)` 字典序确定胜者。
5. 每帧按幅度降序、频率升序选前 `max_peaks_per_frame` 个峰，最终按 `(frame, freq)` 排序输出。

谱峰记录帧索引、频率格和幅度。所有产物通过排序键、显式舍入和规范化 JSON 序列化，保证同一运行时与输入逐字节一致。静音、全零或没有满足门限的峰返回 `E_NO_PEAK`。

### 2. 指纹对

对按 `(frame, freq)` 排序的谱峰逐个作为锚点，仅搜索其后的目标峰。候选目标必须满足：

```text
time_delta_min <= target_frame - anchor_frame <= time_delta_max
freq_delta_min <= |target_freq - anchor_freq| <= freq_delta_max
```

每个锚点最多保留 `targets_per_anchor` 个幅度最高的目标峰；等幅时按目标帧和频率格稳定排序。总计不超过 `max_pairs`。

显式循环策略为 `bounded_no_wrap`：扫描到分片最后一个谱峰时不会回绕到分片开头，避免把尾部到开头的时间差误记为正向时差。产物中记录 `pairing_policy`、`terminal_anchor_policy`、`wraparound=false` 和 `terminal_anchor_edges`。

### 3. 定点量化与桶压缩

对 `anchor_freq`、`target_freq`、`time_delta` 分别使用：

```text
levels = 2**bits - 1
scale  = value_range / levels
q      = round_half_up(value / scale), clipped to [0, levels]
upper_bound = 0.5 * scale = value_range / (2 * (2**bits - 1))
```

频率字段的 `value_range = frame_length // 2`，时间差字段的 `value_range = time_delta_max`。产物记录每个字段的 `scale_num`、`scale_den`、`scale`、`quantization_step`、理论上界、实际最大绝对偏差和有效位数。有效位数定义为：

```text
min(configured_bits, log2(value_range / (2 * actual_max_abs_error)))
```

量化三元组 `(anchor_freq_q, target_freq_q, dt_q)` 通过手写 FNV-1a 64 位哈希和 `hash_seed` 映射到 `bucket_width` 个桶。碰撞统计包括量化值域大小、桶宽是否小于量化值域、`collision_count = vector_count - unique_bucket_count`、`collision_rate_ppm`、最强碰撞桶（同计数时取较小桶号）、最强桶向量数和碰撞数。

固定点碰撞率超过 `max_collision_rate` 时返回：

```json
{
  "code": "E_BUCKET_COLLISION",
  "shard_id": "...",
  "stage": "quantize",
  "details": {
    "bucket": 0,
    "strongest_bucket": 0,
    "collision_rate_ppm": 994382
  }
}
```

### 4. 检索与误差传播审计

将每条参考真值按同一量化参数编码为 `(anchor_freq_q, target_freq_q, dt_q)`。生成向量与参考键精确匹配才算命中：

- `hit_rate = hits / reference_pair_count`
- `false_match_rate = generated_vectors_not_in_reference / generated_vector_count`
- `average_dt_shift = mean(abs(qdt * time_delta_scale - reference_dt))`

误配率同时以整数 ppm 和浮点值输出。分片实测误配率超过 `expected_false_match_rate` 时，审计标记 `passed=false`，`violations` 给出指标名、估计值和上限，分片进入汇总的 `failed_shards`。

语料汇总按分片规模累积输出 `cumulative_by_shard_count`，包含累计误配率和 Wilson 95% 上界；总语料也给出未加权、可信度加权和 Wilson 上界三种估计。

### 5. 分片可信度与可疑标注

可信度全程在百万分之一（ppm）定点域计算：

```text
peak_factor       = min(1_000_000, peak_count * 10_000)
pair_factor       = min(1_000_000, pair_count * 4_000)
collision_factor  = 1_000_000 - collision_rate_ppm
confidence_ppm    = (30*peak_factor + 30*pair_factor + 40*collision_factor + 50) // 100
```

低于 `confidence_threshold` 的分片进入 `suspicious_shards`，与 `failed_shards` 分开汇总。审计可信度权重经整数归一化，总和严格等于 `1_000_000` ppm。

## 检查点、恢复与版本失效

### 工作区布局

```text
<workspace>/
  manifest.json                 清单快照
  workspace.json                工作区/布局版本
  artifacts/peaks/              谱峰产物
  artifacts/pairs/              指纹对产物
  artifacts/quantized/          压缩向量与量化误差
  reports/shards/               分片审计
  reports/summary.json          语料汇总
  reports/summary.sha256        汇总摘要
  errors/                       分片业务错误
  checkpoints/<shard>/state.json
  checkpoints/<shard>/records/  每阶段不可变记录
  .locks/                       真实文件锁
  .stop_requested               优雅停止请求
```

### 原子与顺序

每阶段执行顺序为：

1. 生成规范 JSON 产物。
2. 写入同目录临时文件，`fsync` 后 `os.replace` 原子替换。
3. 在分片锁内校验产物 SHA-256。
4. 原子写检查点记录。
5. 原子更新分片状态并推进 `next_seq`。

进程在任意位置中断都不会留下半写 JSON。若在“产物已写、状态未写”之间中断，下一次运行会以同一版本重写产物并重试该检查点。

检查点记录包含 `shard_id`、`stage`、`input_digest`、`config_version`、`stage_version`、`artifact_digest`、`dependencies`、`params`、`generation` 和 `seq`。

`seq` 在分片内单调连续，不因配置失效而重置。失效只删除当前及下游阶段状态，`next_seq` 保留，因此重建发布继续使用后续连续序号。

### 文件锁与稳定错误

- POSIX 使用 `fcntl.flock(LOCK_EX)`。
- Windows 使用 `msvcrt.locking(LK_LOCK)`。
- 发布时在锁内读取状态、比较分片内序号、验证阶段连续性和产物摘要。

| 错误码 | 触发条件 |
| --- | --- |
| `E_NO_PEAK` | 静音、全零、过短或低于峰门限，无法产生谱峰 |
| `E_BUCKET_COLLISION` | 固定点碰撞率超过配置阈值 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/跳号、重复阶段、产物摘要不匹配、跨分片错位发布 |
| `E_INVALID_STAGE` | 非法阶段名、跳过前置阶段、请求缺失阶段产物 |
| `E_INVALID_CONFIG` | 清单字段、参考真值或量化范围非法 |
| `E_INVALID_WAV` | WAV 格式不满足合同 |

### 版本指纹与最早失效阶段

| 阶段 | 影响该阶段版本的输入 |
| --- | --- |
| `peaks` | 样本内容、帧长/帧移、邻域、最小频格、每帧峰数、峰门限、算法版本 |
| `pairs` | 上游谱峰版本与产物摘要、时间差/频率差范围、对数上限、目标数、循环策略 |
| `quantize` | 上游指纹对版本与摘要、位宽、桶宽、哈希种子/算法、碰撞阈值 |
| `audit` | 上游量化版本与摘要、参考文件内容、期望误配率、可信度阈值 |

启动时按顺序比较依赖。找到最早失效阶段后，删除该阶段及全部下游产物和检查点；上游阶段继续复用。`query_status()` 和 `last_invalidation` 中可观察：

```json
{
  "from_stage": "quantize",
  "reason": "params.bucket_width",
  "changed_fields": ["params.bucket_width"],
  "removed_stages": ["quantize", "audit"],
  "next_seq": 5
}
```

## 库用法

```python
from audiofp import import_dataset, run_pipeline, get_report, query_status

import_dataset("workspace", "samples/fingerprint_config.json")
result = run_pipeline("workspace", stop_after_stage="quantize")
print(result.as_dict())       # stopped=True，当前阶段已完成

resumed = run_pipeline("workspace")  # 同一阶段状态自动复用
report = get_report("workspace")
status = query_status("workspace")
```

外部停止请求：

```python
from audiofp import request_stop, resume_pipeline

request_stop("workspace")        # 当前不可拆分阶段完成后退出
resume_pipeline("workspace")     # 清除停止标记并恢复
```

低层能力也可独立调用：

```python
from audiofp import read_wav, load_manifest, detect_peaks
from audiofp import generate_fingerprint_pairs, quantize_pairs

manifest = load_manifest("samples/fingerprint_config.json")
config = manifest.by_id()["music_a"]
wav = read_wav(config.wav_path, config.shard_id)
peaks = detect_peaks(wav, config)
pairs = generate_fingerprint_pairs(peaks.peaks, config)
quantized = quantize_pairs(pairs.pairs, config)
```

## CLI

```bash
audiofp generate-samples --samples-dir samples --seed 20261123
audiofp import --workspace workspace --manifest samples/fingerprint_config.json
audiofp run --workspace workspace --stop-after-stage quantize
audiofp status --workspace workspace
audiofp report --workspace workspace
audiofp report --workspace workspace --shard music_a
audiofp request-stop --workspace workspace
audiofp resume --workspace workspace
```

业务错误以稳定 JSON 输出到 stderr；`run` 在出现流水线错误时返回退出码 2，在审计阈值失败时返回 3。`--tolerate-business-errors` 可将演示运行强制为 0。

## 确定性样本与演示

固定种子 `20261123` 生成 5 个分片：

| 分片 | 内容/配置 | 预期 |
| --- | --- | --- |
| `music_a` | 稳定音调 + 噪声段 + 节奏段，正常桶宽 | 审计通过 |
| `music_b` | 另一组音高与节奏，正常桶宽 | 审计通过 |
| `music_c` | 第三组音高与节奏，正常桶宽 | 审计通过 |
| `silence` | 2 秒全零 PCM | `E_NO_PEAK` |
| `bucket_collision` | 音乐分片，`bucket_width=1`、碰撞率上限 20% | `E_BUCKET_COLLISION`，最强桶号 0 |

运行：

```bash
python scripts/generate_samples.py --samples-dir samples --seed 20261123
python scripts/run_verification.py
```

`run_verification.py` 会覆盖以下验收路径并把结果写入 `artifacts/verification.json` 和 `artifacts/verification.txt`：

- 完整运行；
- 在 `quantize` 阶段后停止，再从检查点恢复；
- 写入 `.stop_requested` 后在阶段边界停止，再显式恢复；
- 将 `music_c.bucket_width` 从 4096 改为 8192，验证 `peaks`/`pairs` 复用、`quantize`/`audit` 失效；
- 用修改后的配置从零构建，并逐字节比较全部阶段产物、错误和审计报告；
- 校验 3 个音乐分片的量化实际偏差不超过推导上界。

本次实际验证结果：

- 10 项单元测试全部通过，验收脚本 18 项检查全部通过；
- 3 个音乐分片均 `hit_rate_ppm = 1000000`；
- 分片误配率约为 34.24%、54.47%、29.92%，均低于配置上限 80%；
- `music_a`、`music_b` 可信度低于 0.70，进入可疑分片单列；`music_c` 未标记可疑；
- 完整运行与两种停止/恢复路径的全部 `artifacts/`、`reports/`、`errors/`、`checkpoints/` 文件逐字节一致；
- 增量失效后产物与同配置从零构建逐字节一致；
- 静音分片稳定返回 `E_NO_PEAK`，超低桶宽分片稳定返回 `E_BUCKET_COLLISION`。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖谱峰确定性、静音错误、指纹对边界与无回绕策略、量化上界与碰撞错误、审计阈值失败、完整演示、两类停止恢复、增量失效、所有指定版本字段的最早阶段命中、检查点顺序/阶段/跨分片保护、真实文件锁并发发布。

## 约束与解释

- 指纹对的频率差按绝对值筛选；目标峰始终晚于锚点峰。
- 最后一个锚点采用显式 `bounded_no_wrap`，不执行分片内循环回绕。
- 桶碰撞率包含“不同绝对时间但量化后三元组相同”的真实重复指纹，因此高重复音乐段可能产生较高碰撞率；阈值应按业务重复度配置。
- 参考真值来自固定种子生成的去噪音乐骨干，是审计的唯一真值来源。
- 逐字节确定性以同一 Python/NumPy 运行时为前提；跨 NumPy 版本若 FFT 实现变化，应视为算法环境版本变化并重建产物。
