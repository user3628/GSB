# VOXALIGN · 本地确定性流式语音转换工作台

本项目从零实现一个不依赖真实声学模型、GPU、容器或外部服务的 Voice Conversion（VC）流式推理与诊断工作台。输入为固定随机种子生成的 16 kHz / 单声道 / 16-bit PCM WAV 分片与 JSON 转换清单；转换由 NumPy DSP 完成，包含帧分析、声学参数映射、波形重建、一致性校验四个阶段。

交付同时提供：

- 可逐字节复现的四阶段转换流水线；
- 帧块流式推理与整段推理的严格一致性校验；
- 基于清单参考基频区间的真值对齐诊断；
- 稳定业务错误码与输入参数异常注入；
- 原子检查点、真实文件锁、增量失效、优雅停止与恢复；
- 命令行与本地 HTTP/可视化工作台；
- 3 个确定性中英混排演示分片、自动验收套件与验证记录。

## 快速开始

环境要求：Python 3.10+、NumPy。

```bash
python3 -m pip install -r requirements.txt

# 生成固定种子演示数据（3 分片、2 个目标说话人）
python3 -m vcworkbench demo --force

# 四阶段转换；默认同时执行整段与逐块一致性校验
python3 -m vcworkbench convert --block-frames 4

# 启动本地可视化工作台
python3 -m vcworkbench serve --host 127.0.0.1 --port 8765
# 浏览器打开 http://127.0.0.1:8765
```

默认工作区为 `./workbench_data`。也可在命令前后使用 `--workspace DIR` 指定共享工作区。HTTP 服务与 CLI 共用同一清单、状态目录、停止标记和 `flock` 发布锁。

## 目录与产物

```text
workbench_data/
  manifest.json                 # 分片清单
  samples/*.wav                 # 16 kHz mono PCM16
  state/
    artifacts/<shard>/<stage>/<input-fingerprint>/
      manifest.json             # 内容寻址产物清单及摘要
      analysis.json
      frames.csv               # 帧级参数表；未定 F0 写为 UNDEFINED
      mapping.json
      reconstruction.json
      output.pcm
      output.wav
      diagnostics.json
    shards/<shard>/
      ledger.json               # 原子账本，内含链式检查点记录
      highwater.json            # 防回退高水位
      publish.lock              # fcntl.flock 真实文件锁
    control/stop.json           # 优雅停止请求
  exports/
```

每个检查点记录含：分片标识、阶段名、阶段序号、输入内容指纹、配置版本、产物摘要、产物路径、前置阶段摘要、前序记录摘要和记录自身摘要。账本采用临时文件 + `fsync` + `os.replace` 原子替换，中断不会留下半写 JSON。

## 转换流水线

### 1. 帧分析 `analysis`

- 按清单 `frame_shift` 对 PCM16 分帧，帧长 400 样本（25 ms），使用 Hann 窗。
- 自相关基频估计范围 70–450 Hz，搜索峰并做抛物线插值；低能量或不可靠帧写为 `f0_hz: null` / `voiced: false`，CSV 中写为 `UNDEFINED`，绝不填零。
- 计算 32 维 HTK Mel 滤波器组对数能量，并生成整段 Mel 摘要。
- 输出 `analysis.json` 与确定性 `frames.csv`。

### 2. 声学参数映射 `mapping`

- 目标基频 = 源基频 × 目标说话人倍率 × 清单/注入的基频偏移系数。
- 在 Mel 域按 `mel_warp` 重排频带能量，并保持显式 voiced/unvoiced 状态。
- 目标说话人倍率由固定 profile 提供（`T1=1.08`、`T2=0.92`、`T3=1.02`）。

### 3. 波形重建 `reconstruction`

- Mel 能量经三角滤波器组逆投影为功率谱，固定 BLAKE2b 哈希投影生成逐帧相位。
- 乘固定余弦相位包络与 Hann 合成窗，做归一化 overlap-add。
- 按 `quant_bits` 量化并写入 16-bit WAV；统计越界钳制次数。
- 输出样本数严格等于清单 `expected_output_samples`，不足补零、超出截断。
- full 与 stream 使用完全相同的逐帧计算和 OLA 状态机，因此块边界不改变任何字节。

### 4. 一致性校验 `validation`

- 以清单参考基频均值区间为唯一真值来源，不推断缺失真值。
- 统计 voiced 帧落入区间比例、相对区间中心偏差、绝对偏差、区间外帧索引。
- `interpolation`：映射后 F0 位于参考区间内；`extrapolation`：位于参考区间外；`unvoiced`：无可靠源 F0，不进入比例分母。
- 区间外偏差超过容差时返回 `E_PITCH_OUT_OF_RANGE`，附分片、阶段和帧索引。
- 独立重跑逐块分析、映射、重建，将拼接 PCM、逐帧 F0、Mel 摘要与整段产物比较；不一致返回 `E_STREAM_MISMATCH`、分片标识和首个失配帧。

## 版本指纹与增量失效

阶段输入指纹按依赖链计算：

```text
analysis      := f(WAV 原始字节摘要, frame_shift, DSP profile, config_version)
mapping       := f(analysis artifact digest, target_speaker, pitch_shift, mel_warp)
reconstruction:= f(mapping artifact digest, quant_bits, expected_output_samples)
validation    := f(reconstruction artifact digest, reference interval, tolerance, verify_stream)
```

任一输入变化时，比较当前阶段指纹与账本中的当前记录，最早的失配阶段及其全部下游被清除并重建；上游内容寻址产物直接复用。块长不属于任何阶段产物指纹，因此修改块长不会触发重建或改变最终摘要。

示例：`quant_bits` 改变时 `analysis`、`mapping` 复用，只重建 `reconstruction`、`validation`；目标说话人改变时从 `mapping` 开始失效，`analysis` 继续复用。

## 检查点并发与恢复

- 每个分片使用 `fcntl.flock` 独占锁发布；发布要求 `expected_seq == ledger.seq + 1` 和匹配的 `epoch`。
- 落后、重复、跨分片错位发布返回 `E_STALE_CHECKPOINT`。
- 缺失前置阶段或前置摘要不匹配返回 `E_INVALID_STAGE`。
- 高水位记录已提交序号与尾部摘要；账本被整体替换为旧版本时会被识别并拒绝。
- 产物文件被篡改时，清单或载荷 SHA-256 不匹配，返回 `E_STALE_CHECKPOINT`。
- 停止请求由 `state/control/stop.json` 传递；转换器在完成当前帧块后抛出 `OperationStopped`，不生成半阶段检查点。
- `resume` 清除停止标记，复用已完成上游阶段，从第一个缺失/失效阶段继续。最终 PCM、帧参数和诊断与一次从零运行逐字节一致。

## 稳定业务错误码

| 业务码 | 阶段 | 语义 |
|---|---|---|
| `E_CHANNEL_UNSUPPORTED` | analysis | 非单声道或注入声道数不符 |
| `E_SAMPLE_RATE_MISMATCH` | analysis | 实际/声明采样率冲突或非 16 kHz |
| `E_NUMERIC` | analysis | 样本含 NaN/Inf |
| `E_MANIFEST_MISMATCH` | analysis/ingest | 清单声明样本数与 WAV 不符、清单结构错误等 |
| `E_INVALID_PARAM` | analysis/mapping/reconstruction/validation | 帧移、基频偏移、弯折、量化位宽等非法 |
| `E_PITCH_OUT_OF_RANGE` | validation | 映射 F0 超出参考区间容差 |
| `E_REFERENCE_MISSING` | validation | 缺失参考基频区间，拒绝猜测真值 |
| `E_STREAM_MISMATCH` | validation | 流式与整段结果不一致 |
| `E_STALE_CHECKPOINT` | checkpoint | 序号落后/重复、跨分片、摘要不符、回退 |
| `E_INVALID_STAGE` | checkpoint | 前置阶段缺失或摘要不匹配 |
| `E_LATENCY_BUDGET` | diagnostics | 逐帧诊断超过可配置延迟预算 |
| `E_PORT_UNAVAILABLE` | HTTP | 服务端口占用 |

错误 JSON 统一包含 `code`、`message`、`shard_id`、`stage` 与阶段相关 `details`。

## CLI

```bash
# 转换全部或指定分片
python3 -m vcworkbench convert --shard zh_en_01 --block-frames 4

# 逐块流式验证，并导出每块原始 PCM16LE
python3 -m vcworkbench stream --shard zh_en_01 --block-frames 4 --chunks-dir exports/chunks

# 查看整段诊断 / 单帧详细诊断及延迟预算
python3 -m vcworkbench diagnose --shard zh_en_01
python3 -m vcworkbench diagnose --shard zh_en_01 --frame 20 --latency-budget-ms 5

# 异常注入（仅参数，不改源码）
python3 -m vcworkbench --json convert --shard zh_en_01 --inject channels=2
python3 -m vcworkbench --json convert --shard zh_en_01 --inject declared_sample_rate=44100
python3 -m vcworkbench --json convert --shard zh_en_01 --inject nan_index=321
python3 -m vcworkbench --json convert --shard zh_en_01 --inject sample_count_delta=7
python3 -m vcworkbench --json convert --shard zh_en_01 --inject pitch_shift=0

# 停止与恢复
python3 -m vcworkbench stop
python3 -m vcworkbench resume --block-frames 7

# 导出、报告、检查点
python3 -m vcworkbench export --shard zh_en_01 --out exports/zh_en_01.wav
python3 -m vcworkbench report --shard zh_en_01
python3 -m vcworkbench fingerprints --shard zh_en_01
python3 -m vcworkbench state

# 导入外部本地清单及 WAV
python3 -m vcworkbench import --manifest /path/to/manifest.json

# 一键验收（临时工作区）
python3 -m vcworkbench --json verify --out artifacts/verification.json
```

停止模拟也可使用 `convert --stop-after-blocks N`：在第 N 个不可拆分重建帧块后通过同一停止标记退出。

## HTTP / 可视化工作台

```bash
python3 -m vcworkbench serve --host 127.0.0.1 --port 8765
```

主要接口：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 可视化诊断页（零外部前端依赖） |
| GET | `/api/health` | 服务、工作区、NumPy、确定性状态 |
| GET | `/api/state` | 清单、分片、四阶段检查点、诊断摘要 |
| POST | `/api/demo` | 生成固定种子演示数据 |
| POST | `/api/import` | 导入本地清单与 WAV |
| POST | `/api/convert` | 同步转换 |
| POST | `/api/jobs` | 后台 convert/resume/inject/stream 任务 |
| GET | `/api/jobs/<id>` | 任务进度与结果 |
| POST | `/api/jobs/<id>/stop` | 请求任务优雅停止 |
| POST | `/api/stop` | 写入共享停止标记 |
| POST | `/api/resume` | 清除停止标记并提交恢复任务 |
| GET | `/api/diagnostics/<shard>` | 整段基频对齐诊断 |
| GET | `/api/diagnostics/<shard>/frame?frame_index=&latency_budget_ms=` | 单帧诊断与延迟预算 |
| GET | `/api/stream/<shard>?block_frames=` | 逐块 PCM 摘要及 base64 载荷 |
| GET | `/api/report/<shard>` | 转换报告 |
| GET | `/api/export/<shard>.wav` | WAV 下载 |
| GET | `/api/fingerprints/<shard>` | 版本指纹与检查点 |

页面提供分片卡片、四阶段状态、F0 真值带与转换轨迹、逐帧偏差/插值/外推标识、Mel 能量、检查点序号、逐块 PCM 摘要、异常注入、停止/恢复及导出。

## 演示数据

固定种子 `20261122` 生成以下分片：

| 分片 | 源→目标 | 帧移 | 样本数 | 参考 F0 区间 | transcript |
|---|---|---:|---:|---|---|
| `zh_en_01` | S1→T1 | 160 | 17,600 | 193.700–218.428 Hz | 中文 + English 混排 |
| `zh_en_02` | S2→T2 | 192 | 20,000 | 208.849–235.511 Hz | English + 中文混排 |
| `zh_en_03` | S3→T1 | 128 | 16,000 | 181.924–205.148 Hz | 中文 + English + stop/resume |

音频为确定性谐波/共振峰合成语音段（含首尾静音帧），不是真实声学模型输出。

## 实际验证结果

执行：

```bash
pytest -q
python3 -m vcworkbench --json verify --out artifacts/verification.json
```

本次环境（Python 3.14.4、NumPy 2.2.1）结果：

- `pytest`：`6 passed`。
- 一键验收：`40/40` 项通过，完整机器可读记录见 `artifacts/verification.json`。
- 块长 `1`、`3`、`997` 与基线块长 `5` 的最终 PCM/WAV/诊断/逐帧摘要完全一致。
- 逐块 PCM base64 拼接 SHA-256 与整段 PCM 完全一致。
- 停止于 `reconstruction` 第 1 个帧块；恢复时 `analysis`/`mapping` 复用、重建下游，最终摘要与独立全量运行一致。
- 异常注入依次返回 `E_CHANNEL_UNSUPPORTED`、`E_SAMPLE_RATE_MISMATCH`、`E_NUMERIC`、`E_MANIFEST_MISMATCH`、`E_INVALID_PARAM`、`E_PITCH_OUT_OF_RANGE`、`E_REFERENCE_MISSING`。
- HTTP 逐帧诊断在预算内返回 200；`latency_budget_ms=0` 返回 504 与 `E_LATENCY_BUDGET`。
- 检查点载荷篡改、账本序号回退、跨分片发布、缺失前置阶段分别被稳定拒绝。
- 端口占用返回 `E_PORT_UNAVAILABLE`。

基线 PCM 摘要（验证记录中的固定值）：

| 分片 | 样本数 | PCM SHA-256 | 区间内比例 | 转换 F0 均值 |
|---|---:|---|---:|---:|
| `zh_en_01` | 17,600 | `66d577ddb2204663c0f7403652a381258d96c7a11151a114329d1ab0ffb69538` | 1.000 | 206.5265 Hz |
| `zh_en_02` | 20,000 | `18c14e6ceff7aac18af31afc2f5a45f28492328e28b25371b6eb51b83e94313e` | 1.000 | 222.2191 Hz |
| `zh_en_03` | 16,000 | `674fd82493753d326c8faf09150436a7482c9d0c68854da3aaef5bc8df1e23ad` | 1.000 | 194.1984 Hz |

## 约束与边界

- 不使用 Docker、容器、GPU、真实语音数据或任何外部网络服务。
- 浮点字节复现以同一运行时/NumPy 环境为前提；产物、JSON 序列化和哈希编码均采用规范化确定性写法。
- 这是 DSP 基准实现，不宣称自然度或说话人相似度达到真实神经网络 VC 系统水平。
- `SIGKILL` 等强制终止不承诺退出报告；已提交检查点由原子账本与高水位保护。
