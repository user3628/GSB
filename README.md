# vadkit：本地确定性流式语音端点检测组件

`vadkit` 是一个可嵌入音频服务的 Python 3.11 库，同时提供命令行、固定种子样本生成器、批量评测、原子检查点与完整演示。输入限定为 16 kHz、单声道、16 位 PCM WAV；所有语音段真值、噪声和同步位置都由本地生成器产生，不访问网络或外部数据集。

## 实际验证结果

本工作区已使用 Python 3.11.18、pip 26.3、numpy 2.4.6、scipy 1.17.1、pytest 9.2.0 完成验证。

- `24 passed`：覆盖配置错误、短尾帧、双特征、任意块流式一致性、暂定端点、指标复现、检查点、强制终止恢复、多进程发布、版本失效和全部注入错误码。
- 4 个正常分片、5 个参考语音段：批处理与流式均为 `5 hits / 0 false positives / 0 misses / hit_rate 1.0`。
- 平均端点边界偏差 `8.0 ms`，最大偏差 `10.0 ms`；批处理与流式聚合指标完全相同。
- 固定演示结果 SHA-256：`982c0467041a5733d91b6b55e8ff05383fbe74e4b5019618430c73ed47346dcf`。
- `artifacts/demo/demo_summary.json` 连续两次运行 SHA-256 均为 `42c605c2f3d80a2780043508c733a65fe5328918f4e77c2c22d73a4b38b8b1bb`。
- 停止时已完成并持久化 2 个分片；恢复后复用这 2 个检查点，最终 `result.json` 与从零完整运行逐字节一致。
- 五类注入分别返回 `E_SILENT_INPUT`、`E_FEATURE_MISMATCH`、`E_INPUT_LENGTH`、`E_INVALID_CONFIG`、`E_FEATURE_MISMATCH`。

## 快速开始

```bash
# 使用已创建的虚拟环境
.venv/bin/python -m pip --version

# 重新安装或复现依赖
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e .

# 生成固定种子样本、清单和异常样例
.venv/bin/vadkit generate --out data

# 完整演示（同时运行测试数据生成、批/流评测、停止恢复和异常注入）
.venv/bin/vadkit demo --data data --out artifacts/demo --tolerance-ms 80

# 自动化验收
.venv/bin/pytest -q
```

也可以在已有 Python 3.11 环境中执行：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install -e .
```

未使用 Docker 或其他容器环境。

## 组件结构

```text
src/vadkit/
  config.py      配置校验、帧/移换算、配置指纹
  features.py    Hann 分帧、短帧标记、对数能量、频谱平坦度、归一化得分
  stream.py      唯一的状态机/平滑/发布路径，流式与批处理共用
  audio.py       WAV、清单、顺序、声明样本数校验，内容指纹
  metrics.py     容差匹配、命中/误报/漏报、平均/最大偏差
  checkpoint.py  文件锁、代际元数据、原子记录、缓存、序号保护
  pipeline.py    批/流双通道评测、恢复、版本状态
  injection.py   参数化异常注入
  samplegen.py   无外部数据的确定性 WAV/清单生成器
  cli.py         库之上的完整命令行
tests/           24 项 pytest 验收测试
data/            已生成的 WAV、正常清单和异常样例
artifacts/demo/  固定演示、完整结果和停止恢复工作区
```

## 处理链路

1. **输入校验**：清单顺序必须连续；WAV 必须为 16 kHz 单声道 int16；每片实际样本数必须等于 `sample_count`。不符时抛出 `E_INPUT_LENGTH`，附带 `chunk_id`、声明值和实际值。
2. **分帧加窗**：默认 `frame_ms=20`、`hop_ms=10`，帧起点为 `0, hop, 2*hop, ...`。末帧不足帧长时保留实际长度，`is_short=true`，只在 FFT 计算中补零，不伪造样本。
3. **双特征打分**：逐帧计算 `20*log10(RMS)` 和频谱平坦度；能量线性归一化到配置范围，平坦度转为 `1-flatness`，按权重合成 `[0,1]` 得分。默认权重为 `0.62/0.38` 且必须和为 1。
4. **双阈值滞回**：空闲态达到高阈值才进入语音；语音态低于低阈值才退出；每帧输出 `raw_state`、`smoothing_state` 和触发原因。
5. **平滑与发布**：语音短于 `min_speech_ms` 丢弃；低于 `min_silence_ms` 的间隙桥接。退出后必须先观察到完整最短静音才发布端点；此前可通过 `tentative_endpoints` 查询暂定候选。
6. **时间戳**：端点以样本索引换算毫秒并固定小数位；批处理和任意块流式使用同一处理代码，因此端点数、顺序和字段逐项一致。

## 库接口

### 单分片批处理

```python
from vadkit import VADConfig, detect_samples, read_wav

samples, sample_rate = read_wav("data/audio/01_clear.wav", chunk_id="clear_01")
result = detect_samples(
    samples,
    VADConfig(),
    chunk_id="clear_01",
    expected_sample_count=32_000,
)
print([endpoint.as_dict() for endpoint in result.endpoints])
print([frame.as_dict() for frame in result.frames])
```

### 流式会话与暂定端点

```python
from vadkit import StreamingVAD, VADConfig, read_wav

samples, _ = read_wav("data/audio/03_breath_pause.wav")
session = StreamingVAD(VADConfig(), chunk_id="breath_pause_03")

for start in range(0, len(samples), 333):
    session.feed_samples(samples[start:start + 333])
    print(session.published_endpoints)
    print(session.tentative_endpoints)  # 确认最短静音前的暂定候选

result = session.finalize()
```

单次 `feed_samples` 可接收任意正长度样本块。`feed_frame` 可接收已分帧窗口；`stream_frames` 可处理预分帧迭代器。批处理 `detect_samples` 和流式接口共享同一状态机。

### 检查点评测与状态查询

```python
from vadkit import evaluate_dataset, workspace_status

outcome = evaluate_dataset(
    "data/manifest.json",
    "artifacts/run",
    tolerance_ms=80.0,
    stop_after_chunks=2,
)
status = workspace_status("data/manifest.json", "artifacts/run")

# 清除停止请求并继续；已发布检查点会复用
outcome = evaluate_dataset(
    "data/manifest.json",
    "artifacts/run",
    tolerance_ms=80.0,
    clear_stop=True,
)
```

## 命令行

```bash
.venv/bin/vadkit detect --wav data/audio/01_clear.wav --include-frames
.venv/bin/vadkit stream --wav data/audio/02_low_snr.wav --chunk-size 333
.venv/bin/vadkit evaluate --manifest data/manifest.json --workspace artifacts/run --tolerance-ms 80
.venv/bin/vadkit status --manifest data/manifest.json --workspace artifacts/run
.venv/bin/vadkit versions --manifest data/manifest.json --workspace artifacts/run
.venv/bin/vadkit stop --workspace artifacts/run
.venv/bin/vadkit resume --manifest data/manifest.json --workspace artifacts/run
.venv/bin/vadkit inject --data data --kind clipped_peak
.venv/bin/vadkit config
```

`evaluate`/`resume` 的 `--stop-after-chunks N` 和 `--crash-after-chunks N` 用于可重复演示与恢复测试；`--crash-after-chunks` 在检查点落盘后以退出码 `70` 强制终止，模拟无清理的进程死亡。

## 样本与清单

`vadkit generate` 固定随机种子 `20261120` 生成：

- `clear_01`：清晰双语句、静音间隔。
- `low_snr_02`：低信噪比单词。
- `breath_pause_03`：语句内 120 ms 气息停顿，应被最短静音桥接为单端点。
- `impulse_04`：孤立冲击噪声，应被最短语音规则丢弃。
- `anomalies/silent_zero.wav`：全零输入。
- `anomalies/clipped_peak.wav`：连续硬削峰。
- `anomalies/length_mismatch.wav` 与独立 `length_mismatch_manifest.json`：声明 16128、实际 16000 样本。

正常清单 `data/manifest.json` 包含分片顺序、声明样本总数、每片声明样本数、参考语音段毫秒起止、条件标签和内容 SHA-256。生成器不会读取网络数据。

## 错误码

| 业务码 | 触发点 | 退出码 | 定位信息 |
|---|---|---:|---|
| `E_INVALID_CONFIG` | 启动前配置校验、非法容差 | 2 | 非法字段与值 |
| `E_INPUT_LENGTH` | 清单/WAV 样本数不符、PCM 块不完整 | 3 | `chunk_id`、声明/实际样本、样本位置 |
| `E_SILENT_INPUT` | 整片全数字零 | 4 | `chunk_id`、样本位置 |
| `E_FEATURE_MISMATCH` | 非 int16、单声道/维度错误、连续硬削峰 | 5 | `chunk_id`、帧和/或样本位置 |
| `E_STALE_CHECKPOINT` | 发布落后、重复、代际已变化 | 6 | 序号、期望序号 |
| `E_INVALID_STAGE` | 跳过前置分片、记录损坏/断序、批流结果异常 | 7 | 期望/实际阶段或序号 |

削顶峰分片没有单独编造错误码：连续饱和样本会破坏特征语义，因此按特征阶段错误 `E_FEATURE_MISMATCH` 返回，并给出首个削峰样本位置、连续长度和阈值。正常检测默认拒绝连续 8 个达到 `±32760` 的样本。

## 检查点、恢复与并发

工作区布局：

```text
<workspace>/
  .lock                              # fcntl 排他锁
  STOP                               # 优雅停止请求
  meta.json                          # 数据集/配置/代际元数据
  generations/00000001/records/
    00000001.json                    # 原子、连续序号的检查点
  cache/<version-fingerprint>.json  # 输入内容 + VAD 配置指纹缓存
  result.json                        # 完成后的确定性摘要
  result.sha256
```

每条记录包含 `chunk_id`、`input_sha256`、`config_fingerprint`、`version_fingerprint`、`endpoint_summary`、单调 `sequence` 和自校验 `record_sha256`。记录先写入同目录临时文件，`fsync` 后 `os.replace`，再 `fsync` 目录；中断留下的临时文件不会被读取。

停止请求只会在当前分片完整发布后生效。恢复时按连续记录复用结果，不依赖内存状态。若内容或配置版本变化，`workspace_status` 会标记受影响分片；数据集变化但部分分片内容未变时，新代际会从缓存复用未受影响分片。发布在文件锁内执行，只接受连续的下一序号。

## 可复现性说明

- 固定种子、固定样本生成、固定帧算法、固定 JSON 键排序和浮点舍入。
- 摘要不包含时间戳、绝对结果路径、代际号、执行耗时或进程信息。
- 相同 Python/依赖版本和相同输入/配置下，摘要逐字节一致。跨不同 BLAS/FFT 实现时，极小浮点差异可能影响末位分数，因此交付中锁定了 numpy/scipy 版本。
- 摘要同时记录 `vad_config_fingerprint` 和包含容差的 `evaluation_fingerprint`；改变容差会改变指纹并重新计算指标。

