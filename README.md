# SonicSep：本地确定性分块声源分离

SonicSep 是一个从零实现、完全本地运行的短音频分块声源分离系统。输入为固定种子生成的 16 kHz / 单声道 / 16 位 PCM WAV 分片，系统按五个阶段完成 STFT、频带特征、掩码初始化、迭代掩码精化与重叠相加重建，并重点实现可验证的频带增量更新、版本失效传播、原子检查点、优雅停止与恢复。

当前工作区已包含 3 个可重复生成的分片、16 个连续频带、完整 Python 实现、HTTP 交互页面、CLI、自动测试和可重复演示报告。所有分离计算只使用本地 CPU；随机性仅来自固定种子的样本生成器与固定整数哈希投影。

## 已验证结果

最终演示在 Python 3.11.18 + numpy 2.4.6 环境运行，结果写入 `artifacts/demo_report.json`：

- 演示总摘要 `demo_digest`：`8ae78ad8fd47f8f5d8f374a5b73dd759572af76ec8278b882fb498719bde5f2d`
- 3 个分片基线结果摘要：
  - `slice-001`：`5be4166f7c725ed66519a4c214b90f2c6a9e0f6384cfadd221b70774e33e642d`
  - `slice-002`：`1d19e8d94bf38237c768babd876a2e7b4d2897212be0a00cd943cb6fc2135fdc`
  - `slice-003`：`1acc2f2f105e14936d7fff9d210c5d98a7eae737277098782ce1d595f5bc3f51`
- 停止/恢复：第 11 个不可拆分单元后停止，恢复结果与独立从零全量运行逐字节一致。
- 频带增量：仅更新受影响频带 `[1, 2, 3, 4, 5]`，STFT 与特征阶段复用，提交前全量比对通过。
- 失效传播：`mask_iterations` 从 4 改为 5 后，最早失效阶段识别为 `mask_refine`，`stft`、`features`、`mask_init` 均显示为 `reused`。
- 检查点：停止恢复后序号 `576`，最终序号 `1068`；可恢复前缀连续，无损坏、无重复、无跳跃。
- 最终一致性：配置变更后重建所有受影响分片，三个分片再次通过从零全量摘要校验。
- 自动测试：20 项通过。命令见文末。

## 环境安装

任务要求 Python 3.11。项目已在工作区内创建 `.venv`，并安装固定依赖：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

锁定版本：

```text
Python 3.11.18
numpy 2.4.6
soundfile 0.14.0
PyYAML 6.0.4
pytest 9.2.0
```

本地 HTTP 默认端口为 `127.0.0.1:47321`，已在当前环境确认可绑定；无需 Docker、GPU、外部模型或外部服务。

## 快速开始

```bash
# 1. 生成固定种子样本（也可直接使用仓库已有 samples/）
.venv/bin/python scripts/generate_samples.py

# 2. 导入样例与配置
.venv/bin/python -m sonic_sep import

# 3. 五阶段构建全部 3 个分片
.venv/bin/python -m sonic_sep build

# 4. 查看状态、版本指纹、失效原因、逐轮掩码变化
.venv/bin/python -m sonic_sep status
.venv/bin/python -m sonic_sep version
.venv/bin/python -m sonic_sep invalidations
.venv/bin/python -m sonic_sep mask-changes

# 5. 频带增量更新（含从零全量逐字节验证）
.venv/bin/python -m sonic_sep incremental \
  --slice slice-001 --source-id 0 --bands 2:5 --f0-min 165 --f0-max 205

# 6. 本地 HTTP 应用
.venv/bin/python -m sonic_sep serve --port 47321
# 浏览器打开 http://127.0.0.1:47321/

# 7. 一键演示并生成 artifacts/demo_report.json
.venv/bin/python -m sonic_sep demo
```

`demo` 默认重置 `state/`，然后依次验证停止/恢复、增量全量等价和版本失效传播。重复运行会得到相同 `demo_digest`（同一固定环境内）。

演示末尾会模拟“迭代次数从 4 改为 5”的版本变化，因此运行后工作区配置快照为 5 次迭代；若要恢复默认样例状态，再执行一次 `import` 与 `build` 即可。

## 项目结构

```text
sonic_sep/               Python 包
  stft.py                分帧、汉宁窗、零填充标记、谱摘要
  features.py            频带对数能量与频谱平坦度
  masks.py               先验、固定哈希投影、掩码初始化与逐轮精化
  reconstruct.py         分块逆变换、重叠权重、相加重建与钳制统计
  checkpoints.py         文件锁、原子检查点、序号连续性、损坏恢复
  pipeline.py            五阶段编排、增量、版本失效、停止/恢复、全量校验
  workspace.py           导入、配置快照、源先验覆盖、停止信号
  server.py              标准库 HTTP API 与静态应用服务
  cli.py                 命令行入口
  demo.py                可重复验收演示
configs/default.yaml     默认分离配置
samples/                 3 个 WAV 与 manifest.json
state/                   运行时缓存、输出、版本与检查点（可由 demo 重建）
web/                     单页交互式专业应用
scripts/                 独立生成与演示脚本
tests/                   自动验收测试
artifacts/demo_report.json
```

## 输入数据

`samples/manifest.json` 声明：

- `seed`、`sample_rate`、`slice_count`、`total_samples`
- 每个分片的 `slice_id`、`file`、`n_samples`、时长、`source_count`
- 每个声源的 `f0_min_hz`、`f0_max_hz`、`visible`
- 生成器额外写入 WAV 实际 SHA-256；内容变化时会触发最早 STFT 阶段失效，而不是阻止版本检测。

当前数据：

| 分片 | 样本数 | 时长 | 声源数 |
|---|---:|---:|---:|
| slice-001 | 40,000 | 2.5 s | 2 |
| slice-002 | 48,000 | 3.0 s | 3 |
| slice-003 | 56,000 | 3.5 s | 2 |

总样本数 `144,000`，所有 WAV 为 16 kHz、单声道、`PCM_16`。

## 配置

`configs/default.yaml`：

```yaml
sample_rate: 16000
frame_length: 512
hop_length: 128
band_count: 16
mask_iterations: 4
max_sources: 3
block_overlap_percent: 25
block_frames: 16
projection_seed: 20261123
```

校验规则：帧长、帧移、频带数、上限、重叠百分比和块帧数必须为整数；帧长/帧移必须为正，帧移不得大于帧长；奈奎斯特频率区间必须能被 `band_count` 整除；重叠比例为 `0..99`。配置错误返回 `E_INVALID_CONFIG`，包含 `slice_id` 和 `param`。

## 五阶段算法

### 1. 分帧与 STFT

- 对 PCM 样本按 `/32768` 转为浮点，使用 `numpy.hanning(frame_length)` 加窗。
- 帧数 `ceil(n_samples / hop_length)`，每帧记录 `frame_index` 与样本偏移。
- 最后一帧超出输入末尾时确定性补零，并记录 `padded=True` 与有效长度。
- 输出 `complex128` 谱矩阵 `(frames, frame_length/2 + 1)`；摘要与样本/配置绑定，重复运行逐字节一致。

### 2. 频带特征

- 按 Nyquist 区间等分为连续频带，FFT bin 以 `floor(freq / band_width)` 归属，保证连续覆盖。
- 每个帧/频带计算对数能量与频谱平坦度：
  - `log(sum(power) + 1e-15)`
  - `exp(mean(log(power + eps))) / (mean(power) + eps)`
- 特征形状为 `(frames, bands, 2)`。维度不符、空频带、NaN/Inf 都返回 `E_NUMERIC`，包含 `band_index` 与 `frame_index`，绝不静默填充。

### 3. 掩码初始化

- 参考基频区间先验使用到频带区间的高斯距离，远距离频带量化为 0，避免无关频带被误标为受影响。
- 使用 SplitMix64 风格的固定整数哈希投影，不依赖 Python 随机哈希或进程状态。
- 每帧/频带跨声源归一化。不可见声源降低先验权重，但仍保留可分离性。

### 4. 迭代掩码精化

- 每轮按固定 `source_id` 顺序更新；同帧同频带内以其他声源掩码和进行确定性抑制。
- 每轮记录每个频带的掩码绝对变化量，并生成 `round_changes_digest`。
- 停止请求只在当前分块/掩码轮次单元完成后生效，轮次进度原子落盘。

### 5. 分块重叠相加重建

- 基础分块为连续帧块；`block_overlap_percent` 生成相邻块的帧域重叠 halo。
- 重叠帧按覆盖块数分配确定性权重，所有块权重之和为 1。
- 每个声源使用掩码谱做逆变换，乘合成窗和块权重后写入块贡献文件；最终按样本位置累加并做 OLA 归一化。
- 每个声源输出整数样本数严格等于输入分片样本数。
- 转换为 int16 时统计 `-32768..32767` 之外被钳制的样本数（总计与按声源）。

## 增量更新协议

CLI：

```bash
.venv/bin/python -m sonic_sep incremental \
  --slice slice-001 --source-id 0 --bands 2:5 --f0-min 165 --f0-max 205
```

HTTP：

```bash
curl -sS -X POST http://127.0.0.1:47321/api/incremental \
  -H 'Content-Type: application/json' \
  -d '{"slice_id":"slice-001","source_id":0,"bands":[2,5],"f0_min_hz":165,"f0_max_hz":205}'
```

行为：

1. 载入已完成的 STFT、特征、掩码与重建缓存。
2. 根据旧/新参考基频区间计算实际先验变化频带，并并入请求频带区间。
3. 只重算这些频带的掩码初始化与逐轮精化；STFT 与频带特征不重算。
4. 重建阶段刷新受影响时间块及其相邻重叠贡献。
5. 提交前在内存中执行同参数的从零全量计算，比较最终 int16 波形、掩码摘要、每轮变化量摘要和重建统计。
6. 不一致时不提交，返回 `E_INCREMENTAL_MISMATCH`，包含 `band_index`、`block_index` 和阶段名。

演示中源 0 的 f0 区间扩大后，只识别到频带 `[1,2,3,4,5]` 受影响，`stft` 与 `features` 均保持 `reused`，最终 `full_equivalent=true`。

## 检查点与持久化

运行状态位于 `state/`：

- `state/slices/<slice>/cache/`：阶段数组快照
- `state/slices/<slice>/reconstruction/`：各重叠块贡献
- `state/slices/<slice>/outputs/`：每个声源的 WAV
- `state/slices/<slice>/summary.json`：结果摘要、逐轮变化、钳制统计
- `state/slices/<slice>/version.json`：阶段输入/产物版本指纹
- `state/slices/<slice>/invalidations.json`：失效历史
- `state/checkpoints/`：单调序号检查点记录

每个检查点记录包含：

```json
{
  "schema_version": 1,
  "sequence": 42,
  "block_sequence": 1,
  "slice_id": "slice-001",
  "stage": "mask_refine",
  "block_index": 3,
  "frame_start": 48,
  "frame_end": 64,
  "bands": [0, 15],
  "input_fingerprint": "...",
  "config_version": "...",
  "product_digest": "...",
  "round": 2,
  "stage_complete": false,
  "checksum": "..."
}
```

可靠性规则：

- 检查点和缓存均先写同目录临时文件、`fsync`、再 `os.replace`，目录也执行 `fsync`。
- 发布检查点使用真实 `fcntl.flock` 独占文件锁。
- 全局序号必须从可恢复前缀连续 +1；显式落后、重复序号返回 `E_STALE_CHECKPOINT`。
- 同一分块阶段另有单调 `block_sequence`。
- 扫描时忽略截断 JSON、校验和不符、非记录文件和冲突重复序号；完全相同的重复副本忽略额外副本并保留规范文件名。
- 中断留下的 `.tmp` 文件不会被当作检查点。

HTTP/CLI 均可观察：

```bash
.venv/bin/python -m sonic_sep checkpoints
.venv/bin/python -m sonic_sep invalidations
curl -sS http://127.0.0.1:47321/api/checkpoints
```

## 优雅停止与恢复

```bash
# 一个终端构建；另一个终端写入停止请求
.venv/bin/python -m sonic_sep build
.venv/bin/python -m sonic_sep stop

# 恢复（清除 STOP 并从缓存继续）
.venv/bin/python -m sonic_sep resume
```

HTTP：

```bash
curl -sS -X POST http://127.0.0.1:47321/api/stop -d '{}'
curl -sS -X POST http://127.0.0.1:47321/api/resume -d '{"slice_id":"slice-001"}'
```

停止请求只在不可拆分的块/轮次单元边界检查。恢复后输出、掩码摘要和逐轮变化量与独立全量运行逐字节一致；检查点序号连续推进。

## 版本检测与失效传播

阶段输入指纹链：

```text
STFT(audio hash, frame, hop)
  → Features(band partition)
  → MaskInit(source f0/visibility, projection)
  → MaskRefine(iterations)
  → Reconstruct(overlap, block layout, output config)
```

失效检测比较上一版 `version.json` 与当前阶段输入，定位最早变化阶段：

| 变化 | 最早失效阶段 | 上游复用 |
|---|---|---|
| WAV 内容或帧长/帧移 | `stft` | 无 |
| 频带数/频带划分 | `features` | STFT 复用 |
| 参考基频/可见性 | `mask_init` | STFT、特征复用 |
| 掩码迭代次数 | `mask_refine` | STFT、特征、初始化复用 |
| 重叠比例/重建块布局 | `reconstruct` | 前四阶段复用 |

失效记录同时给出原因、最早阶段、受影响频带列表和压缩频带区间。`/api/invalidations`、`/api/version` 与页面均可见。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 交互式控制台 |
| GET | `/api/health` | 健康检查与端口 |
| POST | `/api/import` | 导入 manifest/config |
| POST | `/api/build` | 分阶段构建，可带 `slice_id`、`stop_after_units` |
| POST | `/api/incremental` | 频带/声源增量更新并全量验证 |
| GET | `/api/status` | 各分片状态、阶段产物、检查点 |
| GET | `/api/version` | 当前与持久化阶段指纹 |
| GET | `/api/invalidations` | 失效原因与受影响频带 |
| GET | `/api/mask-changes` | 逐轮掩码变化量 |
| GET | `/api/result` | 完整结果摘要 |
| GET | `/api/checkpoints` | 检查点恢复视图 |
| POST | `/api/checkpoints/publish` | 文件锁下发布检查点 |
| POST | `/api/stop` | 请求优雅停止 |
| POST | `/api/resume` | 清除停止并恢复构建 |
| POST | `/api/verify` | 与从零全量重算比对 |
| GET | `/api/audio/<slice>/<file>` | 播放/下载分离 WAV |

错误统一返回：

```json
{"ok": false, "error": {"code": "E_NUMERIC", "message": "...", "slice_id": "slice-001", "band_index": 5, "frame_index": 37}}
```

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_INVALID_CONFIG` | 配置字段非法；含 `slice_id`、`param` |
| `E_NUMERIC` | 特征/频谱/先验数值或维度错误；含 `band_index`、`frame_index` |
| `E_MANIFEST` | manifest、WAV 格式或一致性错误 |
| `E_INCREMENTAL_MISMATCH` | 增量结果与全量不一致；含频带/块坐标 |
| `E_STALE_CHECKPOINT` | 检查点序号落后、重复或分块序号不连续 |
| `E_NOT_FOUND` | 分片、声源、路由或文件不存在 |
| `E_CONFLICT` | 缓存未完成或已有并发构建 |

## 自动测试

```bash
.venv/bin/python -m pytest -q
```

覆盖：

- WAV/manifest 一致性、固定种子可重复性
- STFT 确定性、零填充标记、非法帧长/帧移
- 特征 NaN/Inf、维度、空/反向 f0 区间
- 五阶段全量构建与独立工作区摘要一致
- 频带增量与全量逐字节一致
- 优雅停止/恢复与一次性全量一致
- 内容、频带划分、迭代次数、重叠比例的失效传播
- 检查点文件锁、并发发布、损坏/重复恢复、陈旧序号
- HTTP 导入、构建、状态、增量、音频、停止/恢复
- 两次完整演示 `demo_digest` 一致

最近一次结果：`20 passed`。

## 说明与边界

- 本项目是确定性的参考先验/频谱掩码分离器，不是神经网络模型；目标是完整实现任务中的本地分块流水线、增量复用、持久化与一致性协议。
- 摘要绑定 numpy 的 FFT 实现。为保证跨运行逐字节复现，应在锁定的 Python/numpy 环境中运行。
- `state/` 是运行数据，可随时通过 `demo` 或 `build` 重建；`samples/` 与 `configs/` 是输入真值样例。
- `demo` 会重置 `state/`，但不会修改业务代码或 `samples/` 中的生成规则。
