# ToneLab 本地音色克隆与发音建模工作台

ToneLab 是一个从零实现、完全本地运行的音色与发音数据工作台。它把确定性
16 kHz / 单声道 / 16-bit PCM 分片、结构版本 2 与版本 1 清单统一到一个内部
声音工程模型，并提供分阶段特征提取、发音建模、定点量化、版本指纹、增量失效、
原子检查点、CLI 与浏览器 HTTP 编辑界面。

本项目不使用真实克隆模型、外部语音服务、GPU、Docker 或容器环境。音色特征来自
确定性频谱包络与谐波统计，发音建模来自确定性共振峰轨迹合成。

## 快速开始

```bash
cd /home/zsl/feedback/uid37fecb20-target-mwnkf3ge-53
./scripts/setup.sh                 # 创建 .venv、安装依赖、生成 data
./scripts/run_demo.sh              # 全量构建、往返、增量/全量一致性、异常验证
.venv/bin/pytest -q                # 40 项测试
```

或手工执行：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install pytest -e .
.venv/bin/python scripts/generate_demo.py --data-dir data
.venv/bin/python -m tonelab.cli demo --data-dir data \
  --workspace workspace/demo --output artifacts/baseline_summary.json
```

最终基准文件为 `artifacts/baseline_summary.json`。当前固定摘要：

```text
seed:                 20261122
shards:               4 (78,240 samples)
v1 stable shard id:   v1-acff54dd9b2d
full build:           78ea332a5ebb02cc8882c72c2a4731d3d1d324c7e26994f106230e01e21df527
baseline summary:     555e0a91b700b91bcb81c4c90ecfa964f6253a56dd9fc572cb0909e02c22d5dd
truncated frames:     7
checkpoints:          16 (sequence 1..16)
project export files: 22
```

## 项目结构

```text
tonelab/                 核心包：模型、格式、特征、建模、量化、指纹、检查点、引擎
tonelab/web/index.html   浏览器交互工作台
scripts/generate_demo.py 固定种子数据生成器
scripts/run_demo.sh      端到端演示与基准摘要生成入口
scripts/run_server.sh    本地 HTTP 服务入口
config/server.json       HTTP host/port/workspace 配置
config/tonelab.example.json  可配置 v1 兼容、量化与清单策略示例
data/                    确定性 WAV、v2/v1 清单、索引与异常注入
workspace/demo/          示例工作区、阶段产物、检查点、导出
tests/                   格式、版本、数值、增量、检查点、停止恢复与 HTTP 测试
artifacts/               baseline_summary.json
docs/ARCHITECTURE.md     设计说明与格式契约
VALIDATION.md            实际验证命令与结果
```

## 统一模型与外部格式

内部模型 `Project -> Shard -> PronUnit` 保存分片标识、音色标识、int16 PCM、
归一化音高曲线、发音单元及共振峰目标区间、结构版本、来源版本与每分片量化元数据。

| 外部形态 | 入口 | 出口 | 备注 |
|---|---|---|---|
| WAV + JSON 侧车 | `load_wav_sidecar` / `/api/import/wav` | `export --format wav` | WAV 采样率、声道、位深与声明值逐项校验/保持 |
| 声音工程 JSON v2 | `import_project_json` / `/api/import` | `export --format json_v2` | 可嵌入 base64 int16 PCM，也可引用 WAV |
| 发音单元标注 | `apply_units_annotation` / `/api/import/units` | `export --format units` | 每单元含 `unit_id/symbol/start_ms/end_ms` |
| 版本 1 清单 | `load_manifest` / `/api/import` | `export --format json_v1` | 整数百分比、整串单元、无共振峰字段 |

`export` 命令执行内部模型到外部形态的直接编解码；`build` 生成在
`workspace/*/exports/build/` 下的项目级导出则消费量化阶段产物，因此项目级导出摘要会
随量化位宽变化。这个区别让格式往返校验保持精确，同时让构建产物严格处于阶段链下游。

格式错配返回 `E_FORMAT_MISMATCH`，并携带 `shard_id`、`declared` 与 `actual`。
清单总量默认按上界校验：实际分片样本不得超过声明总量或分片声明量；可通过
`manifest.exact_total_samples=true` 改为严格相等。

## 分阶段构建与失效传播

阶段固定为 `feature -> model -> quantize -> export`。每阶段有独立内容指纹：

| 变化 | 最早失效阶段 |
|---|---|
| PCM 样本、音色标识、参考基频、特征配置 | `feature` |
| 音高曲线、发音单元、共振峰、结构版本、建模配置 | `model` |
| 量化位宽/缩放因子 | `quantize` |
| 导出配置、WAV 格式 | `export` |

阶段输入包含上一阶段指纹，因此任一上游变化自动传播到全部下游。`status` 与
`fingerprints` 接口返回失效起点、原因、阶段指纹、分片指纹与结构版本指纹。
增量构建只复用指纹和产物摘要均匹配的上游阶段。

## 版本 1 兼容升级

版本 1 导入时：

- 整数百分比通过可配置 `reference_ratio`、`cents` 或 `absolute_offset_hz` 规则换算为归一化曲线，并以 `reference_f0_mean` 得到绝对 F0。
- 整串发音单元按可配置分隔符切分，等分到分片时长，并为每单元生成稳定 `unit_id`。
- 缺失共振峰由按符号分类的可配置缺省规则补齐，同时生成目标区间。
- 无显式 shard id 时，用音色、WAV 文件名、顺序、样本数、音高百分比与单元串生成稳定 ID；不依赖清单文件名。
- 升级后的内部结构版本为 2，保留 `source_structure_version=1`；可再次导出为 v1 并稳定往返。

版本 1 的结构本身不携带音高点时间与任意单元边界，因此 v1 导出是有损时间线编码；
往返校验以 PCM 一致、manifest 二次导出逐字节一致、音高误差不超过整数百分比舍入界为准。

## 数值与量化

- 音高曲线与共振峰轨迹写入定点整数；每个分片记录有效 `pitch_scale`、`formant_scale`、`f0_scale` 与配置值。
- 有效缩放因子受显式量化位宽约束，误差界为 `0.5 / scale`，并记录实际最大误差。
- 音高 `NaN`、`Inf`、越界返回 `E_NUMERIC`，上下文包含分片、点索引与位置。
- 单元负区间、结束早于开始、超出分片时长返回 `E_TIMELINE_INVALID`，包含分片、单元索引和字段位置。
- 分帧尾部按实际样本总数截断，产物记录 `truncated_frames` 与每帧 `end_sample`。

## 检查点、停止与并发

- 每阶段完成后使用同目录临时文件、`fsync` 与 `os.replace` 原子写入检查点。
- 记录包含阶段、分片指纹、结构版本指纹、配置版本、阶段指纹、产物摘要、产物文件与单调序号。
- 发布在 `fcntl` 文件锁内校验连续序号、重复阶段、前置检查点与前置产物文件。
- 落后/重复序号或重复发布返回 `E_STALE_CHECKPOINT`；缺失前置阶段/产物返回 `E_INVALID_STAGE`。
- 停止请求在当前分片四个阶段全部完成后生效；恢复后继续未完成分片。测试验证恢复结果与从零全量构建逐字节一致。

## CLI

```bash
# 导入 v2 + v1 清单
.venv/bin/python -m tonelab.cli init --workspace workspace/demo \
  --manifest data/manifest_v2.json --manifest data/manifest_v1.json --project-id demo

# 合并单个 WAV 侧车或应用单元标注
.venv/bin/python -m tonelab.cli import-wav --workspace workspace/demo \
  --wav exports/manual/wav/shard_001.wav
.venv/bin/python -m tonelab.cli import-units --workspace workspace/demo \
  --path exports/manual/units

# 分阶段构建
.venv/bin/python -m tonelab.cli build --workspace workspace/demo --mode full
.venv/bin/python -m tonelab.cli build --workspace workspace/demo --mode incremental
.venv/bin/python -m tonelab.cli stage --workspace workspace/demo --stage model

# 状态、指纹、检查点
.venv/bin/python -m tonelab.cli status --workspace workspace/demo
.venv/bin/python -m tonelab.cli fingerprints --workspace workspace/demo
.venv/bin/python -m tonelab.cli checkpoints --workspace workspace/demo

# 编辑与单分片操作
.venv/bin/python -m tonelab.cli edit-pitch --workspace workspace/demo \
  --shard shard_001 --point-index 2 --value 1.01
.venv/bin/python -m tonelab.cli edit-unit --workspace workspace/demo \
  --shard shard_002 --unit-index 1 --end-ms 620
.venv/bin/python -m tonelab.cli replace-sample --workspace workspace/demo \
  --shard shard_001 --wav data/shard_001.wav
.venv/bin/python -m tonelab.cli upgrade --workspace workspace/demo \
  --shard v1-acff54dd9b2d

# 导出与往返
.venv/bin/python -m tonelab.cli export --workspace workspace/demo \
  --format json_v2 --out exports/manual/project.json
.venv/bin/python -m tonelab.cli roundtrip --workspace workspace/demo

# 增量对全量逐字节校验
.venv/bin/python -m tonelab.cli consistency --workspace workspace/demo \
  --mutation unit --shard shard_002 --unit-index 1 --end-ms 620

# 停止/恢复、清单校验、HTTP 服务
.venv/bin/python -m tonelab.cli stop --workspace workspace/demo
.venv/bin/python -m tonelab.cli resume --workspace workspace/demo
.venv/bin/python -m tonelab.cli validate --manifest data/manifest_v1.json
./scripts/run_server.sh
```

自定义规则可传 `--config config/tonelab.example.json`，或只写需要覆盖的字段。

## HTTP 工作台

```bash
./scripts/run_server.sh
# 浏览器打开 http://127.0.0.1:8765/
```

`config/server.json` 默认为 `127.0.0.1:8765`；`TONELAB_HOST`、`TONELAB_PORT`、
`TONELAB_WORKSPACE` 可覆盖。主要接口：

- `GET /api/status`、`/api/fingerprints`、`/api/checkpoints`
- `POST /api/import`、`/api/import/wav`、`/api/import/units`
- `POST /api/build`、`/api/stage`、`/api/edit/pitch`、`/api/edit/unit`、`/api/replace-sample`、`/api/upgrade`
- `POST /api/stop`、`/api/resume`、`/api/export`、`/api/roundtrip`、`/api/consistency`

浏览器页面提供构建、状态、音高/单元编辑、样本替换、版本升级、停止/恢复、三种格式
导出、往返审计与增量一致性检查入口。

## 错误码

`E_FORMAT_MISMATCH`、`E_MANIFEST_MISMATCH`、`E_TIMELINE_INVALID`、`E_NUMERIC`、
`E_INCREMENTAL_MISMATCH`、`E_STALE_CHECKPOINT`、`E_INVALID_STAGE`、`E_VALIDATION`、
`E_NOT_FOUND`、`E_BAD_REQUEST`。CLI 与 HTTP 返回同构的 `error` 对象。

详细设计见 `docs/ARCHITECTURE.md`，实际测试与演示结果见 `VALIDATION.md`。

