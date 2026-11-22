# 多源时序遥测增量对齐工作台

**Telemetry Alignment Workbench** 是一个完全本地运行、确定性可复现的多源遥测对齐应用。它从本地 JSONL 分片与 YAML 配置出发，依次完成：

1. **规范化与统一时间轴**：严格校验时间戳，清洗质量异常与越界值，生成单调毫秒网格和清洗报告。
2. **重采样与插值**：按源执行 `ffill`、`linear`、`nearest`，记录每个网格点的来源时间戳、方法与外推标记。
3. **多源融合与冲突裁决**：按优先级与可配置权重融合，记录参与源、各源贡献与确定性仲裁结果；缺口显式保留。
4. **摘要导出**：输出内容确定、可逐字节复现的 `alignment_summary.json`。

应用同时提供 **命令行**、**本地 HTTP API** 与**浏览器控制台**，支持 V1 配置兼容升级、阶段级内容指纹、增量重建、真实文件锁检查点、优雅停止恢复和并发发布保护。不使用 Docker，不访问外部服务。

---

## 快速开始

环境要求：Python 3.11+、NumPy、PyYAML。

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
./run_demo.sh
./run_server.sh
```

打开 `http://127.0.0.1:8765/`。也可自定义端口：

```bash
PORT=8899 ./run_server.sh
```

演示会生成：

- `demo/workspace/`：主演示工作区，完成一次完整运行、一次 Source A 增量重建，并导出 V1→V2 配置。
- `demo/v1_workspace/`：V1 兼容运行与升级后等价性验证。
- `demo/recovery_workspace/`：在 `resample` 后停止、再恢复的验收工作区。
- `demo/reports/demo_report.json`：固定摘要、增量复用、停止恢复和 V1 往返的实际结果。

若只想重新生成并运行演示：

```bash
python -m telemetry_align demo --output-root demo
```

若只想初始化一个新的演示工作区：

```bash
python -m telemetry_align --root demo/workspace init-demo
```

---

## 实际验证结果

本机 Python 3.14.4、NumPy 2.2.1、PyYAML 6.0.3 下实际执行结果：

| 验证项 | 结果 |
| --- | --- |
| 自动化测试 | **30/30 通过**，约 4.6 秒 |
| 固定摘要 SHA-256 | `716aaa760d79c41de6ebffb7f87467eed99b7e29bac84a21c7381e44cf728b16` |
| 统一网格 | 31 点，`0..3000ms`，步长 `100ms` |
| 融合状态 | fused 26、single_source 2、gap 3 |
| 冲突裁决 | 52 个通道级冲突事件 |
| 缺口 | 6 个通道缺口事件，对应 3 个全源缺失网格点 |
| V1→V2 往返 | 语义指纹一致，网格与融合摘要一致 |
| 增量重建 | 最早失效阶段 `normalize`；B/C 的归一化与重采样产物复用；检查点 `1..8` 连续 |
| 停止恢复 | 停在 `resample`，恢复执行 `fusion → summary`；最终网格、融合、摘要逐字节一致；检查点 `1..4` 连续 |
| 并发发布 | 两个进程竞争同序号，仅一个被接受，另一个返回 `E_STALE_CHECKPOINT` |

演示数据采用固定种子 `20261122`。摘要固定值由测试 `tests/test_demo.py` 守护，任何会改变对齐语义的代码变更都会使该测试失败。

---

## 演示数据与配置

### 三源样本

| 源 | 采样步长 | 有效区间 | 独立缺口 |
| --- | ---: | --- | --- |
| `sensor_a` | 100ms | 0..3000ms | 1000..1200ms、2200..2400ms |
| `sensor_b` | 150ms | 0..3000ms | 1500..1650ms、2200..2400ms |
| `sensor_c` | 200ms | 0..2800ms | 2200..2400ms |

三源共同缺失 `2200..2400ms`，因此保留 3 个全源缺失网格点。样本还包含一条质量拒绝记录、一条越界温度值、优先级并列的 `sensor_b`/`sensor_c`，以及中英混排字段标签：

```json
{"sensor_id":"sensor_a","timestamp_ms":0,"channel_values":{"rh_raw":49.1,"temp_raw":21.8},"quality":"good","channel_labels":{"rh_raw":"湿度 Humidity / raw","temp_raw":"温度 Temp / raw"}}
```

### V2 配置要点

```yaml
schema_version: 2
grid: {step_ms: 100, start_ms: 0, end_ms: 3000}
tolerance_ms: 200
resample: {max_gap_ms: 250, allow_extrapolation: true}
interpolation:
  default: linear
  per_source: {sensor_b: ffill, sensor_c: nearest}
sources:
  sensor_a:
    priority: 30
    channel_mapping: {temp_raw: temperature_c, rh_raw: humidity_pct}
    valid_ranges:
      temperature_c: {min: -40, max: 125}
      humidity_pct: {min: 0, max: 100}
    quality_accept: [good, fair]
fusion:
  mode: weighted_mean
  tie_break: source_id_lexicographic
  weights: {sensor_a: 0.5, sensor_b: 0.3, sensor_c: 0.2}
```

### V1 兼容

V1 使用整数百分比容差，且 `sources.*` 不包含 `channel_mapping`：

```yaml
schema_version: 1
tolerance_percent: 20
compatibility:
  tolerance_basis_ms: 1000
  tolerance_rounding: half_up
  default_channel_mapping: {temp_raw: temperature_c, rh_raw: humidity_pct}
sources:
  sensor_a: {priority: 30, valid_ranges: {...}, quality_accept: [good, fair]}
```

导入时换算为 `200ms`，并在升级结果中记录：

- `upgraded_from: 1`
- `degraded: true`
- 百分比、基准、取整规则、公式、换算结果
- 每个源的映射补齐规则与最终映射

V1 可直接运行；也可导出 V2 后再次导入，两者语义指纹、网格与融合结果一致。

---

## 命令行

```bash
# 初始化与完整运行
python -m telemetry_align --root demo/workspace init-demo
python -m telemetry_align --root demo/workspace align

# 分阶段运行、停止与恢复
python -m telemetry_align --root demo/workspace align --until-stage fusion
python -m telemetry_align --root demo/workspace stop --after-stage resample
python -m telemetry_align --root demo/workspace resume

# V1 运行与显式升级
python -m telemetry_align --root demo/workspace align --config v1
python -m telemetry_align --root demo/workspace upgrade-v1

# 查询与导出
python -m telemetry_align --root demo/workspace status
python -m telemetry_align --root demo/workspace fingerprints
python -m telemetry_align --root demo/workspace conflicts
python -m telemetry_align --root demo/workspace gaps
python -m telemetry_align --root demo/workspace cleaning
python -m telemetry_align --root demo/workspace checkpoints
python -m telemetry_align --root demo/workspace verify
python -m telemetry_align --root demo/workspace export --output out/alignment_summary.json

# 增量重建
python -m telemetry_align --root demo/workspace mutate --sensor sensor_a

# 导入本地文件
python -m telemetry_align --root demo/workspace import \
  --source sensor_a=data/sources/sensor_a.jsonl \
  --source sensor_b=data/sources/sensor_b.jsonl \
  --source sensor_c=data/sources/sensor_c.jsonl \
  --config-file v1=data/configs/config_v1.yaml \
  --config-file v2=data/configs/config_v2.yaml \
  --active v2

# HTTP 控制台
python -m telemetry_align --root demo/workspace serve --host 127.0.0.1 --port 8765
```

所有命令输出结构化 JSON；失败时输出稳定错误码并以非零状态退出。

---

## 本地 HTTP API

服务默认只绑定 `127.0.0.1`，端口占用返回 `E_PORT_IN_USE`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/`、`/static/*` | 浏览器控制台 |
| GET | `/api/health` | 本地服务健康状态 |
| GET | `/api/status?config=v2` | 阶段状态、两侧指纹、检查点、计数、失效原因 |
| GET | `/api/fingerprints` | 阶段与源级指纹 |
| GET | `/api/conflicts` | 冲突裁决与贡献明细 |
| GET | `/api/gaps` | 通道缺口与全源缺失点 |
| GET | `/api/cleaning` | 规范化清洗报告 |
| GET | `/api/summary` | 当前对齐摘要（过期时 `E_STALE_RESULT`） |
| GET | `/api/checkpoints` | 检查点账本摘要 |
| GET | `/api/checkpoints/verify` | 哈希链、锚点、产物摘要校验 |
| POST | `/api/demo/init` | 生成确定性演示，可同时运行 |
| POST | `/api/run` | 运行；支持 `config`、`until_stage` |
| POST | `/api/stop` | 请求在阶段边界停止 |
| POST | `/api/resume` | 恢复最近运行 |
| POST | `/api/increment` | 修改一个源并增量重建 |
| POST | `/api/import` | 以内联内容或工作区路径导入三源与双版本配置 |
| POST | `/api/export` | 导出摘要到工作区文件 |
| POST | `/api/upgrade-v1` | 导出并验证 V1→V2 语义等价升级 |

示例：

```bash
curl -s http://127.0.0.1:8765/api/status | python -m json.tool
curl -s -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' \
  -d '{"config":"v2"}' | python -m json.tool
```

---

## 错误码

| 错误码 | 含义 |
| --- | --- |
| `E_TIME_INVALID` | 时间戳重复/非单调/非整数；含 `sensor_id`、时间戳与诊断文件 |
| `E_GRID_INVALID` | 网格步长为零、负数或大于容差；时间范围非法 |
| `E_CONFIG_INVALID` | YAML、结构版本、字段、映射或融合规则非法 |
| `E_SOURCE_INVALID` | JSONL 记录、`sensor_id`、`channel_values` 或数值格式非法 |
| `E_INVALID_STAGE` | 阶段缺失前置产物、首次发布不是 `normalize` 或停止阶段非法 |
| `E_STALE_CHECKPOINT` | 序号落后/重复/跳跃、跨阶段错位、配置变化、回退或篡改 |
| `E_STALE_RESULT` | 当前源/配置已变化，旧结果不得作为当前结果读取 |
| `E_RUN_ACTIVE` | 工作区已有对齐进程持有运行锁 |
| `E_NOT_FOUND` | 工作区、配置、分片或接口不存在 |
| `E_PORT_IN_USE` | HTTP 端口已被占用 |

`E_STALE_CHECKPOINT.details.kind` 进一步区分 `duplicate`、`rollback`、`sequence_gap`、`cross_stage`、`config_changed`、`tampered`。

---

## 增量与检查点设计

### 阶段指纹

```text
normalize-source = H(raw shard bytes, mapping, valid ranges, quality rules)
normalize        = H(grid definition, all normalize-source fingerprints)
resample-source  = H(normalize-source, grid fingerprint, interpolation, gap, tolerance)
resample         = H(grid, all resample-source fingerprints)
fusion           = H(resample, priorities, weights, fusion mode, channel labels)
summary          = H(fusion, export options)
```

失效归因：

- 源分片、映射、有效区间、质量规则、网格定义 → 最早 `normalize`
- 插值方式、最大缺口、对齐容差 → 最早 `resample`
- 优先级、权重、融合模式、标签 → 最早 `fusion`
- 导出选项 → 最早 `summary`

每个阶段按当前指纹查找缓存；只有指纹完全匹配的产物才能复用。未受影响源的归一化与重采样产物可以跨阶段源变更继续复用。旧产物即使仍留在磁盘上，也不会被当前目录引用。

### 原子检查点

每个阶段完成时写入一条记录，至少包含：

```json
{
  "seq": 3,
  "stage": "fusion",
  "config_version": 2,
  "config_fingerprint": "...",
  "input_digest": "...",
  "artifact_digest": "...",
  "fingerprint": "...",
  "catalog": {...},
  "prev_record_hash": "...",
  "record_hash": "..."
}
```

持久化文件：

- `state/checkpoints.jsonl`：完整账本，整文件临时写入、`fsync`、`os.replace`。
- `state/checkpoints.anchor.json`：独立锚点，记录账本摘要、头部序号、头记录哈希与配置指纹。
- `state/checkpoint.pending.json`：账本与锚点两次原子替换之间的恢复标记；若进程中断，下次持锁读取会完成锚点修复。

发布前在真实 `flock` 下重新读取账本，要求 `seq == head.seq + 1`、同运行阶段严格相邻、前置阶段产物摘要匹配。并发发布、旧序号、跳跃、跨阶段错位均被拒绝。

停止请求只写入边界标记；运行器完成当前阶段、发布检查点后再退出。恢复复用同一 `run_id` 与已有上游检查点，因此最终网格、融合与摘要与一次完整运行逐字节一致。

---

## 项目结构

```text
telemetry_align/
  canonical.py       规范 JSON、摘要、原子文件、flock
  config.py          V1/V2 解析、百分比换算、显式升级、指纹
  samples.py         固定种子三源样本、双版本配置、受控源变更
  normalize.py       严格校验、清洗、单调网格
  resample.py        三种插值、缺口限制、线性钳制、来源追踪
  fusion.py          优先级/权重融合、并列仲裁、冲突与缺口
  summary.py         确定性摘要构建
  checkpoint.py      原子账本、锚点、哈希链、并发发布保护
  workspace.py       增量计划、阶段执行、停止恢复、查询与导出
  http_api.py        本地 HTTP API 与静态资源
  demo.py            完整可重复演示
  cli.py             命令行入口
  static/            浏览器控制台
tests/               30 项 unittest 验收测试
demo/                生成器产物与演示报告
```

更详细的内部状态机和设计约束见 `docs/ARCHITECTURE.md`。

