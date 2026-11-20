# Storypipe：确定性分镜时间轴合成管线

Storypipe 是一个完全本地运行的四阶段分镜合成管线。它读取固定种子生成的分镜 PNG、JSONL 脚本和配置，完成帧调度、运镜合成、转场混合与编码封装。全过程只使用 NumPy、Pillow 和 Python 标准库，不调用生成模型、GPU、网络或外部图像服务。

## 快速开始

```bash
python3 -m pip install -r requirements.txt

python3 -m storypipe generate --out artifacts/samples
python3 -m storypipe run --workspace artifacts/runs/small \
  --sample artifacts/samples/small --fresh
python3 -m storypipe run --workspace artifacts/runs/large \
  --sample artifacts/samples/large --fresh
python3 -m storypipe compare \
  --small artifacts/runs/small \
  --large artifacts/runs/large \
  --out artifacts/reports/scale_verification.json

pytest
```

也可以安装为命令：

```bash
python3 -m pip install -e .
storypipe --help
```

## 输入与样本

JSONL 每行是一条分镜：

```json
{"shot_id":"shot-000","image":"shot_000.png","prompt":"雨后街角 / cinematic light","camera_move":"pan","duration_frames":12,"fps":29.97,"transition":"crossfade"}
```

支持字段：

- `camera_move`：`static`、`pan`、`zoom`，同时接受 `静止`、`平移`、`缩放`。
- `transition`：`hard_cut`、`crossfade`，同时接受 `硬切`、`交叉淡化`。
- `fps`：整数、小数或分数字符串；调度内部转换为精确分数。
- `prompt`：只作为版本指纹与业务元数据，不驱动真实生成模型。

内置生成器产生两档确定性样本：

- `small`：5 个分镜、96x54 PNG，混合 24/29.97/25/30/23.976 fps。
- `large`：36 个分镜、96x54 PNG，混合帧率与不同时长。

样本使用固定种子与固定绘制顺序，重复运行 `generate` 会得到同一组 PNG、脚本、配置和 `sample_set_manifest.json`。

## 四阶段设计

### 1. 帧调度

`storypipe/schedule.py` 使用 `fractions.Fraction` 保存精确累计时间，再按 half-up 规则分别量化为微秒和毫秒。每帧记录：

- 全局帧号、来源镜头、镜头内帧号；
- 精确起止时间、微秒起止时间、毫秒起止时间。

同一镜头内上一帧的 `end_exact/end_us` 与下一帧的 `start_exact/start_us` 使用同一精确分数计算，因此不会因浮点累加或混合帧率产生边界漂移。零持续帧、零或负帧率返回 `E_INVALID_TIMELINE` 并附带 `shot_id`。

### 2. 运镜帧合成

`storypipe/motion.py` 按镜头与本地帧进度生成 3x3 浮点变换矩阵：

- 静止：单位矩阵；
- 平移：配置中的起止相对偏移线性插值；
- 缩放：缩放比例、中心点和漂移轨迹线性插值。

所有帧使用双线性采样。目标像素映射到源图后先裁剪到 `[0, width-1] x [0, height-1]`，再计算四个邻点的浮点权重。输出全程为浮点，最终才在编码阶段量化。

### 3. 转场混合

`storypipe/transition.py` 为每条全局帧建立混合计划。硬切不增加混合源。交叉淡化配置窗口为 `W` 帧，在交界两侧共覆盖 `2W` 帧：

- 出镜头尾部第 `k` 帧的 Alpha 为 `(k+1)/(2W)`；
- 入镜头头部第 `k` 帧的 Alpha 为 `(W+k+1)/(2W)`；
- 每个输出帧保留来源镜头、转场 ID、角色、窗口和完整混合源权重。

窗口超过任一侧镜头帧数时返回 `E_TRANSITION_OVERFLOW`，并在错误中给出两侧镜头帧数。混合使用浮点权重，量化阶段执行 `clip`、乘以 255、加 0.5、向下取整。

### 4. 编码封装

`storypipe/stage_io.py` 将最终浮点帧量化为 RGB8 PNG，并写出：

- `frames/frame_XXXXXXXX.png`：全局帧号命名的帧序列；
- `frame_summaries.jsonl`：逐帧摘要；
- `manifest.json`：清单、统计、像素摘要、PNG 摘要、输出树摘要和固定运行摘要。

编码先写入事务目录，全部成功后整体替换 `output`。中断或异常不会留下可见的半成品清单。

## 缓存策略选择

`storypipe/cache_policy.py` 根据分镜数、总帧数、图像尺寸、最大镜头帧数和内存预算，对三种策略评分：

- `per_frame_recompute`：逐帧重算，最小常驻帧集；
- `segment_cache`：按相邻镜头段缓存，峰值受预算限制；
- `full_cache`：全量驻留，仅在安全且规模足够时作为候选。

选择结果写入 `algorithm_plan.json`，其中包含：

- 选中策略和业务理由；
- 每个候选的估算峰值、工作量、内存/计算/简单度评分；
- 淘汰原因，例如超出预算、对小规模不必要、或总分较低。

默认样本实测：

- 小档选择 `per_frame_recompute`，帧缓冲峰值 62,208 字节；
- 大档选择 `segment_cache`，帧缓冲峰值 2,115,072 字节；
- 两档预算均为 8,388,608 字节，所有阶段均满足预算。

## 版本指纹与增量失效

四阶段各自保存输入组件摘要，并形成链路指纹：

| 变化项 | 最早失效阶段 |
| --- | --- |
| 帧率、持续帧数、调度配置 | `schedule` |
| 图像内容、提示词、运镜类型、运镜配置、缓存预算/选择 | `compose` |
| 转场类型、交叉淡化窗口、转场配置 | `transition` |
| 编码配置 | `encode` |

`inspect()`、CLI `fingerprints` 和 HTTP `/v1/fingerprints` 会展示：

- 当前总指纹与各阶段指纹；
- 最早失效阶段及全部下游阶段；
- 每个阶段发生变化的组件路径；
- 已有检查点的序号、输入指纹和产物摘要。

未失效的上游阶段会直接复用检查点和产物。例如仅修改提示词时，`schedule` 检查点序号和产物摘要保持不变，从 `compose` 开始重建。

## 检查点、并发与停止恢复

检查点位于 `checkpoints/<stage>.json`，包含阶段名、输入指纹、配置版本、产物摘要、产物路径、组件摘要和全局单调序号。写入流程为：同目录临时文件、`fsync`、`os.replace`、目录 `fsync`，因此读者不会看到半写 JSON。

发布时使用 `fcntl.flock`：

- 落后或重复发布返回 `E_STALE_CHECKPOINT`；
- 跳过前置阶段发布返回 `E_INVALID_STAGE`；
- 正常发布在锁内分配下一个全局序号。

停止请求写入 `control/stop.json`。各阶段只在完整镜头/镜头块单元写完后检查停止标志；恢复时删除停止标志并复用已完成单元。测试覆盖了在 `compose` 和 `encode` 中途停止后恢复，最终输出目录与从零运行逐字节一致。

## CLI

```bash
storypipe generate --out artifacts/samples
storypipe import --sample artifacts/samples/small --workspace artifacts/runs/small
storypipe run --workspace artifacts/runs/small [--fresh] [--stop-after-units N]
storypipe status --workspace artifacts/runs/small
storypipe fingerprints --workspace artifacts/runs/small
storypipe stop --workspace artifacts/runs/small [--reason TEXT]
storypipe resume --workspace artifacts/runs/small
storypipe export-manifest --workspace artifacts/runs/small
storypipe export --workspace artifacts/runs/small --out /tmp/manifest.json
storypipe verify --workspace artifacts/runs/small
storypipe compare --small artifacts/runs/small --large artifacts/runs/large
storypipe serve --host 127.0.0.1 --port 8765
```

业务错误统一输出：

```json
{"ok":false,"error":{"code":"E_TRANSITION_OVERFLOW","message":"...","stage":"transition","shot_id":"shot-000","details":{}}}
```

## 本地 HTTP 接口

启动：

```bash
python3 -m storypipe serve --host 127.0.0.1 --port 8765
```

接口：

- `GET /health`
- `GET /v1/status?workspace=...`
- `GET /v1/fingerprints?workspace=...`
- `GET /v1/cache-plan?workspace=...`
- `GET /v1/checkpoints?workspace=...`
- `GET /v1/export/manifest?workspace=...`
- `GET /v1/verify?workspace=...`
- `POST /v1/import`：`{"sample_dir":"...","workspace":"..."}`
- `POST /v1/run`：`{"workspace":"...","fresh":false}`
- `POST /v1/stop`：`{"workspace":"...","reason":"..."}`
- `POST /v1/resume`：`{"workspace":"..."}`
- `POST /v1/compare`：`{"small_workspace":"...","large_workspace":"..."}`
- `POST /v1/checkpoints/publish`：检查点发布与并发错误验证入口

## 业务错误码

| 错误码 | 场景 |
| --- | --- |
| `E_INVALID_TIMELINE` | 零帧、负帧率/零帧率、非法时间轴字段 |
| `E_INVALID_SHOT` | 非法运镜类型、非法转场类型、重复/缺失镜头字段 |
| `E_MISSING_ASSET` | 图像或必要输入文件缺失、无法解码 |
| `E_TRANSITION_OVERFLOW` | 交叉淡化窗口超过相邻镜头帧数 |
| `E_INVALID_CONFIG` | 配置类型、预算或窗口参数非法 |
| `E_INVALID_ASSET` | 分镜图像尺寸不一致 |
| `E_STALE_CHECKPOINT` | 落后、重复或并发落败的检查点发布 |
| `E_INVALID_STAGE` | 阶段顺序被违反 |
| `E_ARTIFACT_CORRUPT` | 阶段引用产物摘要或尺寸不一致 |
| `E_PIPELINE_BUSY` | 同一工作区已有运行进程 |

## 目录结构

```text
storypipe/
  schedule.py        精确帧时间轴
  motion.py          运镜矩阵与双线性采样
  transition.py      转场计划与权重
  cache_policy.py    三策略评分选择
  frame_provider.py  受预算约束的帧缓存
  checkpoints.py     原子检查点、文件锁、序号校验
  fingerprints.py    分阶段内容指纹
  pipeline.py        四阶段执行、事务发布、停止恢复
  verify.py          时间轴/像素/规模对比验证
  cli.py             命令行
  http_api.py        本地 HTTP API
tests/               35 项自动化测试
artifacts/samples/   两档固定种子样本
artifacts/runs/      本次实际运行产物
artifacts/reports/   运行、比较与测试报告
```

单次工作区布局：

```text
<workspace>/
  input/{config.json,shots.jsonl,images/,sample_manifest.json}
  algorithm_plan.json
  checkpoints/{schedule,compose,transition,encode}.json
  stages/01_schedule/schedule.json
  stages/02_compose/{compose.json,units/}
  stages/03_transition/{transition.json,frames/,units/}
  stages/04_encode/output/{frames/,frame_summaries.jsonl,manifest.json}
  state/execution/*.json
  control/stop.json
```

## 实际验证结果

样本集摘要：

```text
sample_set_digest = sha256:f15f8b540750bf9bb4dc328fb2f09ee6e256c1d1b22b1ae2199e60e534bafd3a
small sample_digest = sha256:f97e67384ff316aef8472e65f27b657163a6b32e2103c8d4a390c8ff866dcd06
large sample_digest = sha256:f6bb6fbfcb80e73f1885cefd2c4b390fa0fc8179011efc387d24b88a8aaa62e0
```

小档：

```text
shots = 5, frames = 67
cache_strategy = per_frame_recompute
timeline_digest = sha256:607bb5ad6608bde9e844c3d18168973dc267dbef3f1aa8c4513d190c7943b35e
deterministic_summary = sha256:83e2974608443cd31e4a3752c5b698cbfa657d4ac8c97cc9462f27a8ab1f7f57
output_tree_digest = sha256:b342d04f573f3f151c72d0d3dd19e27cde5e5827a81a707850a3bdee38a152a4
```

大档：

```text
shots = 36, frames = 504
cache_strategy = segment_cache
timeline_digest = sha256:14d5ea49acc42894c575596c72540a0178c41e89f7fcae4daf36036ba1334ff6
deterministic_summary = sha256:a192c818304aa793bfb018dc8b50743243d893078ca78e91fe39d7227ff8c12d
output_tree_digest = sha256:384ba694e0b5be66620221799242c912eaa512c2e27c50eef8e6be61eacda280
```

大档帧分配：

```text
static = 180, pan = 156, zoom = 168
crossfade boundaries = 21, hard-cut boundaries = 14
crossfade frames = 84, hard-cut marker frames = 14 (no blending)
```

`artifacts/reports/scale_verification.json` 保存了完整对比；`artifacts/reports/run_small.json` 与 `artifacts/reports/run_large.json` 保存两档阶段状态。验证同时重新读取每个 PNG，核对像素摘要、PNG 摘要和输出树摘要。

测试结果：

```text
35 passed
```

覆盖项包括：样本双次重建一致性、混合帧率毫秒边界、双线性裁剪、运镜确定性、三种缓存策略、两档缓存预算、提示词/图像/帧率/时长/运镜/转场/预算增量失效、非法运镜与缺失图像、转场溢出、检查点重复/落后/顺序/并发、HTTP 导入/运行/状态/停止/恢复/导出，以及 `compose`/`encode` 中断恢复逐字节一致。
