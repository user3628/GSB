# 本地确定性视频预测与运动时序状态演进引擎

这是一个不依赖 GPU、容器、真实视频生成模型或外部服务的本地应用。它从固定随机种子生成的灰度帧序列和运动 JSON 清单出发，执行多步滚动状态推演、确定性双线性变形渲染、逐步真值误差计算、原子检查点恢复、优雅停止和按版本指纹的增量失效重建。

核心保证：

- 同一输入、配置和本地运行时下，状态轨迹、预测帧摘要、误差曲线和领域报告可逐字节复现。
- 每个滚动步完成后才发布检查点；停止请求只在步骤边界生效。
- 停止后恢复与一次从零完整推演得到相同的报告字节，检查点序号连续且无重复。
- 帧内容或清单阶段摘要变化时，从最早受影响的滚动步开始失效，只清理该序列的下游链路。
- 同一个工作区的并发布发布由进程级文件锁保护，落后、重复和跨序列错位发布返回 `E_STALE_CHECKPOINT`。

## 环境与快速开始

要求 Python 3.10+ 和 NumPy。当前实际验证环境为 Python 3.14.4、NumPy 2.5.5。

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
make demo
```

也可以逐步运行：

```bash
python3 scripts/generate_samples.py --out data
python3 -m motion_engine import --workspace runtime --manifest data/manifest.json
python3 -m motion_engine run --workspace runtime --series all
python3 -m motion_engine status --workspace runtime
python3 -m motion_engine serve --workspace runtime --port 18765
```

`make demo` 会依次生成样本、运行 17 项测试并执行停止/恢复验收演示。也可以只运行：

```bash
make test       # python3 -m unittest discover -s tests -v
make verify     # python3 scripts/verify_demo.py
```

## 演示样本与内容指纹

固定种子为 `20261121`。生成器位于 `motion_engine/sample_gen.py`，命令行包装为 `scripts/generate_samples.py`。样本保存在 `data/`，`data/fingerprints.json` 记录清单文件与每段 `.npy` 的内容摘要。

| 序列 | 帧数组形状 | 预测步数 | 时间步长 | 方向突变步 | 加速度衰减 |
| --- | --- | ---: | ---: | ---: | ---: |
| `alpha_translate` | `[19, 48, 64]` | 16 | 1 帧 | 7 | 1.0（匀速） |
| `beta_deform` | `[31, 40, 56]` | 14 | 2 帧 | 9 | 0.982 |
| `gamma_turn` | `[15, 36, 48]` | 12 | 1 帧 | 6 | 0.994 |

三组样本都包含平移方块、缓慢变形的亮色椭圆色块和一次速度方向突变。真值帧使用同一确定性渲染器生成，并叠加固定噪声种子产生的 `[-1, 1]` 观测噪声，因此误差非零但可重复。

已记录的 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| `data/manifest.json` | `8fc2d9018b15af354f9726365f4c501d680ac1a6878bb0a3e88e39a244498882` |
| `data/alpha_translate.npy` | `74acf96a65a82e16aeb14db19bd7b28e3970f3b938b0cf028a77543da060c6ac` |
| `data/beta_deform.npy` | `7abba3afeed6502ddb85b2e6b43cd8ad2f5ad3023cb024c7d3cccd1910d88fc6` |
| `data/gamma_turn.npy` | `f39ea61fddbb68cdc18fe717e505ed859b90c56912477335129e087b25d72831` |

重新运行 `scripts/generate_samples.py` 会得到完全相同的清单、帧文件和指纹文件；测试 `test_sample_generator_is_reproducible` 会逐文件比较两次生成结果。

## 命令行工作流

### 导入与推演

```bash
# 导入清单及其同目录帧文件；会先完整校验形状、像素范围、帧数和时间轴
python3 -m motion_engine import --workspace runtime --manifest data/manifest.json

# 继续推演所有序列（已完成的步骤不会重复）
python3 -m motion_engine run --workspace runtime --series all

# 每次只推演一个不可拆分步骤
python3 -m motion_engine step --workspace runtime --series alpha_translate

# 清空一个序列的检查点后从零推演
python3 -m motion_engine run --workspace runtime --series alpha_translate --fresh

# 在第 5 个新完成步骤后发出协作式停止请求并退出
python3 -m motion_engine run --workspace runtime --series beta_deform --stop-after 5

# 从最近连续一致检查点恢复
python3 -m motion_engine resume --workspace runtime --series beta_deform

# 要求恢复点必须正好是某个合法最新步骤，非法时返回 E_INVALID_STATE
python3 -m motion_engine resume --workspace runtime --series beta_deform --from-step 5
```

`SIGINT` 和 `SIGTERM` 只会设置停止标志，不会打断正在执行的步骤。当前步骤完成并原子写入检查点后，进程在下一步开始前退出。

### 查询与报告

```bash
python3 -m motion_engine status --workspace runtime
python3 -m motion_engine status --workspace runtime --series alpha_translate
python3 -m motion_engine report --workspace runtime --series alpha_translate --output alpha-report.json
```

`status` 输出当前版本阶段摘要、检查点连续前缀、下一序号、损坏/重复文件、实时失效判定、重建起始步和最近一次持久化失效原因。`report` 只输出领域报告，不包含 `performed_steps`、停止标志等运行封装字段，因此完整运行与停止/恢复运行可直接按字节比较。

## 本地 HTTP 工作台

默认仅绑定 `127.0.0.1`：

```bash
python3 -m motion_engine serve --workspace runtime --host 127.0.0.1 --port 18765
```

接口：

| 方法 | 路径 | 功能 |
| --- | --- | --- |
| `GET` | `/api/health` | 健康状态与活动推演 |
| `POST` | `/api/import` | 导入本地清单路径，请求体 `{"manifest_path": "...", "reset": false}` |
| `GET` | `/api/series` | 所有序列的检查点、版本指纹和失效状态 |
| `GET` | `/api/series/{id}` | 单序列状态 |
| `GET` | `/api/series/{id}/steps` | 同为单序列状态，含逐步报告摘要 |
| `GET` | `/api/series/{id}/report` | 确定性领域报告 |
| `POST` | `/api/series/{id}/run` | 从最近有效步骤继续，可选 `max_steps`、`step_delay` |
| `POST` | `/api/series/{id}/step` | 仅执行一个滚动步 |
| `POST` | `/api/series/{id}/resume` | 恢复，可选 `from_step` 校验 |
| `POST` | `/api/series/{id}/stop` | 请求当前活动推演在步骤边界停止 |

示例：

```bash
curl -s -X POST http://127.0.0.1:18765/api/import \
  -H 'Content-Type: application/json' \
  -d '{"manifest_path":"/absolute/path/to/data/manifest.json"}'

curl -s -X POST http://127.0.0.1:18765/api/series/alpha_translate/step \
  -H 'Content-Type: application/json' -d '{}'

# step_delay 便于演示在远端请求 /stop 时当前步完成后再退出
curl -s -X POST http://127.0.0.1:18765/api/series/alpha_translate/run \
  -H 'Content-Type: application/json' -d '{"step_delay":0.05}'

curl -s -X POST http://127.0.0.1:18765/api/series/alpha_translate/stop \
  -H 'Content-Type: application/json' -d '{}'

curl -s -X POST http://127.0.0.1:18765/api/series/alpha_translate/resume \
  -H 'Content-Type: application/json' -d '{}'
```

错误响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_INVALID_STATE",
    "message": "unknown series_id",
    "series_id": "missing",
    "position": "series_id"
  }
}
```

## 运动状态模型

状态分量固定为 `x, y, vx, vy, deformation`。清单初始状态对应第 0 步；每个滚动步 `step` 使用以下确定性公式：

```text
if step == direction_event.step:
    vx = vx * vx_factor
    vy = vy * vy_factor

x  = x + vx
y  = y + vy
vx = vx * acceleration_decay
vy = vy * acceleration_decay

deformation = initial.deformation
            + amplitude * sin(frequency * step + phase)
```

随后每个分量按该步 `expected_state_ranges[step - 1]` 钳制。越界会产生记录：

```json
{
  "step": 3,
  "component": "x",
  "requested": 71.25,
  "clamped": 63.0,
  "lower": -16.0,
  "upper": 63.0
}
```

钳制后的状态是下一步的输入。每次运行都按固定的 `x, y, vx, vy, deformation` 顺序检查，避免组件顺序影响轨迹或报告。

## 确定性预测帧渲染

渲染以第 0 帧为局部规范画布，使用逆向仿射映射：

```text
dx = (state.x - initial.x) * translation_gain
dy = (state.y - initial.y) * translation_gain
d  = (state.deformation - initial.deformation) * deformation_scale

scale_x = max(0.25, 1 + d)
scale_y = max(0.25, 1 - 0.5*d)

src_x = (dst_x - center_x - dx) / scale_x + center_x
src_y = (dst_y - center_y - dy) / scale_y + center_y
```

每个目标像素对四个邻域像素执行双线性采样；边界坐标为边缘扩展（`edge_mode: clamp`）；浮点结果先裁剪到 `[0, 255]`，再用 `np.rint` 量化并转换为 `uint8`。渲染只使用本地 NumPy，不访问外部服务。

每步帧摘要包含：

- `mean`：`float64` 全帧均值。
- `variance`：总体方差。
- `fingerprint`：绑定 dtype、shape 与 C 顺序像素字节的 SHA-256。

逐步误差为预测帧与时间轴对应真值帧的逐像素平均绝对误差。领域报告按步骤顺序输出 `error_curve`，`aggregate.sum_mae` 由同一列表使用有序 `sum` 累加，消费者可直接复核。

时间轴使用整数帧：

```text
truth_frame_index(step) = origin_frame + step * stride_frames
```

如果 `truth_frame_index(prediction_steps)` 超出 `frame_count - 1`，返回 `E_TIMELINE_OVERFLOW`，并包含序列 ID、超出帧数、最后所需帧索引和实际帧数。声明帧数与实际 `.npy` 帧数不符时返回 `E_INVALID_STATE`。

## 检查点、恢复与并发发布

检查点位于：

```text
<workspace>/checkpoints/<series-id>/step-00000001.json
<workspace>/checkpoints/<series-id>/step-00000002.json
...
```

每条记录包含序列 ID、步索引/序号、清单配置指纹、输入帧内容指纹、所选真值帧指纹、逐步输入指纹、完整状态与状态摘要、预测帧摘要、MAE、钳制事件、版本指纹和记录自校验和。

原子写入过程：

1. 在目标目录创建唯一临时文件。
2. 写入完整 JSON，调用 `flush` 和 `fsync`。
3. 使用同目录 `os.replace` 原子发布为最终文件名。
4. 对目录执行 `fsync`。

因此写入任意位置中断最多留下 `.tmp-*` 文件，不会被识别为记录。恢复时按文件名读取 JSON 后还会验证记录结构、序列一致性、记录校验和、状态摘要及序号；损坏、半写、重复序号和不连续后缀都会被忽略。

每个序列使用独立的 `fcntl.flock` 文件锁。发布在锁内重新扫描当前连续前缀，只接受 `step_index == last_valid_step + 1` 的记录。重复/落后、错误期望步或跨序列错位发布均返回 `E_STALE_CHECKPOINT`。

恢复只从连续一致前缀的末尾继续。已经发布的步骤不再推演；状态直接从最后一条有效记录恢复。停止请求和 `max_steps` 都只在步骤循环顶部检查，因此当前步骤渲染、比对、落盘完成后才会退出。

## 版本指纹与增量失效

每个序列当前版本由以下阶段摘要共同构成：

- 模型版本。
- 帧序列内容（形状与逐帧指纹）。
- 初始状态。
- 速度衰减与方向突变配置。
- 形变参数。
- 时间轴。
- 渲染参数。
- 每步期望状态范围。
- 预测步数。
- 完整清单配置摘要。

每个滚动步另有输入指纹，包含规范基帧、该步真值帧、真值帧索引、初始状态、动力学、形变、时间轴、渲染参数和该步范围。恢复时逐项比较已发布记录与当前逐步指纹，第一处差异即 `rebuild_from_step`。

典型失效位置：

| 变化 | 最早受影响步 |
| --- | --- |
| 基帧或渲染参数或初始状态或衰减/方向配置 | 1 |
| 真值帧 `k` 所在的对齐步 | 对应该步 |
| 某一步的期望范围 | 该步 |
| 时间轴 origin/stride | 1 |
| 预测步数增加 | 保留已有前缀，从旧末尾继续 |
| 预测步数减少 | 保留新步数内前缀，删除多余尾部 |

失效发生时，该步及全部下游检查点被删除并重建，上游记录保持原字节不变。未变化序列不共享检查点目录，因此不会被触碰。`status`/HTTP 查询会展示当前与检查点版本指纹、原因阶段、前后阶段摘要、重建起始步以及持久化在 `<workspace>/invalidations/` 的最近失效记录。

## 错误码

| 错误码 | HTTP | 含义 |
| --- | ---: | --- |
| `E_INVALID_STATE` | 422 | 非法恢复步、缺失前置状态、未知序列、形状/像素/帧数/清单不合法 |
| `E_TIMELINE_OVERFLOW` | 422 | 预测时间轴超出可用真值帧，带 `series_id` 和 `overflow_frames` |
| `E_STALE_CHECKPOINT` | 409 | 落后、重复、非连续或跨序列检查点发布 |
| `E_INTERNAL` | 500 | 未预期的本地内部错误 |

`E_INVALID_STATE` 始终尽量附带 `series_id` 与 `position`；帧数不符时 `position` 为 `frame_count`。

## 实际验证结果

测试命令：

```bash
python3 -m unittest discover -s tests -v
```

结果：`Ran 17 tests ... OK`。覆盖样本可复现、固定报告摘要、渲染确定性、钳制、停止/恢复逐字节一致、损坏/半写/重复记录、帧与各配置阶段增量失效、未变化序列复用、时间轴溢出、非法恢复、跨序列/重复发布、双进程文件锁竞争、HTTP 分步/停止/恢复及 CLI 冒烟。

完整推演固定报告指纹：

| 序列 | `report_fingerprint` |
| --- | --- |
| `alpha_translate` | `e3403c6b0d4b342290b8f55cf57479f7b341aec35d997012be0b2dbf8bc2f9d0` |
| `beta_deform` | `0d3baa53260f13480fb417dd00139f51199fe54a396505bbaee09a883196bf07` |
| `gamma_turn` | `865aebc7590de1bdbba9d8f56c5209ad70171b69c41ffc1fb7f1f903b4084c23` |

真实 CLI 停止/恢复验收（`beta_deform`，14 步）：

```text
停止点：第 5 步
恢复起点：第 6 步
恢复后新执行步数：9
最终完成：true
完整报告 SHA-256：   e83c4b7bbe531f3bb0735e93d53182270b05c302594d17770eebbd06582bbb91
恢复报告 SHA-256：   e83c4b7bbe531f3bb0735e93d53182270b05c302594d17770eebbd06582bbb91
报告字节一致：true
检查点文件数：14
检查点文件字节差异：0
```

真实增量失效验收：修改 `alpha_translate.npy` 第 5 帧的一个像素后，查询返回 `rebuild_from_step = 5` 和 `stage = truth_frame`，前 4 步检查点字节不变，第 5 步及下游重建；重建后的领域报告与同输入从零完整推演逐字节一致。

本地 HTTP 测试在随机空闲端口成功绑定并在独立线程中完成导入、分步、停止、恢复、报告和错误码请求；另用真实 `python -m motion_engine serve` 子进程在 `127.0.0.1:18765` 绑定并校验健康响应中的工作区路径与本次进程一致。当前环境已有服务占用 `8765`，因此默认端口设为 `18765`；仍可用 `--port` 或 `--port 0` 自由选择。CLI 测试通过真实子进程执行生成、导入、单步、状态和报告命令。

## 目录结构

```text
motion_engine/
  manifest.py      清单校验、阶段指纹
  inputs.py        帧加载、时间轴与逐步输入指纹
  model.py         状态更新与钳制
  render.py        双线性变形渲染、帧摘要、MAE
  checkpoints.py   原子检查点、扫描、恢复前缀、发布锁
  runner.py        滚动执行、停止、失效判断、领域报告
  workspace.py     导入、查询与工作区边界
  cli.py           命令行应用
  server.py        本地 JSON HTTP 应用
  sample_gen.py    固定种子演示样本
scripts/
  generate_samples.py
  verify_demo.py
tests/
  test_engine.py
  test_http_cli.py
data/              已生成样本与指纹
```

## 边界说明

- 这是本地文件系统应用；HTTP 导入接收本机清单路径，不包含上传鉴权或公网部署能力，默认只绑定回环地址。
- 文件锁使用 POSIX `fcntl.flock`，目标运行环境为本地 Linux/macOS 类系统。
- 逐字节确定性以相同代码、Python/NumPy 版本、CPU 浮点语义和本地文件系统为前提；报告和摘要中不写入墙钟时间。
- 强制 `SIGKILL` 不承诺生成退出报告；依靠原子检查点，重启后从最后完整步骤继续。
- 输入是二维灰度帧数组 `[frames, height, width]`，不包含视频编解码依赖。
