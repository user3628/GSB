# LocalTrack Workbench

本地、确定性、无外部服务依赖的视频目标跟踪可视化与诊断工作台。

系统只使用本地生成的低分辨率灰度帧、本地 NumPy/PyYAML 和 Python 标准库，实现：

- 固定随机种子的 3 类确定性运动序列：匀速平移、缓慢缩放、一次方向突变；
- 帧级外观建模、像素对齐网格候选采样、对数域响应打分；
- 轨迹状态演进、连续低置信度遮挡判定、基于中断前锚点的恢复；
- 每帧不可拆分单元的原子检查点、文件锁、链式摘要、优雅停止与逐字节一致恢复；
- 按内容摘要定位最早失效帧与阶段，隔离失效状态并复用未受影响前缀；
- 本地 HTTP 仪表盘与 CLI，支持导入、分阶段执行、诊断、停止、恢复与报告；
- 业务错误码和持久化错误诊断。

当前工作区已经生成并验证了完整演示数据与摘要。

## 快速开始

已验证环境：Python `3.14.4`（要求 3.11+）、NumPy `2.2.1`、PyYAML `6.0.3`。运行时不需要网络、GPU、Docker、预训练权重或外部跟踪服务。

```bash
# 本工作区已有 .venv，且通过系统本地离线包创建
.venv/bin/python -m pip install -e . --no-deps
.venv/bin/python -m tracklab --root . validate
.venv/bin/python -m tracklab --root . status
.venv/bin/python -m tracklab --root . serve
# 浏览器打开 http://127.0.0.1:8765/
```

如果从零创建环境，可使用本机已有包：

```bash
python3 -m venv --system-site-packages .venv
.venv/bin/python -m pip install -e . --no-deps
```

或在有本地 wheelhouse 的离线环境中：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --no-index --find-links /path/to/offline-wheels -r requirements.txt
.venv/bin/python -m pip install -e . --no-deps
```

Windows 使用 `.venv\Scripts\python`，所有生成的路径、序列 ID 和文件名均为 ASCII 且不含 Windows 保留字符。

## 一键确定性演示

```bash
.venv/bin/python -m tracklab --root . demo
```

演示依次执行：

1. 用固定种子重新生成 3 个序列、跟踪清单、参考摘要和遮挡序列；
2. 从零完整运行所有序列，保存固定摘要与检查点快照到 `runs/snapshots/full/`；
3. 清空运行状态，在 `seq_uniform=12`、`seq_scaling=9`、`seq_direction=20` 帧处写入停止请求并优雅退出；方向突变序列在遮挡期间停止；
4. 恢复每个序列并完成剩余帧，保存到 `runs/snapshots/resume/`；
5. 比较两次的 `run_bundle.json`、`summary.json` 和全部检查点文件。

默认 40 帧/序列的实际验证结果：

| 项目 | 结果 |
|---|---|
| 完整运行与停止恢复的 run bundle | 逐字节一致 |
| 报告 summary | 逐字节一致 |
| 检查点树 | 完全一致 |
| 检查点文件数 | 120 |
| 恢复时重放已发布帧 | 0 |
| 完整/恢复检查点树摘要 | `b03c165212662b6e7fd5d5579848b38f3b418ea4f86c5545c37996a53fddab36` |
| 固定运行摘要 | `180aa109787bd85d74f76590ce592a3f27a57bef97fb93d7bd68e97f23c95b7b` |
| 演示报告摘要 | `f4798346c52af8b571ad20156759ac84377bf4e1e6d8def022aa37601becece3` |

序列结果：

| 序列 | 平均 IoU | 最大中心误差 | 遮挡/恢复 | 模板更新事件 |
|---|---:|---:|---|---:|
| `seq_uniform` | 0.889919 | 0.047227 | 无 | 40 |
| `seq_scaling` | 0.926048 | 0.053990 | 无 | 39 |
| `seq_direction` | 0.912874 | 0.160147 | 18–23 帧遮挡，24 帧恢复 | 34 |

这些数值来自 `reports/summary.json`、`reports/run_bundle.json` 与 `reports/demo_summary.json`，均由实际运行生成。

## 数据与清单

生成器脚本：`tracklab/generator.py`，固定种子 `20261123`，也提供 `tools/generate_demo.py` 包装器。

```text
data/
  tracking_manifest.json
  reference_summary.json
  sequences/
    seq_uniform/frames/000000.npy
    seq_scaling/frames/000000.npy
    seq_direction/frames/000000.npy
```

每段序列在清单中声明：

- `sequence_id`、`image_shape`、`frame_rate`；
- `visible_frames` 与 `sample_count_upper_bound`；
- `initial_box`（归一化 `x,y,w,h`）；
- 每帧 `frames[].path`、`frames[].box_xywh_norm` 参考真值、帧文件 SHA-256；
- `search_window_expansion`、`appearance_update_threshold`、`trace_confidence_floor`；
- 声明的运动模型与预期遮挡区间。

校验会拒绝序列 ID/路径非法、帧数不一致、样本上界不足、形状或 dtype 错误、帧文件缺失等情况。

参考真值的使用边界：

- 外观阶段会按初始框和参考真值框分别生成模型记录、内容摘要和诊断信息；
- 跟踪模板只由初始框与帧像素初始化；
- 候选采样、外观相似度、中心先验、轨迹更新完全不读取参考真值；
- 参考真值只在轨迹状态固定后用于 IoU 和中心误差，是验证的唯一来源。

## 四阶段处理

### 1. 帧级外观建模

`tracklab/appearance.py`

- 对每帧裁剪初始锚点区域和参考真值区域；
- 灰度直方图：16 bins，L1 归一化；
- 梯度方向直方图：8 bins，Sobel 风格中心差分、梯度幅值加权、L1 归一化；
- 附加确定性 8×8 空间签名（去均值、L2 归一化），用于低分辨率下区分平移/尺度错位；
- 所有浮点数按 12 位稳定化，通过规范 JSON 计算 SHA-256；
- 空框、越界框、像素裁剪为空或候选区域全零会产生 `E_INVALID_REGION`；候选区域全无效会产生 `E_NUMERIC`；
- 输出 `runs/artifacts/<seq>/appearance.json`，逐帧包含序列 ID、帧号、两个框、像素区域、直方图、空间签名和摘要。

### 2. 候选采样与响应打分

`tracklab/scoring.py`

- 搜索窗口由序列的 `search_window_expansion` 控制；
- 候选位置使用固定整数像素步长网格，避免低分辨率下浮点框与像素边界错位；
- 宽度、高度分别使用 `[-1, 0, +1]` 像素步进组合，保证真实像素尺寸组合可被采样；
- 外观相似度：

```text
gray   = Bhattacharyya(gray_hist_template, gray_hist_candidate)
grad   = Bhattacharyya(grad_hist_template, grad_hist_candidate)
space  = cosine(spatial_signature_template, spatial_signature_candidate)
app    = 0.40 * gray + 0.25 * grad + 0.35 * space
prior  = exp(-0.5 * ((dx / half_w / sigma)^2 + (dy / half_h / sigma)^2))
log_s  = log(app + eps) + 0.30 * log(prior + eps)
score  = exp(log_s)
```

- 保留前 `top_k=6` 候选以及灰度、梯度、空间、中心先验、对数分等分量；
- 搜索窗口完全在画面外、候选数为零、任一分量或最终分为 NaN/Inf 时返回 `E_NUMERIC`，错误上下文包含序列、帧、预测框、搜索窗口、网格步长和候选统计；
- 错误不会静默复用上一帧结果；
- 输出 `runs/artifacts/<seq>/candidates.json`，记录阶段指纹、搜索配置、已发布帧的候选摘要，以及未完成轨迹的下一帧候选预览；
- 阶段 2 要求完成且当前的外观阶段，阶段 3 要求当前且自校验通过的候选阶段产物；缺失、损坏或指纹不一致返回 `E_INVALID_STAGE`；
- `all` 完成轨迹后会刷新候选产物为全帧审计快照，`preview_frame` 归零为 `null`。

### 3. 轨迹演进、遮挡与恢复

`tracklab/tracker.py`

- 状态包含当前框、速度、尺寸速度、模板、模板来源帧、低分连续计数、遮挡锚点、最近置信帧；
- 速度经平滑与比例上限约束，避免候选抖动导致预测过冲；
- 响应分低于 `trace_confidence_floor` 连续 `occlusion_patience=2` 帧时进入遮挡；
- 进入遮挡时冻结最近置信状态为锚点，记录锚点帧、帧区间和低分序列；
- 遮挡期间始终在锚点附近、与中断前相同扩展比例（乘恢复倍率）的窗口检索，不读取真值；
- 恢复阈值是 `confidence_floor + recovery_margin`，恢复记录显式包含锚点帧、历史帧列表、搜索窗口、候选数、响应分和恢复依据；
- 模板只在响应分达到序列的 `appearance_update_threshold` 时按 `alpha=0.15` 做确定性指数更新；
- 每帧有整数 `logical_time_ms = round(frame_index * 1000 / fps)`，用于确定性毫秒时间轴；
- 轨迹状态锁定后才计算真值 IoU/中心误差。

### 4. 可视化诊断汇总

`tracklab/workbench.py`、`tracklab/http_api.py`、`tracklab/static/`

- 生成逐帧目标框、真值框、IoU、中心误差、响应分、状态动作、模板事件、遮挡区间和恢复依据；
- HTTP 仪表盘显示灰度帧、真值轮廓（绿）、跟踪轮廓（青色）、误差曲线、遮挡区间、阶段指纹、失效事件和错误诊断；
- `reports/run_bundle.json` 和 `reports/summary.json` 使用规范 JSON，确定性可比较。

## 检查点与恢复一致性

每个序列的每帧轨迹演进完成后写一个检查点：

```text
runs/checkpoints/<seq>/checkpoint_00000001.json
runs/checkpoints/<seq>/checkpoint_00000002.json
...
```

每份记录包含：

- `sequence_id`、`frame_index`、单调整数 `ordinal`；
- 输入帧内容 SHA-256、配置指纹、序列版本指纹；
- 各阶段来源指纹与阶段摘要；
- 候选记录、轨迹记录、完整可恢复状态；
- `previous_state_digest`、`state_digest`、`checkpoint_digest`。

一致性机制：

- 写文件使用同目录临时文件 + `fsync` + `os.replace` + 目录 `fsync`；
- 真正跨进程排他锁：POSIX `fcntl.flock`，Windows `msvcrt.locking`；
- 发布时必须正好是当前连续前缀的下一帧序号，落后、重复、跨序列、链摘要不匹配返回 `E_STALE_CHECKPOINT`；
- 扫描只接受从序号 1 开始连续、摘要与链都有效的记录；
- 损坏、截断、重复序号或缺口之后的记录被忽略，并从最近一致前缀恢复；
- 失效记录被移动到 `runs/invalidated/<seq>/<event-token>/`，不留在活动链中；
- 恢复时读取已发布检查点状态继续，不重新计算已发布帧。

停止行为：

- `SIGINT`/`SIGTERM`、HTTP/CLI 停止请求或配置的帧预算都会在当前不可拆分逐帧单元完成后生效；
- 退出前只写已经完整发布的帧和运行状态，不承诺强制 `SIGKILL` 后生成退出报告；
- `resume` 会清除停止请求，从检查点连续前缀继续。

## 版本变化与增量失效

`tracklab/versions.py` 为每帧保存：

- 帧内容 SHA-256；
- 初始框、参考真值框；
- 外观配置指纹；
- 候选/搜索配置指纹；
- 轨迹/更新阈值/置信下限指纹；
- 外观、候选、轨迹阶段摘要与组合摘要。

任一输入变化时：

- 帧内容或参考真值在第 `k` 帧变化：失效阶段从 `appearance` 的第 `k` 帧开始；
- 初始框变化：从第 0 帧开始；
- 搜索窗口或候选参数变化：从第 0 帧开始，受影响阶段为 `candidates`；
- 更新阈值、置信下限、遮挡参数变化：从第 0 帧开始，受影响阶段为 `trajectory`；
- 第 `k` 帧之前的上游检查点保留并直接复用；
- 第 `k` 帧及之后的活动检查点被隔离，随后重建。

失效事件写入：

```text
runs/artifacts/invalidations/<seq>/<event-token>.json
```

事件包括 `earliest_affected_frame`、`affected_frame_interval`、`retained_through_frame`、阶段、原因、旧/新指纹和隔离文件列表。

测试已验证：修改 `seq_direction` 第 10 帧参考真值后，只保留 0–9 帧，事件为 `appearance/reference_truth_changed`，受影响区间 `[10, 39]`；继续修改搜索扩展比例后，最早失效帧回到 0，阶段为 `candidates`。

## 错误码

| 错误码 | 含义 | HTTP |
|---|---|---|
| `E_INVALID_REGION` | 空框、越界框、空像素区域、全零区域 | 400 |
| `E_NUMERIC` | 搜索窗口全在画面外、零候选、NaN/Inf | 422 |
| `E_STALE_CHECKPOINT` | 落后/重复/跨序列发布、链摘要或状态替换不匹配 | 409 |
| `E_INVALID_STAGE` | 非法阶段跳转、缺少前置阶段、跳帧诊断 | 400 |
| `E_MANIFEST` | 清单、路径、帧数或帧文件不合法 | 400 |
| `E_CONFIG` | 配置文件或参数不合法 | 400 |
| `E_NOT_FOUND` | 序列、帧或路由不存在 | 404 |
| `E_IO` | 本地 I/O、请求体或服务端异常 | 400/500 |

业务错误会写入 `runtime/diagnostics/errors/`，CLI 将错误 JSON 输出到 stderr 并使用稳定退出码。HTTP 返回 `{"ok":false,"error":{"code", "message", "context"}}`。

## CLI

```bash
.venv/bin/python -m tracklab --root . generate --force
.venv/bin/python -m tracklab --root . validate
.venv/bin/python -m tracklab --root . import --manifest /path/to/tracking_manifest.json --source-data-dir /path/to/data
.venv/bin/python -m tracklab --root . run --sequence seq_direction
.venv/bin/python -m tracklab --root . run --sequence seq_direction --stage appearance
.venv/bin/python -m tracklab --root . run --sequence seq_direction --stage candidates
.venv/bin/python -m tracklab --root . run --sequence seq_direction --stage trajectory
.venv/bin/python -m tracklab --root . run --sequence seq_direction --stage tracking
.venv/bin/python -m tracklab --root . run --sequence seq_direction --stop-after 20
.venv/bin/python -m tracklab --root . stop --sequence seq_direction
.venv/bin/python -m tracklab --root . resume --sequence seq_direction
.venv/bin/python -m tracklab --root . status --sequence seq_direction
.venv/bin/python -m tracklab --root . diagnose --sequence seq_direction --frame 20
.venv/bin/python -m tracklab --root . invalidations --sequence seq_direction
.venv/bin/python -m tracklab --root . report
.venv/bin/python -m tracklab --root . serve --host 127.0.0.1 --port 8765
.venv/bin/python -m tracklab --root . verify
.venv/bin/python -m tracklab --root . demo --json
```

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 本地仪表盘 |
| GET | `/api/health` | 健康检查 |
| GET | `/api/sequences` | 序列、进度、遮挡与阶段状态 |
| GET | `/api/sequences/<id>` | 单序列状态、版本指纹、遮挡与检查点链 |
| GET | `/api/sequences/<id>/frame/<n>` | 灰度像素与当前真值/跟踪框 |
| GET | `/api/sequences/<id>/diagnostics/<n>` | 逐帧外观、候选、轨迹、来源指纹 |
| GET | `/api/sequences/<id>/result` | 完整序列报告 bundle |
| POST | `/api/import` | 导入清单与帧文件 |
| POST | `/api/generate` | 重新生成固定种子演示数据 |
| POST | `/api/run` | 触发 `appearance`/`candidates`/`trajectory`/`tracking`/`all`/`report` |
| POST | `/api/stop` | 写入优雅停止请求 |
| POST | `/api/resume` | 清除停止请求并恢复 |
| GET/POST | `/api/report` | 构建总报告 |
| GET | `/api/invalidations` | 失效事件与持久化错误诊断 |

服务默认只绑定 `127.0.0.1:8765`，不访问外部网络。

## 测试

```bash
.venv/bin/python -m pytest -q
```

覆盖：

- 生成器重复执行逐字节一致、清单帧数与样本上界；
- 清单拒绝 Windows 设备名、尾随点/空格等不可移植帧路径；
- 外观记录逐字节一致、自然错误上下文；
- 搜索窗口越界、零候选、NaN 直方图的 `E_NUMERIC`；
- 检查点重复/跨序列/链摘要替换拒绝；
- 损坏检查点忽略、隔离和从最近前缀继续；
- 两个独立进程同时发布同一序号时的真实文件锁竞争；
- 真值变化和搜索比例变化的增量失效与上游复用；
- 非法阶段跳转、候选阶段前置门禁、候选产物篡改拒绝；
- 独立候选阶段重复执行逐字节一致，完整运行的候选审计覆盖全部检查点；
- `demo` 的短输出与 `--json` 路径均保持可解析；
- 3 个序列完整运行 vs 停止恢复的 bundle、summary、检查点树逐字节一致；
- HTTP 健康、序列、帧、诊断、报告和错误码。

实际测试结果记录在 [docs/VERIFICATION.md](docs/VERIFICATION.md)。

## 目录

```text
config/workbench.yaml       本地端口、目录、生成器与跟踪参数
tracklab/generator.py       固定种子数据生成
tracklab/appearance.py      阶段 1
tracklab/scoring.py         阶段 2
tracklab/tracker.py         阶段 3
tracklab/workbench.py       阶段编排、恢复、报告与诊断
tracklab/checkpoints.py     原子检查点与文件锁
tracklab/versions.py        版本指纹与增量失效
tracklab/http_api.py        HTTP API
tracklab/static/            无外部依赖仪表盘
tracklab/cli.py             CLI
tracklab/demo.py            完整/停止恢复等价性演示
tests/                      单元与集成测试
data/                       本地生成数据
runs/checkpoints/           活动检查点链
runs/artifacts/             外观、候选与失效诊断产物
runs/snapshots/             完整与恢复快照
reports/                    固定摘要与逐序列报告
runtime/                    锁、停止请求、错误诊断
```

## 限制与说明

- 这是本地确定性诊断工作台，不是通用真实世界跟踪模型；不使用预训练权重或真实视频。
- 单目标、轴对齐矩形框、灰度输入，运动模型由演示生成器声明。
- 当前外观模型是低分辨率直方图 + 梯度方向直方图 + 空间签名的经典组合；候选打分针对本工作区生成的亮色矩形目标。
- 文件锁一致性依赖本地文件系统对独占锁和原子替换的支持；共享网络文件系统不在验收范围。
- 强制终止进程不保证清理锁文件或生成退出报告；下一次运行会忽略不完整/损坏记录并继续。
