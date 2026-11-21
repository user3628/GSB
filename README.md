# KDE Drift Lab

一个从零构建、仅依赖 Python 标准库与 NumPy 的本地核密度漂移检测与分布比对工作台。应用提供确定性 NPZ 数据生成器、逐列 KDE、分块归约、MMD/KL 指标、固定种子显著性裁决、原子阶段检查点、增量失效、优雅停止/恢复、CLI、本地 HTTP API 与纯原生前端诊断面板。

## 快速开始

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

# 生成 small/large 两档可重复数据
.venv/bin/python -m kde_drift generate --out data

# 运行完整可重复演示（两种规模、分批一致性、增量重算、中断恢复、错误码）
.venv/bin/python -m kde_drift demo --out runs/demo

# 启动本地工作台；port 0 会自动选择可用端口
.venv/bin/python -m kde_drift serve --data-dir data --workdir-base runs/http --port 0
```

服务只绑定 `127.0.0.1`。终端会输出实际地址，例如 `http://127.0.0.1:41237`。

```bash
# 自动化测试
.venv/bin/python -m unittest discover -s tests -t . -v
```

## 数据与口径

`data/` 中的文件由固定种子 `20261121` 生成，NPZ 采用固定 ZIP 时间戳与排序条目，因此重新生成可逐字节重建。

| 档位 | 行数/侧 | 列数 | 缺失列 | 说明 |
| --- | ---: | ---: | --- | --- |
| small | 1500 | 8 | `[2, 6]` | 连续、重尾、偏态列 |
| large | 15000 | 24 | `[2, 6, 11, 18]` | 更宽特征矩阵与更多重尾列 |

每档包含：

- `reference.npz`: `features` (`float64`)、`source_labels` (`reference`)。
- `current.npz`: `features`、`source_labels` (`current`)。
- `declaration.json`: 行数、列数、列名、缺失列下标、生成种子与期望统计口径。
- `data/index.json`: 两档数据注册表。

声明文件明确记录：密度为逐列公共网格上的单位面积曲线；MMD 为无偏一维核 MMD²；KL 为 `reference_to_current` 方向、`epsilon=1e-12` 的梯形积分近似；漂移规则为 `score > threshold AND p_value <= alpha`。

## 算法实现

### 带宽与逐列 KDE

缺失值先按 `mean`、`median` 或 `zero` 填充，并在结果中记录缺失数量、比例、填充方法与填充值。填充后执行列规范化排序，再估计样本标准差 `s`（`ddof=1`，要求至少 2 个观测）：

```text
Silverman: h = 1.06 * s * n^(-1/5)
Scott:     h = 1.0592 * s * n^(-1/5)
Fixed:     h = configured value
```

每列在参考与待检样本的并集范围上建立公共 `linspace` 网格。支持高斯核与 Epanechnikov 核。核贡献按 `chunk_size` 分块，块内 `np.sum`、块间 `kahan` 或 `pairwise` 归约；最终对面积做梯形归一化。

数值诊断同时输出：

- `theoretical_upper_bound`: 基于机器 epsilon、块深度和归一化绝对项和的误差传播上界。
- `observed_high_precision_error`: 与本地 `longdouble` 高精度参考曲线的最大绝对差；平台不支持更高精度时明确标记为 `float64`。
- `verified_upper_bound`: `max(theoretical, observed)`，这是本地验证使用的保守包络。
- 块数量、块大小、归约方法、归一化前后积分。

带宽为零/负、无限、NaN、样本不足、全缺失列、非有限网格或核贡献都会抛出带 `stage`、`column`、`location` 的 `E_NUMERIC`，不会返回空曲线。

### MMD 与 KL

MMD 使用与密度阶段带宽关联的共享核带宽，按行块构造一维核矩阵并做补偿累加：

```text
MMD_u² = (ΣKxx - n_x)/(n_x(n_x-1))
       + (ΣKyy - n_y)/(n_y(n_y-1))
       - 2ΣKxy/(n_x n_y)
```

结果记录完整样本量、实际评估样本量、评估索引范围、核类型、共享带宽以及三个核矩阵块的累加次数。为保持大规模档位可交互，评估样本上限为 2048；超过上限时使用确定性的等距索引，并在结果中明确标记 `selection_policy`，不静默丢弃样本口径。

KL 近似使用同一密度网格：

```text
KL(ref || cur) ≈ Σ p(x) log((p(x)+ε)/(q(x)+ε)) Δx
```

列数或列名不一致时抛出 `E_COLUMN_MISMATCH`，`details.differences` 列出列数、名称数量及每个名称差异。

### 漂移裁决

对每列使用由 `SeedSequence([seed, column_index, n_ref, n_cur, bootstrap_size]) → PCG64` 派生的固定随机源。在合并样本上做有放回池化 bootstrap，构建零分布 MMD²，计算：

```text
p = (1 + #{null_score >= observed_score}) / (B + 1)
drift = (score > threshold) AND (p <= alpha)
```

输出保存得分、阈值、`alpha`、显著依据、重采样次数、零分布摘要、参考/当前/池化索引范围、抽样总数与索引摘要、MMD 所依赖的样本量与核参数，以及当前全配置版本指纹。数据至少需要两列；所有列都参与显著性重采样。

## 分批与确定性

`BatchAccumulator` 在写入接受状态前完整校验批；空批、维度不一致、无限值或标签/索引长度错误会返回 `E_INVALID_BATCH`，包含批号与位置，已接受批不受污染。

最终归约不依赖输入批大小或批顺序：

1. 所有批在显式累加器中合并。
2. 每列独立排序为规范化列序；缺失值填充均值也基于排序后的观测集合计算。
3. 密度、MMD 与 bootstrap 都从该规范序出发，并使用配置固定的块大小重新分块。
4. 批大小与批顺序不进入阶段输入指纹，也不进入结果摘要。

因此相同样本集合、相同配置下，不同批大小/顺序的密度曲线、MMD、KL、漂移裁决和最终摘要逐字节一致。

## 检查点、失效与发布保护

工作目录布局：

```text
workdir/
  checkpoints/{bandwidth,density,metrics,drift}.json
  artifacts/{stage}-rNNNNNN-sNNNNNN.json
  .publish.lock
  STOP
```

阶段依赖与失效边界：

| 阶段 | 输入摘要包含 | 最早变化来源 |
| --- | --- | --- |
| bandwidth | 数据内容、kernel、bandwidth、imputation | 参考/待检样本、核、带宽规则、填充 |
| density | bandwidth 结果、grid、chunk_size、summation | 网格、分块、求和、上游 |
| metrics | density 结果、mmd、kl、imputation | MMD/KL 配置、上游 |
| drift | metrics 结果、threshold、alpha、resamples、seed | 阈值、显著性、重采样、上游 |

检查点记录包含阶段名、全局单调 `sequence`、阶段内 `stage_revision`、输入内容指纹、全配置版本、上游结果指纹、累加器摘要、结果指纹和产物摘要。产物和记录分别在目标目录内通过临时文件 + `fsync` + `os.replace` 原子发布；中断最多留下未引用的临时文件或产物，不会留下半写 JSON 记录。

发布时使用 `flock` 排他锁；调用方提交期望的前一阶段版本，只有 `expected_previous_revision == current_revision` 才允许发布，阶段序号和全局序号均只向前推进。重复/落后发布返回 `E_STALE_CHECKPOINT`；缺失或指纹不一致的前置阶段返回 `E_INVALID_STAGE`。查询接口持续返回受影响链、可复用上游与具体失效原因。

停止请求在每个不可拆分单元完成后生效：带宽按列、密度按分块、指标按列、bootstrap 按重采样。未完成阶段不发布检查点；恢复后从最后一个有效阶段继续，结果与一次完整运行逐字节一致。

## 错误码

| 错误码 | 含义 |
| --- | --- |
| `E_NUMERIC` | 零/非有限带宽、样本量不足、NaN/Inf 核参数、无效密度/指标数值；含阶段、列名、位置 |
| `E_COLUMN_MISMATCH` | 参考/待检列数或列名不一致；附差异清单 |
| `E_INVALID_BATCH` | 空批、维度错误、批元数据错误；附批号与位置，不污染已归约状态 |
| `E_STALE_CHECKPOINT` | 并发、落后或重复的阶段发布被版本围栏拒绝 |
| `E_INVALID_STAGE` | 前置检查点缺失、产物缺失/被篡改或指纹不一致 |

一条命令即可演示全部错误码：

```bash
.venv/bin/python -m kde_drift errors --data-dir data
```

## CLI

```bash
# 校验两档数据
.venv/bin/python -m kde_drift validate --data-dir data

# 运行/增量恢复 small
.venv/bin/python -m kde_drift run \
  --dataset small --config configs/default.json \
  --data-dir data --workdir runs/small

# 分批送入；结果应与整批逐字节一致
.venv/bin/python -m kde_drift run \
  --dataset small --data-dir data --workdir runs/small-reverse \
  --batch-size 173 --batch-order reverse

# 查询阶段状态/版本指纹/失效原因
.venv/bin/python -m kde_drift status \
  --dataset small --data-dir data --workdir runs/small

# 配置变化后可查询最早失效阶段，再运行增量重建
.venv/bin/python -m kde_drift run \
  --dataset small --config configs/strict-alpha.json \
  --data-dir data --workdir runs/small

# 导出
.venv/bin/python -m kde_drift export --kind density \
  --dataset small --data-dir data --workdir runs/small \
  --out exports/density.json

# 请求停止（独立进程写入 STOP 文件）
.venv/bin/python -m kde_drift stop --workdir runs/small
```

`--stop-after-units N` 是用于自动化验收的确定性停止钩子：完成第 N 个不可拆分单元后退出。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 服务信息与 NumPy 版本 |
| GET | `/api/datasets` | 两档样本声明摘要 |
| GET | `/api/config/default` | 默认配置 |
| GET/POST | `/api/session` | 查询/切换数据集与配置 |
| POST | `/api/run` | 后台评估；支持 `batch_size`、`batch_order` 或显式 `batches` |
| POST | `/api/stop` | 写入协作式停止请求 |
| POST | `/api/resume` | 从有效检查点恢复 |
| GET | `/api/status` | 状态、阶段序号/版本、有效性和进度 |
| GET | `/api/invalidation` | 最早失效阶段、复用链和原因 |
| GET | `/api/results?stage=all` | 逐列密度、误差包络、指标、裁决及依据 |
| GET | `/api/export/density` | 下载确定性密度曲线 JSON |
| GET | `/api/export/report` | 下载判定报告 JSON |
| GET | `/api/error-codes` | 错误码目录 |

```bash
curl -X POST http://127.0.0.1:8765/api/run \
  -H 'Content-Type: application/json' \
  -d '{"dataset":"small","batch_size":173,"batch_order":"reverse"}'

curl -X POST http://127.0.0.1:8765/api/stop -d '{}' \
  -H 'Content-Type: application/json'

curl http://127.0.0.1:8765/api/status
```

## 前端工作台

访问服务根路径即可使用。原生 Canvas/CSS/JS，无 CDN、无外部服务。页面包含：

- 数据集、核、带宽、填充、网格、分块、求和、阈值、`alpha`、重采样参数与批设置。
- 四阶段检查点时间线，显示 `sequence`、`stage_revision`、输入指纹与失效状态。
- 参考/待检密度曲线叠加图与逐列选择。
- 理论上界、高精度观测误差、验证包络、带宽、缺失填充、MMD 与 p-value 诊断。
- 漂移裁决表与完整失效原因链。
- 运行、停止、恢复、强制重建、密度/报告导出。

## 实际验证结果

本环境：Python `3.11.18`、NumPy `1.26.4`。最近一次执行：

```bash
.venv/bin/python -m unittest discover -s tests -t . -v
# Ran 28 tests ... OK

.venv/bin/python -m kde_drift demo --data-dir data --out runs/demo
# all_checks_passed = true
```

固定演示摘要（默认配置）：

| 项目 | 结果 |
| --- | --- |
| small 漂移列 | `lognormal_02`, `exponential_06`（2 列） |
| small `output_digest` | `ee166f62622b2fd8f68adbc33227cec9a4e29a9dfe888a60557063ab2194abe9` |
| large 漂移列 | `lognormal_02`, `exponential_06`, `lognormal_14`, `exponential_18`（4 列） |
| large `output_digest` | `9e14bdb4e09e4b25b16d45d6da7f63f4582224340efe7d4ff7269bc18ddc8eff` |
| 演示总摘要 | `daa24d3f996842f7ad032710cb5d6cc73f5a1e6aac1c85cddcbb2c222a279f77` |

演示还验证了：

- 生成器重放到临时目录后所有文件 SHA-256 完全一致。
- `batch_size=173`、`batch_order=reverse` 的 density/metrics/drift 与整批运行逐字节一致。
- 将 `drift.alpha` 从 `0.05` 改为 `0.01` 后，最早失效阶段为 `drift`，上游 `bandwidth/density/metrics` 复用；全局序号从 `4` 推进到 `5`，漂移列由 2 列变为 0 列，配置版本改变。
- 在密度首个分块后停止；停止后仅 `bandwidth` 有效且序号为 1；恢复后序号连续为 `[1,2,3,4]`，density/metrics/drift 与完整运行逐字节一致。
- `E_NUMERIC`、`E_COLUMN_MISMATCH`、`E_INVALID_BATCH`、`E_STALE_CHECKPOINT`、`E_INVALID_STAGE` 全部实际触发。

详细机器可读结果见 `runs/demo/demo_results.json`，导出的固定产物为 `runs/demo/density_curves.json` 与 `runs/demo/drift_report.json`。

## 项目结构

```text
kde_drift/
  generator.py       固定种子、确定性 NPZ/声明生成
  data.py            声明加载、列 schema 校验、内容指纹
  batch.py           批校验与不污染累加器
  kde.py             带宽、填充、分块 KDE、高精度误差包络
  metrics.py         分块核矩阵 MMD 与 KL
  drift.py           固定种子 bootstrap 与阈值裁决
  checkpoints.py     原子检查点、flock、阶段版本围栏
  workbench.py       阶段编排、增量失效、恢复、导出
  service.py         后台任务服务状态机
  http_server.py     127.0.0.1 JSON API/静态文件
  static/            原生工作台 UI
  cli.py             命令入口
  demo.py            可重复验收演示
tests/               28 个自动化测试
configs/             默认与严格显著性配置
data/                已生成两档样本
runs/demo/           固定演示产物与结果
```

## 设计边界

- 每列独立分析；跨列行配对不用于密度/MMD/KL 计算，批不变性通过逐列规范序实现。
- 大规模 MMD 使用声明的确定性上限 2048；完整样本量和实际评估样本量都会输出，不伪装成全量精确计算。
- 高精度参考是本地 `longdouble` 对照；`verified_upper_bound` 是理论界与本地观测误差的保守包络，不宣称跨架构逐位相同。相同环境、相同输入与配置下结果逐字节一致。
- 不使用 Docker、外部数据库、外部随机源或网络服务。
