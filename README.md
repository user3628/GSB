# 本地图像分类 / 多标签预测校验工作台

一个完全本地、确定性、可复现的 CV 多标签验证工作台。输入是由固定随机种子生成的
RGB PNG 与 JSON 清单；打分表由确定性伪推理生成，不依赖真实模型、GPU、Docker 或
外部服务。工作台支持四阶段校验、异常注入、增量失效、原子检查点、协作停止/恢复、
并发发布保护、本地 HTTP 仪表盘与命令行操作。

## 1. 快速开始

环境：Python 3.10+、NumPy、Pillow、Linux 文件锁（标准库 `fcntl`）。本次实测环境为
Python 3.14.4、NumPy 2.5.5、Pillow 13.0.0。

```bash
python3 -m pip install -r requirements.txt

# 生成 8 个确定性样本与清单
python3 -m workbench generate --seed 20261121

# 完整运行四阶段
python3 -m workbench run

# 查看阶段状态/版本指纹/失效原因
python3 -m workbench status
python3 -m workbench stages
python3 -m workbench invalidations

# 启动本地 HTTP 仪表盘
python3 -m workbench serve --host 127.0.0.1 --port 8765
# 浏览器打开 http://127.0.0.1:8765

# 自动化验证
python3 -m unittest discover -s tests -v

# 一键演示：基线、停止恢复、异常注入、恢复后哈希一致性
python3 -m workbench demo
```

所有路径均在当前工作区内解析，配置文件拒绝越界路径。生成/运行后的主要产物：

- `samples/manifest.json`：真值、类别表、逐标签分数与伪推理来源说明。
- `configs/default.json`：阈值、互斥标签对、分块大小、归一化与指标规则。
- `configs/injection.json`：显式、可审计的异常注入操作。
- `checkpoints/`：逐样本逐阶段检查点、发布高水位、失效审计。
- `reports/report.json`：确定性校验报告。
- `reports/report.sha256`：报告字节级 SHA-256。
- `reports/failures.json`、`reports/conflicts.json`：失败与冲突定位。
- `reports/demo_summary.json`：停止恢复与注入演示的实际结果。

## 2. 工作区结构

```text
workbench/
  generator.py     固定种子 PNG/清单/伪推理生成器
  manifest.py      清单、真值、注入与来源模型
  stages.py        四阶段的纯函数式产物构建
  metrics.py       P/R、AP、宏/微平均、共现一致性
  store.py         原子检查点、序号、文件锁、快照保护
  pipeline.py      增量规划、失效传播、停止恢复、报告生成
  injection.py     五类异常注入参数入口
  importer.py      工作区内本地清单/图像导入
  api.py           本地 HTTP API 与仪表盘
  cli.py           CLI 入口
  static/index.html 交互式前端
samples/           本地 PNG、manifest 与说明
configs/           默认配置与注入配置
checkpoints/       逐样本逐阶段检查点与完整性水位
reports/           确定性报告、失败/冲突明细、演示摘要
tests/             20 项 unittest 验证
```

## 3. 确定性样本与清单

生成器使用固定种子 `20261121`。PNG 像素由标准库随机数生成，保存参数固定；伪推理分数为：

```text
score(label) = clamp(0.04 + 0.30 * uniform(sha256(seed, image_sha256, label)), 0.01, 0.99)
```

生成器再施加样本级确定性偏置，以构造命中/漏报/误报和业务异常。清单中的真值来源与打分表
均在 `samples/manifest.json` 内显式可查；报告和查询接口同时给出清单路径、图像路径、
图像 SHA-256、清单记录序号、配置版本和注入操作。

清单核心结构：

```json
{
  "seed": 20261121,
  "classes": ["cat", "dog", "vehicle", "indoor", "outdoor", "day", "night"],
  "samples": [
    {
      "image_id": "s01_dual",
      "path": "images/s01_dual.png",
      "width": 32,
      "height": 32,
      "truth": [
        {"label": "cat", "lower_bound": 0.71817},
        {"label": "indoor", "lower_bound": 0.681588}
      ],
      "scores": {"cat": 0.79817, "dog": 0.223085, "vehicle": 0.62285}
    }
  ]
}
```

`scores` 必须与类别表一一对应。非有限值在 JSON 中显式写为字符串 `"NaN"`、`"Inf"` 或
`"-Inf"`，不会使用非标准 JSON 扩展。

演示数据覆盖：

| image_id | 场景 | 预期结果 |
|---|---|---|
| `s01_dual` | 双标签 cat + indoor，另有 vehicle 误报 | 合法，参与指标 |
| `s02_single` | 单标签 vehicle，分数低于真值下界 | 合法漏报，参与指标 |
| `s03_empty` | 空真值标签 | 合法输入，不报错 |
| `s04_unknown` | 真值含类别表外 `unicorn` | `E_UNKNOWN_LABEL` |
| `s05_nan` | cat 分数为 NaN | `E_NUMERIC` |
| `s06_dim_mismatch` | 清单声明尺寸与 PNG 不符 | `E_DIM_MISMATCH` |
| `s07_conflict` | cat/dog 同时为真且预测为正 | `E_LABEL_CONFLICT` 警告 |
| `s08_multi` | 双标签 day + outdoor，另有 vehicle 误报 | 合法，参与指标 |

## 4. 四阶段语义

### 阶段 1：图像解码与预处理

- Pillow 解码本地 PNG，强制 RGB；读取实际宽高。
- 与清单 `width`/`height` 比较。不符时产生 `E_DIM_MISMATCH`，错误详情含
  `image_id`、`declared`、`actual`，同时保留诊断与实际画像。
- 固定归一化：`(pixel / 255 - 0.5) / 0.5`。
- 按 `preprocess.block_size` 分块，计算归一化 RGB 均值与块内灰度平均绝对梯度。
- 画像序列化时固定字段顺序、浮点精度与 8 位小数量化，生成 `profile_digest`。
- 元数据缺失产生 `E_MISSING_FIELD`；解码失败产生 `E_IMAGE_DECODE`。

### 阶段 2：逐标签打分校验

- 真值标签：`score >= lower_bound` 为 `hit`，否则为 `miss`。
- 非真值标签：`score >= scoring.default_threshold` 为 `false_positive`，否则为
  `true_negative`。
- 最终正标签集合为 hits + false positives。
- 配置中的互斥标签对若同时出现在真值集合或预测集合，产生 `E_LABEL_CONFLICT`，
  详情包含 `image_id`、`label_pair`、`kinds`。
- 空真值合法，全部低分时为 true negatives。
- 未知标签、NaN/Inf、打分表长度不符分别产生 `E_UNKNOWN_LABEL`、`E_NUMERIC`、
  `E_SCORE_LENGTH`。硬错误样本不进入指标；冲突是 warning，仍进入指标与冲突明细。

### 阶段 3：多标签指标

- 逐标签 TP/FP/FN/TN、precision、recall、F1、support、average precision。
- AP：按 `(-score, sample_key)` 排序，在真值正例处累加 `precision@rank` 后除以正例数；
  无正例时 AP 为 `null`，不做无依据推断。
- micro：跨标签汇总 TP/FP/FN 后计算 P/R/F1。
- macro：对有定义标签的指标做算术平均。
- 共现矩阵：分别统计真值与预测的标签对角/共现次数，输出差值矩阵；
  `consistency = 1 - sum(abs(delta)) / max(sum(truth+predicted), 1)`。
- 所有浮点输出统一量化，度量只依赖清单真值和显式 stage-2 判定，不推断缺失信息。

### 阶段 4：报告与失败定位

- 每样本写出状态、失败阶段/错误码/详情、冲突、上游摘要与来源路径。
- 汇总阶段状态、指标、失败列表、冲突列表、逐样本检查点产物摘要，形成
  `reports/report.json`。
- 报告不含时间戳、进程号或检查点序号，因此停止恢复、重算后仍可逐字节一致。

## 5. 错误码

| 错误码 | 触发点 | 稳定诊断字段 |
|---|---|---|
| `E_DIM_MISMATCH` | 阶段 1 尺寸校验 | `image_id`, `declared`, `actual` |
| `E_UNKNOWN_LABEL` | 阶段 2 类别表校验 | `labels`, `classes`, `image_id` |
| `E_NUMERIC` | 阶段 2 分数有限性/数值性校验 | `label`, `value`, `image_id` |
| `E_MISSING_FIELD` | 阶段 1 必需字段校验 | `fields`, 可用的 `image_id`/`sample_key` |
| `E_SCORE_LENGTH` | 阶段 2 打分表长度/标签集合校验 | `expected_labels`, `actual_labels`, `missing_labels` |
| `E_LABEL_CONFLICT` | 阶段 2 互斥判定 | `image_id`, `label_pair`, `kinds` |
| `E_STALE_CHECKPOINT` | 发布/读取检查点 | `reason`, 阶段、序号、摘要或样本错位详情 |

注入接口在运行前把操作应用到内存清单，不修改原始 manifest；注入文件本身作为来源摘要进入
报告。注入只改变目标样本的相关 fingerprint，因此未涉及样本的检查点继续复用。

## 6. 检查点、失效传播与并发保护

检查点布局：

```text
checkpoints/stages/<stage>/index.json
checkpoints/stages/<stage>/records/<sequence>__<sample-hash>.json
checkpoints/.publish_guard.json
checkpoints/invalidations.json
checkpoints/control/run_state.json
checkpoints/control/stop.request
```

每个检查点记录包含：阶段名、阶段序号、`sample_key`、`image_id`、全局阶段序号、输入内容
指纹、逐字段输入摘要、配置版本、产物摘要、上游产物摘要与实际产物。

原子性：

- 记录、索引、高水位和报告均写入同目录临时文件，`fsync` 后 `os.replace`，再 `fsync`
  目录；中断不会留下可被读取的半文件。
- 记录文件 append-only；索引只在完整记录写完后切换指针。孤立临时文件/未入索引记录不会被读取。

增量失效：

- 阶段 1 指纹包含图像 SHA-256、声明尺寸、预处理配置与图像来源。
- 阶段 2 指纹包含阶段 1 产物摘要、真值、打分表、类别表与打分配置。
- 阶段 3 指纹包含阶段 2 产物摘要与指标配置。
- 阶段 4 指纹包含上游摘要、报告配置与来源元数据。
- 规划器从阶段 1 顺序比较，找到最早的 stale/missing 阶段；删除该阶段及下游的当前索引指针，
  写入失效审计后重建。上游未变结果继续复用。
- `status` 同时返回记录的 `input_fingerprint` 与当前计算得到的 fingerprint，可在运行前
  观测 stale 状态与失效原因。

并发发布：

- 所有发布、索引更新和失效操作使用 `checkpoints/.publish.lock` 上的 `flock`。
- 调用方可提供 `expected_sequence` 与 `expected_sample_key`；只有 `current + 1` 且样本身份
  一致时才发布。落后、重复、跨样本错位均返回 `E_STALE_CHECKPOINT`。
- 阶段索引计算记录映射摘要；`checkpoints/.publish_guard.json` 保存独立高水位。读取与发布时
  校验序号回退和摘要不匹配，拒绝被整体替换的旧快照。

停止/恢复：

- `stop` 写入 `checkpoints/control/stop.request`；每个不可拆分的“样本-阶段”单元完成后检查。
- `run --stop-after N` 用于确定性演示，在第 N 个新发布检查点后停止。
- `resume` 清除停止标记并从已有检查点继续；最终报告不包含序号/时间，测试验证与一次从零
  运行逐字节相同。

## 7. 命令行

```bash
# 数据与运行
python3 -m workbench generate --seed 20261121 [--no-clean]
python3 -m workbench run [--config configs/default.json] [--stop-after N]
python3 -m workbench resume
python3 -m workbench stop

# 查询
python3 -m workbench status
python3 -m workbench stages
python3 -m workbench sample s01_dual
python3 -m workbench failures
python3 -m workbench conflicts
python3 -m workbench invalidations --limit 20

# 异常注入（写入 configs/injection.json）
python3 -m workbench inject dim_mismatch --sample s01_dual --width 99 --height 88
python3 -m workbench inject unknown_label --sample s02_single --label alien
python3 -m workbench inject numeric --sample s02_single --label vehicle --value NaN
python3 -m workbench inject numeric --sample s02_single --label vehicle --value Inf
python3 -m workbench inject missing_field --sample s02_single --field image_id
python3 -m workbench inject score_length --sample s02_single --label vehicle
python3 -m workbench injection
python3 -m workbench clear-injection

# 本地导入、导出、HTTP、端到端演示
python3 -m workbench import --manifest samples/manifest.json --name imported-demo --overwrite
python3 -m workbench export --output reports/exported_report.json
python3 -m workbench serve --host 127.0.0.1 --port 8765
python3 -m workbench demo --seed 20261121 --stop-after 5
```

## 8. 本地 HTTP API

仪表盘：`GET /`。常用接口：

| 方法 | 路径 | 功能 |
|---|---|---|
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 运行状态、阶段完整性、样本 fingerprint/stale 状态、失效审计 |
| GET | `/api/stages` | 阶段序号、记录数、快照摘要、guard 完整性 |
| GET | `/api/samples` | 样本阶段矩阵 |
| GET | `/api/sample?key=...` | 单样本来源、原始清单记录与四阶段检查点 |
| GET | `/api/sample-image?key=...` | 本地 PNG |
| GET | `/api/report` | 完整确定性报告 |
| GET | `/api/report.sha256` | 报告 SHA-256 |
| GET | `/api/failures` | 失败样本定位 |
| GET | `/api/conflicts` | 互斥冲突明细 |
| GET | `/api/invalidations?limit=50` | 增量失效审计 |
| GET | `/api/export` | 下载报告 |
| POST | `/api/run` | 后台触发运行，可带 `mode`、`stop_after`、`clear_outputs` |
| POST | `/api/resume` | 恢复运行 |
| POST | `/api/stop` | 协作停止请求 |
| POST | `/api/inject` | 追加异常注入 |
| POST | `/api/clear-injection` | 清除注入 |
| POST | `/api/generate` | 重建确定性演示数据 |
| POST | `/api/import` | 导入工作区内本地清单/图像 |
| POST | `/api/export` | 导出报告到工作区路径 |

服务只监听指定本地地址，默认 `127.0.0.1`，不访问外部网络。

## 9. 实际验证结果

详细记录见 [`docs/VALIDATION.md`](docs/VALIDATION.md)。

- 自动化测试：`20` 项全部通过，耗时约 `13.8s`。
- 基线：8 个样本、28 条检查点；5 个样本进入指标；3 个硬失败；1 个互斥冲突。
- 基线报告 SHA-256：`19f497ba8165b0b6c699cb738641898a77700382d66ec85a922e622a5b29b60f`。
- micro：TP=6、FP=2、FN=1，precision=0.75、recall=0.857142857143、F1=0.8。
- macro：precision=0.833333333333、recall=0.833333333333、F1=1.0、AP=1.0。
- 共现一致性：`0.8`。
- 停止于第 5 条检查点后恢复，最终报告仍为上述 SHA-256，逐字节一致。
- 注入 `s08_multi` 的打分表长度异常后产生 `E_SCORE_LENGTH`；仅从 stage 2 起失效，
  `s01_dual` 的 stage-2 产物摘要不变；清除注入后报告恢复为同一 SHA-256。

## 10. 复现与边界说明

- 报告、失败、冲突、演示摘要均使用排序键、固定浮点量化和确定性 JSON 序列化。
- 清单中非有限分数使用字符串 token，避免依赖 Python 对非标准 JSON 的宽容解析。
- 无正例标签的 AP、无预测标签的 precision 等无定义值明确输出 `null`，不伪造数值。
- 检查点 guard 位于 `checkpoints/` 根目录；若只替换单个阶段目录，可通过序号回退/摘要不匹配
  识别。若连同整个工作区信任根一起替换，则属于工作区外信任问题，不在本工具可检测范围内。

