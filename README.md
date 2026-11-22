# 本地确定性批处理文本分类流水线

这是一个仅依赖 Python 标准库与 NumPy 运行环境的本地批处理分类项目。输入为固定随机种子生成的 JSONL 文本分片，管线完成：

1. 文本规范化、中文单字/相邻二字与拉丁连续串切分、稳定特征哈希；
2. 按批多项朴素贝叶斯训练、未知词回退、对数域打分与显式诊断；
3. 留出折阈值校准、误报率约束和待复核裁决；
4. 语言/来源通道分层、类别加权和整数/定点累加汇总；
5. 评估预算调度、确定性并行归约；
6. 四阶段原子检查点、优雅停止与恢复；
7. 内容/配置版本指纹、阶段失效传播和单批增量替换；
8. `flock` 并发发布保护、稳定业务错误码和完整本地演示。

项目不使用 Docker、不访问网络，也不依赖外部随机性或服务端状态。

## 快速开始

```bash
python3 -m pip install -r requirements.txt
make doctor
make data
make verify
make run
```

如果已存在 `data/` 和 `demo_workspace/`，也可以直接重复执行 `make run`。相同配置下，已完成阶段会从检查点复用。

主要入口：

```bash
python3 -m pipeline.cli generate-data --out data --seed 20261123
python3 -m pipeline.cli verify-data --index data/index.json --output data/verification.json
python3 -m pipeline.cli run --workspace demo_workspace --config data/config.yaml --data data/index.json
python3 -m pipeline.cli verify-run --workspace demo_workspace
```

## 演示数据

`generate-data` 固定使用种子 `20261123`，生成 4 个批次、共 8,800 条：

| 批次 | 档位 | 条数 | 类别数 |
| --- | --- | ---: | ---: |
| `batch_01_small` | small | 400 | 4 |
| `batch_02_small` | small | 400 | 4 |
| `batch_03_large` | large | 4,000 | 7 |
| `batch_04_large` | large | 4,000 | 7 |

样本包含中文、英文、中英混排、全角字符、全半角与空白变化、极端不均衡类别和空正文。每条记录结构为：

```json
{
  "doc_id": "b01-000001",
  "text": "正文",
  "lang": "zh|en|mixed",
  "channel": "api|app|import|web",
  "label": "account|billing|...",
  "risk_level": 0
}
```

`label` 是多类单标签，`risk_level` 是 0-3 级风险元数据，不参与训练目标。批次序号、语言标签、通道、类别分布、空正文数量和内容摘要由 `verify-data` 核对。

加载器也接受 `body`、`language`、`source_channel`/`source`、`category`、`risk` 作为对应字段的兼容别名。

## 配置

`data/config.yaml` 声明哈希维度、平滑强度、类别阈值、误报率上限、评估预算、并行工作线程和校准参数。核心字段：

```yaml
version: "demo-2026-11-23-v1"
hash_dim: 1024
smoothing: 0.35
unknown_token: "<UNK>"
labels: [account, billing, content, fraud, privacy, security, spam]
thresholds: {account: 0.10, billing: 0.10, content: 0.10, fraud: 0.05}
target_fpr: 0.08
threshold_max: 0.98
evaluation_budget: 9000
workers: 4
class_weights: {account: 1.0, billing: 1.2, content: 1.5, fraud: 2.0}
calibration:
  fold_modulus: 5
  fold_remainder: 0
  min_samples_per_class: 5
```

实际数组与映射使用 YAML 块格式，示例仅为字段说明。项目内置小型 YAML 子集解析器，因此除 NumPy 外不需要安装 PyYAML。

配置校验错误码：

- `E_INVALID_CONFIG`：字段缺失、类型错误、阈值越界、权重非正等；
- `E_INVALID_SMOOTHING`：平滑强度为负；零被允许，但会产生显式零概率诊断；
- `E_INVALID_FPR_LIMIT`：误报率上限不在 `[0, 1]`。

## 核心语义

### 规范化与特征哈希

- `Unicode NFKC` 统一全半角与兼容字符；
- 空白折叠为单个 ASCII 空格并裁剪；
- 使用 Unicode lower 小写化；
- 中文连续区段生成单字与相邻二字词项；
- ASCII 拉丁字母与数字连续串作为一个词项；
- 词项使用固定参数的 BLAKE2b 哈希到 `hash_dim`，不使用 Python 随机化哈希；
- 特征记录保留 `position`、`start`、`end`、`bucket`、`hash_collision_count` 和冲突桶；
- 规范化后为空返回 `E_EMPTY_DOC`；非空但无可用词项返回 `E_NO_TOKENS`，均保留在特征与诊断 JSONL 中。

哈希冲突不丢弃词项：训练按哈希桶合并计数，同时保留 `bucket_terms` 作为冲突追踪依据。

### 多项朴素贝叶斯

每批独立训练。类别先验使用训练文档的极大似然估计；词项条件概率使用配置平滑：

```text
log P(c) + sum_token log((count(c, bucket) + alpha) / (tokens(c) + alpha * (V + 1)))
```

`V + 1` 为已观测哈希桶和未知标记桶。训练集中未见过的原始词项回退到 `unknown_token`。所有后验分数在对数域通过 `math.fsum` 累加；概率仅用于报告与阈值裁决。

显式异常：

- `alpha = 0` 时，零计数条件概率写为 `null`/`-inf` 语义并产生 `E_ZERO_SMOOTHING`，不会崩溃或静默补值；
- 声明类别没有训练样本时，输出 `log_prior: null` 与 `E_EMPTY_CLASS`，不删除类别；
- 空正文和零词项文档不参与训练，但仍在逐条分数中带错误码返回。

同一批输入与配置会得到相同的模型摘要、参数摘要和逐条分数。

### 阈值校准与裁决

按 `sha256(doc_id) % fold_modulus == fold_remainder` 固定划分留出折。用训练折训练、留出折打分，对每个类别执行一对多阈值选择：

- 从配置阈值下界到 `threshold_max` 搜索可达到目标 FPR 的最小阈值；
- 输出阈值、命中数/命中率、误报数/误报率、是否达到目标、正负样本计数、全部校准 `doc_id` 与分数、样本范围；
- 正或负校准样本低于下限：`E_CALIBRATION_LOW_SAMPLES`；
- 在允许阈值范围内无法达到 FPR 上限：`E_FPR_UNREACHABLE`，保留可达的最优阈值；
- 类别无样本：`E_EMPTY_CLASS`。

最终单标签裁决为：概率达到各自阈值的类别中选最高者；若无类别达标，结果为 `REJECT` + `E_THRESHOLD_REJECTED`。预测类别对应校准需要复核时，结果保留预测但标记 `REVIEW`。

每条结果包含：

- `class_decisions`：每个类别的概率、阈值、是否达标、校准是否待复核；
- `score_evidence`：批次分数 JSONL 的相对路径、行号和文件摘要；
- `calibration_ref`：校准产物的相对路径、文件摘要和校准摘要。

分数明细包含每个词项的有效词项/桶、未知词回退、每类对数先验、似然和、最终对数分数与概率。模型文件包含类别计数、桶计数、桶词项、分母和每桶对数似然，因此裁决可从模型与分数明细重新计算。

### 分层汇总

汇总同时生成：

- 总体；
- `by_language`：每个语言 × 类别，包含零样本记录；
- `by_channel`：每个通道 × 类别，包含零样本记录；
- `by_language_channel`：语言与通道交叉层，包含零样本记录。

精确率、召回率、F1、TP/FP/FN/支持数来自逐条结果按 `doc_id` 排序后的整数计数。类别权重先转换为 `1e-9` 定点整数，加权分子/分母也使用整数累加，最终只做一次浮点除法。报告中的 `results_digest` 与按排序结果重算的汇总摘要用于验证一致性。

### 调度与并行

- 以整批为不可拆分调度单元；
- 接纳前读取批次元数据中的 `row_count` 预估消耗；
- 不超过 `evaluation_budget` 的批次按批号接纳，超预算批次写入 `schedule.json`，原因为 `E_EVALUATION_BUDGET_EXCEEDED`，并继续检查后续可能适合的批次；
- 使用 `ThreadPoolExecutor`，最多 `workers` 个批次并行；
- 不使用线程超时；所有已提交批次必须完成或显式抛错，线程池以 `wait=True` 收敛；
- 主线程按批号和 `doc_id` 排序归约，最终产物顺序不依赖完成顺序。

`--execution-workers N` 可用于验证执行并发度，但不修改配置，也不改变最终产物；它会被限制在配置的 `workers` 以内。

### 检查点与恢复

每批四个阶段分别发布检查点：

```text
features -> train_score -> calibration -> summarize
```

每条检查点记录包含全局单调序号、批次、阶段、批内阶段序号、输入内容指纹、配置版本、产物路径/摘要和各产物摘要。发布流程：

1. 对 `checkpoints/publish.lock` 加真实 `fcntl.flock` 排他锁；
2. 扫描并校验已有记录序号连续；
3. 校验批内阶段连续、前置产物存在且摘要正确；
4. 将新记录写入同目录临时文件，`fsync` 后 `os.replace`，再同步目录；
5. 只有完整最终文件可见，临时文件不会被当成记录。

业务错误：

- `E_STALE_CHECKPOINT`：落后、重复、`expected_sequence` 不匹配、产物跨批错位；
- `E_INVALID_STAGE`：未知阶段、阶段跳转、缺失前置产物；
- `E_MISSING_ARTIFACT`：当前提交的产物缺失。

收到 `SIGINT`、`SIGTERM` 或停止文件信号时，已提交的一窗批次会完成四阶段并发布，之后停止调度。再次运行会从已完成检查点继续。最终 `results.jsonl`、`summary.json`、`calibrations.json`、`parameters.json`、`report.json` 与从零完整运行逐字节一致；检查点全局序号保持连续，但恢复运行的具体序号数量可能因中断点不同而不同。

### 版本指纹与增量失效

每个阶段使用内容摘要和配置子集构成版本指纹：

| 变化 | 最早失效阶段 |
| --- | --- |
| 批次正文、哈希维度、规范化/分词/哈希实现版本 | `features` |
| 平滑强度、未知标记、标签集合、留出折参数 | `train_score` |
| 类别阈值、误报率上限、阈值上限、校准样本下限 | `calibration` |
| 类别权重、汇总摘要模式 | `summarize` |

上游摘要匹配时会复用已有产物；失效阶段及全部下游写入显式 `invalidate` 记录后重建。单批替换入口：

```bash
python3 -m pipeline.cli replace-batch \
  --workspace demo_workspace \
  --config data/config.yaml \
  --data data/index.json \
  --batch-id batch_01_small \
  --source /path/to/replacement.jsonl
```

该命令原子替换批次文件并更新索引元数据，执行增量运行，再在临时工作区从零全量运行并比较最终产物。不一致返回 `E_INCREMENTAL_MISMATCH`，详情包含首个差异中的 `doc_id`/`batch_id` 和字段路径。

也可以单独检查阶段影响：

```bash
python3 -m pipeline.cli detect-changes --workspace demo_workspace --config data/config.yaml --data data/index.json
```

## 产物

主要稳定产物：

| 路径 | 内容 |
| --- | --- |
| `features/<batch>.jsonl` | 规范化长度、词项偏移、桶、冲突计数、错误码 |
| `diagnostics/features/<batch>.jsonl` | 空正文/无词项等诊断 |
| `models/<batch>.json` | 全量训练模型与参数 |
| `models/<batch>.holdout.json` | 留出折训练模型 |
| `scores/<batch>.jsonl` | 全量逐条分数、词项追踪、对数分数、概率 |
| `scores/<batch>.holdout.jsonl` | 留出折逐条分数 |
| `calibration/<batch>.json` | 阈值、命中率、误报率、校准样本及分数 |
| `results/<batch>.jsonl` | 批内裁决与证据引用 |
| `summaries/<batch>.json` | 批内汇总 |
| `results.jsonl` | 全局按 `doc_id` 排序的逐条结果 |
| `summary.json` | 分层与定点加权汇总 |
| `calibrations.json` | 跨批阈值/复核摘要 |
| `parameters.json` | 配置摘要与每批模型参数摘要 |
| `schedule.json` | 预算接纳/拒绝情况 |
| `report.json` | 确定性验收摘要与产物摘要 |
| `incremental_report.json` | 失效记录与检查点序号状态 |
| `checkpoints/records/*.json` | 不可变、连续序号的阶段记录 |

`report.json` 不包含时间戳、绝对路径、运行耗时或检查点数量，因此可用于逐字节比较。`incremental_report.json` 和 `run_manifest.json` 是诊断文件，不属于最终结果等价比对集合。

## 验证

完整测试：

```bash
python3 -m unittest discover -v
```

当前环境与最近一次完整运行的记录见 [VERIFICATION.md](VERIFICATION.md)。测试覆盖：

- NFKC/空白/全半角、CJK 单字与二字、拉丁串、偏移、冲突计数、空正文；
- NB 确定性、未知词回退、零平滑与空类别显式诊断；
- 阈值 FPR 约束、低样本复核、不可达目标；
- 分层零样本记录与定点加权汇总；
- 预算拒绝与后续可接纳批次；
- 原子检查点失败、重复/跳阶段/跨批/缺前置产物、双进程竞争；
- 并行与顺序最终产物逐字节一致、停止恢复一致；
- 平滑/哈希/阈值/FPR/权重的最早失效阶段；
- 单批增量替换与从零全量一致、首个差异错误码；
- 非法平滑与不可达 FPR 注入。

## 设计边界

- 训练严格按批进行，不跨批合并参数；这样可以验证批内确定性、恢复和增量复用。
- 阈值校准是一对多留出折校准，最终单标签裁决采用阈值达标类别中的最高概率。
- 空正文/无词项文档必须显式报错并从分类指标中排除，不从先验或缺失值推断类别。
- `risk_level` 是输入元数据，不是第二训练目标。
- 类别先验采用未平滑 MLE；配置平滑强度用于词项条件概率。零样本类别通过显式空先验表达。
- 本项目定位为确定性本地演示与验证工具，不包含生产级网络服务、认证或分布式对象存储。
