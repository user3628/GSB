# 合同条款要素抽取组件

这是一个可从零重建、离线运行、结果逐字节确定的合同条款领域组件。它读取本地 UTF-8 JSONL 合同分片，完成三级处理：

1. **规范化与切分**：Unicode NFKC、全半角归一、空白折叠、条款序号剥离、按中文句读/换行切分，并输出稳定 `clause_id` 与原始字符偏移。
2. **要素抽取**：用本地种子标注估计正则规则与词典条目的权重，抽取金额、日期、期限、违约金比例四类候选，使用定点 Decimal 归一金额与比例。
3. **冲突裁决**：在同一 `contract_id` 内按可配置生效日期、条款/模板优先级及确定性仲裁键选择唯一值，同时保留全部候选、淘汰值和淘汰依据。

组件不依赖真实模型、GPU、外部 API 或网络。运行依赖只有 NumPy，项目内附带从离线环境重打包的本地 wheel。

## 快速开始

环境要求：Python 3.10+。当前仓库的默认本地 wheel 为 CPython 3.14 / Linux x86_64，因此 `scripts/setup_local.sh` 默认使用 `python3.14`。

```bash
bash scripts/setup_local.sh
source .venv/bin/activate

# 重建全部确定性样本
python scripts/build_samples.py

# 运行 21 项测试
./run_tests.sh

# 一键执行测试、停止恢复、异常注入、v1 互操作和完整验证
bash scripts/demo_all.sh
```

离线安装过程只执行：

```text
pip install --no-index --find-links vendor numpy
pip install --no-index --no-build-isolation .
```

固定版本也记录在 `requirements.txt`，但演示安装不会访问 PyPI。

`vendor/numpy-2.5.5-cp314-cp314-manylinux_2_27_x86_64.whl` 由 `scripts/build_numpy_wheel.py` 从本机预装 NumPy 重打包，网络不会被访问。项目自身使用无第三方构建依赖的 `build_backend.py`，因此也能在隔离环境中离线安装。

## 项目结构

```text
contract_elements/
  normalization.py   NFKC/空白/序号/切分/偏移映射
  extraction.py      正则规则、词典证据、种子权重估计、定点数值
  arbitration.py     合同内冲突裁决与淘汰诊断
  interop.py         结构版本 1 导入/回导固定点
  versioning.py      内容寻址缓存、阶段指纹与失效原因
  pipeline.py        批量编排、原子检查点、停止恢复、实验产物
  experiment.py      精确率/召回率、冲突准确率、兼容成功率
  service.py         本地 HTTP 服务
  cli.py             命令行入口
config/default.json  规范化、抽取、裁决、运行时配置
data/                36 条合同、14 条种子、词典、v1 样例、冲突真值、注入参数
scripts/             重建、离线安装、演示与验证脚本
tests/               21 项 unittest
artifacts/validation.json  本次实际验证结果与产物哈希
```

## 样本与输入协议

`data/contracts.jsonl` 含 36 条合同条款、9 个合同，覆盖四类要素、中英混排、字典独有表达和冲突取值。每行核心字段：

```json
{
  "contract_id": "C-ALPHA-01",
  "template_id": "TPL-MASTER",
  "clause_no": 1,
  "clause_text": "第1条 甲方应于2025年1月15日前支付人民币1,200,000.00元。",
  "effective_date": "2025-03-15",
  "annotations": [
    {"element_type": "amount", "start": 20, "end": 40, "normalized_value": "CNY:1200000.00"}
  ]
}
```

- `annotations` 是真值/种子结构标注，使用原始 `clause_text` 的 Unicode 字符偏移。
- 抽取器只使用 `data/seeds.jsonl` 估计权重；输入行上的标注用于验证和指标真值，不参与候选规则打分。
- 候选偏移使用 `normalized_clause_codepoint`，即拆分后 `unit.text` 的 Unicode 字符偏移。

规范化后的值格式固定：

| 要素 | 归一化示例 |
|---|---|
| 金额 | `CNY:1200000.00`、`USD:50000.00` |
| 日期 | `2025-01-15` |
| 期限 | `business_day:30`、`day:45`、`month:12`、`year:2` |
| 违约金比例 | `0.000500`、`0.002500` |

金额精度由 `extraction.amount_scale` 控制，比例精度由 `extraction.ratio_scale` 控制。`NaN`、无穷值和超过 `max_abs` 的定点溢出不会静默消失：候选仍会保留为 `valid:false`，并返回 `E_NUMERIC`。

## 正则-词典打分

`SeedEstimator` 在本地种子标注上计算每条正则规则/词典条目的命中数、精确率与权重：

```text
weight = 0.5 + 0.5 * precision
confidence = 0.45 + 0.35 * regex_score + 0.20 * dictionary_score
```

可通过 `extraction.regex_weight_overrides` 与 `extraction.dictionary_weight_overrides` 覆盖权重。候选保留全部证据：

```json
{
  "candidate_id": "C-ALPHA-01#0001.00....:amount:...",
  "element_type": "amount",
  "start": 17,
  "end": 40,
  "normalized_value": "CNY:1200000.00",
  "confidence": "0.995000",
  "evidence": [
    {"kind": "regex", "id": "AMT_CNY_PREFIX", "weight": "1.000000"},
    {"kind": "dictionary", "id": "DICT_AMT_CNY", "weight": "1.000000"}
  ],
  "valid": true
}
```

`data/elements.json` 同时包含证据词典条目和可直接产生候选的词典条目，例如 `半年 -> month:6`。

## 冲突裁决

`config/default.json` 中的默认排序为：

```json
["effective_date", "clause_priority", "clause_no", "unit_index", "source_start", "candidate_id"]
```

- `effective_date`、`clause_priority` 降序；其余升序。
- 优先级由 `clause_priority[clause_no] + template_priority[template_id]` 构成。
- 生效日期或优先级并列时，继续比较条款号、单元序号、偏移与 `candidate_id`，保证确定性。
- 每个裁决记录都包含 `evaluated_candidates` 和 `eliminated`；淘汰项包含值、候选 ID、双方排名和具体原因，如 `older_effective_date`、`lower_clause_priority`、`higher_clause_no`。

## 结构版本 1 互操作

`data/annotations_v1.jsonl` 是 `structure_version: 1` 样例：

- `elements[].start/end` 为原文字符偏移；
- 金额使用带千分位字符串，例如 `"1,250,000.00"`；
- clause 不包含 `clause_no`，导入时从 `第N条` 推导，否则使用合同内序号；
- 支持 `codepoint` 与 `utf8_byte` 两种偏移基准，可在 `interop.offset_basis` 配置。

导入后转换为内部版本 2 结构，再导出回版本 1。验证脚本执行 `import -> export -> import -> export`，两次导出的 v1 JSONL 和两次内部版本 2 结构均逐字节一致：

```bash
contract-elements import-v1 \
  --output output/demo/interop/annotations_v1.export.jsonl \
  --internal-output output/demo/interop/internal_v2.json

contract-elements export-v1 \
  --internal-input output/demo/interop/internal_v2.json \
  --output output/demo/interop/annotations_v1.reexport.jsonl

cmp output/demo/interop/annotations_v1.export.jsonl \
    output/demo/interop/annotations_v1.reexport.jsonl
```

## 异常注入与业务错误码

注入参数在 `data/injections.json`，仅在读取后对内存副本生效，不修改输入、源码或配置存储。

| 场景 | 业务码 | 阶段 |
|---|---|---|
| 非法条款序号 | `E_INVALID_CLAUSE_NO` | `normalize` |
| 超长条款 | `E_TEXT_TOO_LONG` | `normalize` |
| 未知标注要素类型 | `E_UNKNOWN_ELEMENT` | `extract` |
| 生效日期格式非法 | `E_INVALID_DATE` | `arbitrate` |
| NaN/定点溢出 | `E_NUMERIC` | `extract` |
| 空条款/仅序号 | `E_CLAUSE_EMPTY` | `normalize` |

错误对象始终带 `code`、`contract_id`、`stage`，并按需附带偏移、要素名和上限等 `details`。

```bash
contract-elements batch \
  --output output/demo/injected \
  --runtime output/demo/injected-runtime \
  --injections data/injections.json
```

## 版本指纹、缓存与失效

每个阶段有独立依赖组件与内容摘要：

- `normalize`：原文、合同/条款标识、规范化配置。
- `extract`：规范化结果、种子标注、词典、权重、金额/比例精度。
- `arbitrate`：抽取结果、裁决顺序、优先级、生效日期。

缓存按内容寻址存放在 `RUNTIME/cache/`。变更时从最早受影响阶段开始失效：

- 改文本/序号：从 `normalize` 开始。
- 改种子、词典、权重或精度：复用 `normalize`，从 `extract` 开始。
- 只改优先级/生效日期：复用规范化与抽取，只重建 `arbitrate` 并发出新裁决。

运行时状态写入 `RUNTIME/version_state.json`，包含总体 `version_fingerprint`、三级 `stage_digests`、依赖摘要、`invalidations`、缓存命中与运行计数：

```bash
contract-elements version --runtime output/validation/full-runtime
```

实际验证中，将 `clause_priority["1"]` 从 100 改为 1 后，首次失效阶段为 `arbitrate`，原因 `arbitration_priority_or_policy_changed`；规范化与抽取的摘要保持不变，缓存命中分别为 36/36，裁决缓存 0 命中。

## 检查点与停止恢复

批量执行按规范化、抽取、裁决阶段推进。每完成一个不可拆分处理单元就原子写入 `RUNTIME/checkpoint.json`，其中 `seq` 严格单调递增。停止请求有两种方式：

```bash
# 可复现的自动停止演示
contract-elements batch ... --stop-after-units 7

# 实际运行中的协作式停止请求
contract-elements stop --runtime output/demo/resume-runtime
contract-elements resume --output ... --runtime output/demo/resume-runtime
```

停止时只完成当前单元，保存检查点后退出。恢复会核对 `run_id`，若输入、种子、词典或配置已变化则返回 `E_CHECKPOINT_MISMATCH`。本次验证在第 7 个单位停止，恢复后最终 6 个产物与一次完整运行逐字节一致。

## 命令行

```bash
contract-elements batch [--injections FILE] [--resume] [--stop-after-units N]
contract-elements experiment [--injections FILE]
contract-elements parse --contract-id C-ALPHA-01
contract-elements candidates --contract-id C-ALPHA-01
contract-elements conflicts --contract-id C-ALPHA-01
contract-elements version --runtime RUNTIME
contract-elements stop --runtime RUNTIME
contract-elements resume --output OUTPUT --runtime RUNTIME
contract-elements import-v1 --output V1_JSONL --internal-output INTERNAL_JSON
contract-elements export-v1 --internal-input INTERNAL_JSON --output V1_JSONL
contract-elements serve [--host 127.0.0.1] [--port 18080]
```

所有数据路径均可通过 `--config --input --seeds --dictionary --v1 --gold --output --runtime` 覆盖。

## 本地 HTTP 接口

默认监听 `127.0.0.1:18080`，只使用标准库 `ThreadingHTTPServer`：

```bash
contract-elements serve
```

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 健康检查 |
| GET | `/v1/version` | 版本指纹、阶段摘要、失效原因、检查点进度 |
| GET | `/v1/contracts/{contract_id}` | 单合同完整解析 |
| GET | `/v1/contracts/{contract_id}/candidates` | 要素候选与证据 |
| GET | `/v1/contracts/{contract_id}/conflicts` | 冲突裁决与淘汰依据 |
| POST | `/v1/experiments` | 执行批量实验 |
| POST | `/v1/stop` | 写入协作式停止请求 |
| POST | `/v1/resume` | 从检查点恢复 |
| POST | `/v1/interop/import` | 导入 v1 并返回内部结构与兼容性 |

## 可复现性与实验摘要

- 所有 JSON/JSONL 使用 UTF-8、键排序、紧凑分隔符、末尾换行。
- 输出中不含时间戳、随机数、主机名或进程号。
- 比例与指标以固定精度字符串保存，避免浮点展示差异。
- 批量完成后写 `results.jsonl`、`candidates.jsonl`、`conflicts.jsonl`、`errors.jsonl`、`summary.json`、`experiment_summary.json`。
- `experiment_summary.json` 包含配置指纹、实验身份、版本指纹、显式真值指标的计数、产物 SHA-256，以及互操作固定点摘要。

指标定义：

- **要素精确率/召回率**：每条 clause 每个要素类型取置信度最高的有效候选，与显式 `annotations` 规范化真值按 `(clause_id, element_type, value)` 匹配。
- **冲突裁决正确率**：逐个读取 `data/gold_conflicts.json` 的期望胜者，同时要求诊断中存在 `conflict: true` 和非空 `eliminated`。
- **版本兼容转换成功率**：逐条 v1 记录执行两次导入/导出固定点验证。

## 本次实际验证结果

完整机器可读结果见 `artifacts/validation.json`。本次在 Python 3.14.4、NumPy 2.5.5 下执行：

```text
测试：21 项全部通过
合同条款：36 条，9 个合同
种子标注：14 条
词典条目：46 条
候选：51 个，有效 51 个
裁决要素：34 个，冲突 7 个
要素精确率：1.000000
要素召回率：1.000000
要素 F1：1.000000
冲突裁决正确率：1.000000（7/7）
版本兼容转换成功率：1.000000（2/2，逐字节固定点）
停止点：seq=7；恢复完成：seq=82
停止恢复 6 个产物与完整运行：逐字节一致
四类注入：E_INVALID_CLAUSE_NO / E_TEXT_TOO_LONG / E_UNKNOWN_ELEMENT / E_INVALID_DATE 均命中预期阶段
config_fingerprint：c447cc45cd8e2e0803b1bc616f7558af9c5e5a14f0093d050417f57c55f15517
version_fingerprint：8a753c03a21d27b97678122dbab5b0918e1cfc9b1441245a62fd8c4d8a80eb4b
```

`results.jsonl` SHA-256：`c971aec7893e4b3544e989b65abfb835552bcdead41929a683ef62291eaa8b05`

## 设计边界

- 规则与词典是本地、可解释、可审计的领域基线，不追求开放域语义泛化。
- 金额支持 CNY/RMB、USD、HKD 及元/万元/亿元等常见写法。
- v1 互操作支持字符/UTF-8 字节偏移、千分位金额、缺省条款序号推导；返回样例是规范化固定点，不承诺保留输入 JSON 的原始键顺序。
- 冲突语义按要素类型而非合同角色建模；生产接入时可在 `annotations` 或扩展规则中增加角色字段。
