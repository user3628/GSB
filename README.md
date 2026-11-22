# POS 标注体系迁移与跨版本对齐引擎

这是一个完全本地、确定性的词性标注资产治理引擎。它从旧词性体系（自由字符串、大小写/空白/Unicode 变体、自定义扩展标签）生成候选标准标签，对齐同一批语料的旧/新标注，裁决冲突映射，并以分片级检查点、版本化映射表和增量失效传播保证可恢复、可审计。

不依赖模型、词典服务、GPU、数据库、容器或外部网络。

## 交付内容

- `posalign/`：引擎、CLI 与本地 HTTP API。
- `scripts/generate_samples.py`：确定性 JSONL/YAML 样本生成器。
- `scripts/demo.py`：一键完整演示。
- `samples/`：3 个正常分片、治理 YAML、对齐失败注入分片、固定摘要文件。
- `tests/`：33 项自动化测试。
- `artifacts/validation_report.json`、`artifacts/TEST_RESULTS.md`：本次实际验证记录。

## 环境与安装

要求 Python 3.11+ 与 NumPy。治理 YAML 优先使用 PyYAML；若 PyYAML 不可用，会自动使用仓库内的受限 YAML 子集解析器。代码已在 Python 3.14.4 / NumPy 2.2.1 上实测，语法与行为兼容 Python 3.11+。

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/generate_samples.py
```

运行测试时安装开发依赖：`python -m pip install -r requirements-dev.txt`。

Windows PowerShell：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\generate_samples.py
```

也可以执行 `python -m pip install -e .` 后直接使用 `posalign` 命令；不安装时统一使用 `python -m posalign`。

## 输入

正常分片为 UTF-8 JSONL，每行一条标注记录：

```json
{"sent_id":"v2-001","tokens":["I","爱","NLP","。"],"old_tags":["Pronoun","verB Gerund","Noun+Custom","，"],"new_tags":["PRON","VERB","NOUN","PUNCT"],"annotator":"annotator-a","structure_version":2,"confidence":0.94}
```

- `sent_id`：跨分片必须唯一。
- `tokens`：旧标注已切分 token；长度须等于 `old_tags`。
- `new_tags`：可缺省。缺省时记录作为 migration-only 的 `unpaired` 句，不作为对齐失败；若存在但不能满足对齐阈值则失败。
- `new_tokens`：可选扩展字段，用于表示新标注侧切分结果不同。缺省时等于 `tokens`。这是本实现对“token 序列不一致”的显式本地表示。
- `annotator` / `annotator_id`：标注者标识，用于可靠度加权。
- `structure_version`：`1` 或 `2`。也可用 `schema_version`。
- `confidence`：v2 为 `[0,1]` 小数；v1 必须为整数百分比。

版本 1 额外规则：`tokens: []` 表示整句未切分；导入后标记 `unsplit`、`alignable: false`，在迁移报告与汇总中单列。整数置信度按 `value / 100` 换算，并记录 `from`、`to`、`basis` 与规则来源。升级记录带有 `compatibility.degraded: true` 和转换原因。

治理配置见 `samples/governance.yaml`，包括标准标签集、别名表、规则候选、对齐容差、裁决策略顺序/权重、候选优先级、标注者可靠度先验与置信阈值。

## 四阶段流程

每个分片按以下阶段推进，阶段产物只有完整成功后才发布：

1. **解析/迁移（parse）**：校验记录，升级 v1 结构，执行 NFKC、空白折叠、大小写归并和候选生成。
2. **对齐（align）**：用 token LCS 对齐新旧序列，提取标签对、插入、删除与错位区间。整批分片全部成功后才写该批新对齐检查点；任何失败都不发布新的对齐产物。
3. **裁决（adjudicate）**：跨全部分片聚合标签对证据，选出唯一映射；落选候选和依据完整保留。
4. **汇总（summarize）**：按分片汇总解析、对齐、v1 兼容与映射使用情况，再生成全局确定性业务产物。

### 候选生成

规范化顺序为 Unicode NFKC → 空白折叠 → `casefold()`。候选来源保留为：

- `alias_table`：别名表命中，保留别名给出的全部候选。
- `rule`：标准标签忽略大小写命中，或按 `+ / @ : - _` 等扩展分隔符提取标准词性主干后命中。

别名缺失仍保留规则候选；完全无候选时保留原标签并记录 `alias_missing`、`no_candidate`。多候选记录 `multiple_alias_candidates`、`multiple_candidates`，绝不静默取第一个。

### LCS 对齐与容差

token 默认按 NFKC + `casefold` 比较。设 LCS 匹配数为 `m`，旧/新 token 数为 `n_old`、`n_new`：

```text
alignment_rate = 2 * m / (n_old + n_new)
```

低于 `alignment.min_rate` 时返回 `E_ALIGNMENT_FAILED`，错误 details 包含 `sent_id`、实际对齐率、阈值与旧/新 token。每条对齐句还记录 `edit_operations`、`insertions`、`deletions` 和 `misaligned_spans`。

### 冲突裁决

对候选标准标签 `c`：

```text
frequency(c)             = 票数(c) / 总票数
annotator_reliability(c) = 可靠度加权票数(c) / 全部可靠度加权票数
candidate_priority(c)    = 治理配置中的全局候选优先级得分
confidence(c)            = Σ 策略权重 × 对应得分
```

默认策略顺序为 `frequency → annotator_reliability → candidate_priority`，权重在 YAML 中配置。总分并列时按策略贡献依次比较，仍并列则按标准标签的确定性字典序仲裁。`resolved` 与 `candidate_index` 分开存储/查询；落选候选含 `decision: rejected`、`rejection_reasons`、票数、可靠度贡献和 sent_id 证据。

置信阈值用于标记裁决结果是否可直接消费：低于阈值的唯一胜者仍作为可审计 provisional mapping 保留，但 `accepted: false`，汇总会单独统计 `low_confidence_pairs`。

### 映射表版本化

每次真正重建裁决结果时，映射表版本和映射序号连续加一。当前表与历史归档均包含：

- `version`、`sequence`
- 旧标签别名表指纹
- 裁决配置指纹
- 输入摘要
- 映射摘要（对映射核心的 SHA-256）
- 完整已裁决映射、候选索引与诊断

发布在真实文件锁内完成：归档文件 → 原子 `current.json` → 原子 ledger → state anchor。读取时同时校验版本/序号、自摘要、ledger、归档逐字节一致性和外部 anchor。版本回退返回 `E_STALE_MAPPING` 并分类为 `rollback`；指纹变化、摘要不符、归档替换或 anchor 不一致分类为 `tampered`；只有 `latest+1` 的连续推进被接受。失败不会覆盖旧表和历史版本。

### 检查点、中断与恢复

阶段对象在 `state/objects/<sha256>.json` 内容寻址存储。每个阶段完成后写入：

- 阶段名与分片名
- 分片内容指纹
- 治理配置版本与各阶段指纹
- 映射表版本
- 全局单调序号与前一序号
- 输出对象摘要和阶段输入指纹
- 前置阶段清单

检查点文件、对象文件和 `state/head.json` 都采用“临时文件 + fsync + `os.replace`”原子写；任意时点中断不会产生半写记录。发布检查点使用 POSIX `flock` / Windows `msvcrt.locking`，并校验序号连续、阶段顺序、重复发布和前置阶段；对应错误为 `E_STALE_CHECKPOINT` 或 `E_INVALID_STAGE`。

parse/align 尚不消费映射表，其检查点记录 `mapping_table_version: 0`；adjudicate/summarize 记录本批实际发布或复用的映射表版本。

优雅停止只在完整句子边界检查 `state/STOP` 或 `--stop-after-sentences`。停止报告不承诺强制终止后的状态；恢复从最后一个完整阶段检查点继续。实测恢复后的迁移、对齐、映射、汇总、manifest 与 fixed digest 和全量运行逐字节一致。

### 增量失效传播

阶段输入摘要决定最早失效点：

| 变化 | 最早失效阶段 |
| --- | --- |
| 分片 token/标签内容 | 对应分片 parse |
| 别名表、候选规则、v1 规则 | 全部 parse |
| 对齐容差/比较规则 | 全部 align |
| 裁决优先级、可靠度先验、置信阈值 | 全部 adjudicate |
| 映射摘要/版本或下游摘要 | summarize |

该阶段及全部下游检查点被清除并重建，未受影响上游对象继续复用。`status` 同时展示 `reasons`、`invalidated_products` 与 `reused_products`。失效期间会写入 `mapping/STALE.json` 和 `output/STALE.json`；旧映射查询返回 `E_STALE_MAPPING`，只有显式 `--allow-stale` 可做取证式查看。

## CLI

Linux/WSL：

```bash
python -m posalign run \
  --workspace runtime/workspace \
  --config samples/governance.yaml \
  --shards 'samples/shard_*.jsonl' \
  --reset

python -m posalign status --workspace runtime/workspace
python -m posalign state --workspace runtime/workspace
python -m posalign resolved --workspace runtime/workspace
python -m posalign candidates --workspace runtime/workspace --label '名'
```

Windows PowerShell（引号内通配符由 CLI 自己展开）：

```powershell
.\.venv\Scripts\python.exe -m posalign run --workspace runtime\workspace --config samples\governance.yaml --shards "samples\shard_*.jsonl" --reset
.\.venv\Scripts\python.exe -m posalign status --workspace runtime\workspace
.\.venv\Scripts\python.exe -m posalign candidates --workspace runtime\workspace --label "名"
```

优雅停止与恢复：

```bash
python -m posalign run ... --stop-after-sentences 5
python -m posalign run ... --clear-stop
```

也可以从另一个进程执行 `python -m posalign stop --workspace runtime/workspace`。

v1 反向导出：

```bash
python -m posalign export-v1 \
  --config samples/governance.yaml \
  --shards samples/shard_02_v1.jsonl \
  --output runtime/v1_roundtrip.jsonl
```

对齐失败注入：

```bash
python -m posalign run \
  --workspace runtime/failure \
  --config samples/governance.yaml \
  --shards samples/shard_01_v2.jsonl samples/injected_alignment_failed.jsonl \
  --reset
```

预期返回 `E_ALIGNMENT_FAILED`，并写出 `output/alignment_failure.json`；不会写出新的 align 检查点。

## 本地 HTTP API

```bash
python -m posalign serve --workspace runtime/workspace --host 127.0.0.1 --port 8765
```

仅面向本机，不启动外部服务。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/status` | 检查点、失效原因、复用清单、映射和输出状态 |
| GET | `/state` | `/status` 的别名 |
| GET | `/mapping/resolved` | 已裁决映射 |
| GET | `/mapping/candidates?label=名` | 保留的候选与落选依据 |
| GET | `/migration` | 全量迁移候选、来源与诊断 |
| GET | `/summary` | 最新完整汇总；失效时返回 `E_STALE_MAPPING` |
| POST | `/run` | 启动/恢复批次 |
| POST | `/stop` | 请求优雅停止 |
| POST | `/clear-stop` | 清除停止请求 |
| POST | `/export-v1` | 反向导出 v1 |

PowerShell 示例：

```powershell
Invoke-RestMethod http://127.0.0.1:8765/status
Invoke-RestMethod "http://127.0.0.1:8765/mapping/candidates?label=%E5%90%8D"
$body = @{
  shards = @("samples/shard_01_v2.jsonl", "samples/shard_02_v1.jsonl", "samples/shard_03_v2.jsonl")
  config = "samples/governance.yaml"
  reset = $true
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8765/run -ContentType "application/json" -Body $body
```

## 输出

完整运行在 `output/` 原子生成：

- `migration.json`：标签规范化、候选、来源、缺失/多候选诊断、v1 单列。
- `alignment.json`：逐句标签对、编辑操作、插入/删除/错位位置与统计。
- `mapping_candidates.json`：全部候选、票数、可靠度、分数分解、落选理由。
- `mapping_resolved.json`：唯一映射、置信度、接受状态与仲裁摘要。
- `summary.json`：全局汇总；v1 未切分句在 `v1.unsegmented_sentences` 单列。
- `stats.json`：扁平统计。
- `manifest.json` 与 `fixed_digest.txt`：业务产物固定摘要与逐文件 SHA-256。

当前样本与治理配置的固定摘要是：

```text
35893ac713ac4dd3fa8299619cbc3a35880367cdb2f064e0849a03d6776332f3
```

该值由 `samples/expected_fixed_digest.txt` 锁定，并由测试和演示脚本校验。

## 演示与验证

```bash
python scripts/demo.py
```

演示包含：

- 3 个中英混排正常分片，含 v1/v2、自由字符串、大小写/空白/NFKC 变体与自定义扩展标签。
- 13 句完整运行，固定摘要校验。
- 第 5 句后优雅停止，再恢复；8 个输出文件与全量运行逐字节一致。
- v1 导入统计与严格 v1 JSONL 反向导出，原始 3 条记录语义完全往返。
- `E_ALIGNMENT_FAILED` 注入与零新增对齐检查点。
- 单分片变化只重建该分片 parse/align，并全局重裁决；阈值变化复用全部 parse/align。
- 失效期间旧映射查询被 `E_STALE_MAPPING` 阻断。
- 映射表回退识别为 `E_STALE_MAPPING / rollback`。
- 本地 HTTP 健康、状态、候选与汇总查询成功。

实际报告写入 `runtime/demo/DEMO_REPORT.json`，交付副本见 `artifacts/validation_report.json`。

测试：

```bash
python -m pytest -q
```

本次实测：`33 passed`。覆盖候选与诊断、v1 换算/导出、LCS 与失败（含显式空 `new_tokens`）、裁决并列、检查点并发文件锁/序号/前置阶段、原子写失败、映射回退/摘要/指纹/历史归档/整目录替换篡改、全量与恢复一致性、后期阶段停止边界、增量失效、stale 阻断和 HTTP。

## 稳定错误码

| 错误码 | 含义 |
| --- | --- |
| `E_ALIGNMENT_FAILED` | 配对失败或对齐率低于容差；不写部分对齐结果 |
| `E_STALE_MAPPING` | 映射表回退/篡改/失效，或试图跃迁版本序号 |
| `E_STALE_CHECKPOINT` | 检查点序号落后、重复、非连续或跨阶段倒退 |
| `E_INVALID_STAGE` | 缺失前置阶段、对象或映射版本 |
| `E_INVALID_INPUT` | 文件、路径、参数或跨分片 sent_id 冲突 |
| `E_INVALID_CONFIG` | 治理 YAML 或策略配置非法 |
| `E_INVALID_RECORD` | JSONL 记录结构、UTF-8 或字段长度非法 |
| `E_INTERNAL` | 未预期内部错误 |

