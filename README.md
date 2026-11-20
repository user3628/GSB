# 本地 REE 格式互操作与版本兼容管线

这是一个完全本地、确定性的关系与事件抽取（Relation and Event Extraction, REE）实验管线。它不依赖真实模型、GPU、网络或外部数据集，使用种子标注的局部计数、平滑概率和规则约束完成实体、关系与事件论元抽取，并提供 BIO、JSON 区间、旧版扁平结构三种标注格式的双向转换。

## 快速开始

要求 Python 3.11。仓库已经包含生成后的确定性样本，仍建议按以下顺序验证：

```bash
# 安装 Python 3.11 与虚拟环境（本仓库实际使用 uv）
uv python install 3.11
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt

# 生成 4 个普通文档分片、4 个种子标注分片、1 个 v1 旧版分片
.venv/bin/python scripts/generate_samples.py

# 完整确定性演示
.venv/bin/python -m ree demo

# 自动化测试
.venv/bin/pytest -q
```

若不使用 `uv`，也可以用系统 `python3.11 -m venv .venv` 和 `pip install -r requirements.txt`。

## 目录结构

```text
ree/
  models.py        统一中间表示、规范化、核心字段摘要
  config.py        YAML 配置模型与阶段相关配置子摘要
  alignment.py     字符偏移到 token 下标的可配置对齐
  formats.py       BIO / JSON 区间 / LEGACY_FLAT_V1 编解码
  estimator.py     局部计数、转移计数、平滑概率
  extractor.py     约束感知实体、关系、事件抽取
  validation.py    两条跨格式往返链
  storage.py       文件锁、原子写、内容寻址产物、连续检查点
  pipeline.py      四阶段执行、版本失效、恢复、错误协议
  api.py           FastAPI HTTP 接口
  cli.py           argparse 命令行接口
  demo.py          确定性演示与固定摘要
  sample_data.py   固定随机种子样本生成器
config/extraction.yaml
data/documents/shard_01.jsonl ... shard_04.jsonl
data/seeds/seed_01.jsonl ... seed_04.jsonl
data/legacy/legacy_v1.jsonl
artifacts/expected_demo_summary.json
artifacts/demo_summary.json
tests/
```

所有输入、配置、输出路径均相对于工作区根目录。运行时状态位于 `.ree/`，属于可重建内容。

## 数据与配置

样本生成器固定随机种子 `20261121`，共生成 10 篇文档：8 篇当前结构、2 篇结构版本 1。样本包含中英混排、`New York` / `New York University` 等重叠实体，以及 `held talks` / `talks` 等重叠触发词事件。`L002` 还包含一个故意错位的字符区间和一个重名实体，用于验证对齐失败与名称消歧诊断。

文档分片记录：

```json
{"doc_id":"d001","tokens":["Alice","Chen","met","..."],"text":"...","source":"local_seed","annotation_version":2}
```

种子标注使用当前 JSON 区间结构；旧版分片在文档记录中内嵌 `annotation_version: 1` 的 `annotation`。

`config/extraction.yaml` 定义实体类型、关系头尾类型定义域、事件论元角色类型定义域、触发词表、特征开关、平滑强度、置信阈值、旧版对齐模式和名称消歧规则。

## 统一中间表示

每篇文档维护：

- 实体：`id`、`type`、闭开 token 区间 `[start, end)`
- 关系：`id`、`type`、头实体 `head`、尾实体 `tail`
- 事件：`id`、`type`、触发词 token 区间、`arguments` 中角色到实体 ID 列表的映射

所有列表按固定几何规则排序；JSON 使用 `sort_keys=True`、固定分隔符和 UTF-8，确保同一输入的字节级确定性。

### BIO

BIO 载荷包含标准 `B-`、`I-`、`E-`、`S-` 前缀与类型后缀。为同时表达重叠实体，编码器把实体区间做确定性分层，每层是一个普通 BIO 序列：

```json
{
  "format": "BIO",
  "tags": ["B-ORG", "E-ORG"],
  "bio_layers": [{"name": "layer_1", "tags": ["B-ORG", "E-ORG"]}],
  "companion": {"entities": [], "relations": [], "events": []}
}
```

事件与关系在 `companion` 中承载。解码时同时校验 BIO 标签与 companion 实体，若不一致返回 `E_BIO_COMPANION_MISMATCH`。

### 旧版升级

旧版实体使用字符偏移，关系使用实体名称字符串，事件论元为单值。导入时执行：

- 字符偏移按 `legacy.alignment.mode` 对齐到 token：`strict` 或 `expand`
- 重名实体按 `legacy.name_disambiguation` 选择：`first` 或 `longest`
- 单值论元规范化为列表
- 写入 `source_annotation_version: 1` 和升级项 provenance

无法对齐的实体不会伪造 token 区间，而是保留 `E_ALIGN_FAIL` 诊断，包含 `doc_id`、`offset`、`char_start`、`char_end`。重名引用产生 `E_AMBIGUOUS_NAME` 诊断。

## 抽取算法

1. 从种子标注统计实体类型、实体表面、关系类型、关系表面、关系上下文词、事件类型、角色类型、BIO 标签转移计数。
2. 概率使用加性平滑；分母显式保留一个 `<UNK>` 桶，因此未知或零计数标签不会得到零概率。
3. 实体候选按种子表面词表生成，在对数域叠加边界转移得分。
4. 关系候选只在配置允许的头尾实体类型内产生，并要求观察到的关系表面或关系上下文证据。
5. 事件由配置触发词表生成；每个论元角色只接受该事件类型允许的实体类型，并按类型概率和距离排序。
6. 所有非法关系类型组合和论元类型组合在输出前硬过滤，最终结果中不会出现。

## 四阶段与检查点

阶段固定为：

```text
parse -> estimate -> extract -> validate
```

每个阶段产物存储在内容寻址文件 `.ree/artifacts/<stage>/<sha256>.json`。检查点单独存储为 `.ree/checkpoints/00000001-parse.json` 等文件，包含：

- 单调连续序号 `seq`
- 阶段名
- 阶段输入指纹
- 完整配置版本摘要
- 产物摘要与相对路径
- 阶段输入各组成部分摘要

检查点与配置、输入清单、停止请求都使用临时文件、`fsync` 和 `os.replace` 原子发布，不会留下半写 JSON 记录。检查点发布在跨进程 `fcntl` 文件锁内比较序号；落后、重复、跳跃或缺少前置阶段的发布返回 `E_STALE_CHECKPOINT`。

停止请求在文档边界检查，当前不可拆分文档完成后退出，不发布未完成阶段检查点。恢复后复用已完成的 fresh 上游阶段，最终抽取与校验产物和一次完整运行逐字节一致。

## 版本指纹与失效传播

阶段指纹只包含该阶段真实依赖：

- `parse`：文档分片、种子标注、旧版分片、对齐/消歧配置、支持版本
- `estimate`：parse 产物摘要、类型表、平滑强度
- `extract`：estimate 产物摘要、类型表、触发词、特征开关、置信阈值、抽取参数
- `validate`：extract 产物摘要、兼容版本与往返格式集合

状态查询会比较最新检查点指纹和当前期望指纹，并从最早受影响阶段向下游传播失效。失效记录包含原因、受影响阶段、变化组件、新旧指纹和当时的最后检查点序号。上游阶段在条件匹配时继续复用；下游旧产物不会被当成新结果返回。

## CLI

```bash
# 查看阶段状态、指纹和失效历史
.venv/bin/python -m ree status
.venv/bin/python -m ree fingerprints
.venv/bin/python -m ree reset

# 逐阶段执行
.venv/bin/python -m ree run-stage parse
.venv/bin/python -m ree run-stage estimate

# 完整运行、阶段间停止、恢复
.venv/bin/python -m ree run --stop-after-stage estimate
.venv/bin/python -m ree resume

# 查询与导出
.venv/bin/python -m ree results --doc-id L001
.venv/bin/python -m ree diagnostics
.venv/bin/python -m ree export --output artifacts/unified_ir.json

# 格式转换
.venv/bin/python -m ree convert --doc-id d002 --from-format BIO --to-format JSON

# 版本错误注入
.venv/bin/python -m ree compat-check --schema-version 99

# 导入额外分片
.venv/bin/python -m ree import documents data/documents/shard_01.jsonl
```

## HTTP

```bash
.venv/bin/uvicorn ree.api:app --host 127.0.0.1 --port 8000
```

主要接口：

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/import` | 导入 `documents`、`seeds` 或 `legacy` JSONL |
| `POST` | `/run` | 同步执行所有失效阶段，可传 `stop_after_stage` |
| `POST` | `/resume` | 清除停止请求并恢复 |
| `POST` | `/stop` | 请求在当前文档后优雅停止 |
| `POST` | `/run-stage/{stage}` | 执行指定阶段 |
| `GET` | `/status` | 阶段状态、指纹、失效历史 |
| `GET` | `/fingerprints` | 精简指纹视图 |
| `GET` | `/results?doc_id=...` | 查询抽取 IR |
| `GET` | `/artifacts/{stage}` | 查询 fresh 阶段产物 |
| `GET` | `/diagnostics` | 对齐、往返和升级诊断 |
| `POST` | `/convert` | 指定文档三种格式互转 |
| `POST` | `/roundtrip` | 查询单文档两条往返链结果 |
| `GET` | `/export` | 导出统一 IR |
| `POST` | `/compat/check` | 检查声明结构版本并执行兼容解码 |

业务错误以 HTTP 409 返回：

```json
{"error":{"code":"E_STALE_ARTIFACT","stage":"extract","message":"artifact for stage extract is stale: ..."}}
```

## 稳定错误码

| 错误码 | 含义 |
|---|---|
| `E_INVALID_STAGE` | 阶段名非法 |
| `E_MISSING_ARTIFACT` | 阶段产物或前置阶段产物缺失 |
| `E_STALE_ARTIFACT` | 阶段产物已被版本变化标记失效 |
| `E_STALE_CHECKPOINT` | 检查点落后、重复、跳跃、跨阶段错位或并发发布失败 |
| `E_UNSUPPORTED_VERSION` | 声明结构版本高于当前支持版本，或没有兼容路径 |
| `E_ALIGN_FAIL` | 旧版字符区间无法按配置对齐到 token |
| `E_AMBIGUOUS_NAME` | 旧版名称引用匹配到多个实体 |
| `E_BIO_INVALID` | BIO 标签结构非法 |
| `E_BIO_COMPANION_MISMATCH` | BIO 标签与伴随实体记录不一致 |

错误对象始终包含 `code`、`stage` 和可读 `message`；可定位错误还包含 `doc_id` 与 `offset`。

## 实际验证结果

在 Python `3.11.18`、`.venv` 中执行的结果：

```text
$ .venv/bin/pytest -q
16 passed
```

确定性演示结果：

- 文档：10 篇；普通文档分片 4 个；旧版分片 1 个
- 抽取：46 个实体、64 个关系、19 个事件、70 个事件论元
- 约束违规：0
- 往返链：20/20 通过，失败 0
- 旧版升级：L001、L002 均标记来源版本 1，升级项包含字符偏移到 token、名称到 ID、单值到列表
- 对齐诊断：L002 产生 1 个 `E_ALIGN_FAIL`（`offset: 1`）和 3 个 `E_AMBIGUOUS_NAME`
- 停止恢复：停止于 `estimate` 后检查点序列 `[1, 2]`，恢复后连续为 `[1, 2, 3, 4]`
- 平滑强度变化：最早失效阶段为 `estimate`，传播到 `estimate/extract/validate`
- 错误注入：`E_UNSUPPORTED_VERSION`、`E_MISSING_ARTIFACT`、`E_STALE_ARTIFACT`、`E_STALE_CHECKPOINT` 均实际触发

固定摘要：

```text
run_sha256        a5666e8a97d36245729fbf8b9416111350497115ecb8a8591b68d03cb0021e0d
extraction_sha256 a5fd8f14c563b29d92552143fdb692c294333ac1e985b64dc4588c5ad3eabc8d
validation_sha256 f887bf32c3c7ee9e7aeae17c30e87d13c3fc271eb44c1097a356c61bf62482b7
config_version    21fcde7d5c306692f3f1eb613d797d13f743a3e4093ea29c5eeb5292bf4762ea
```

基准保存在 `artifacts/expected_demo_summary.json`。`python -m ree demo` 输出中的 `fixed_digest_match` 应为 `true`。

HTTP 端到端冒烟验证也已执行：`/health`、`/status`、`/results`、`/convert`、`/diagnostics`、`/run-stage`、`/stop`、`/resume` 返回 200；声明版本 99 的 `/compat/check` 返回 HTTP 409 与 `E_UNSUPPORTED_VERSION`。若希望在已有运行状态上重新观察停止/恢复，请先执行 `python -m ree reset`。

## 说明

- 运行时不需要网络；样本与摘要已固定在仓库中。
- BIO 使用多图层扩展表达重叠实体，每一层本身仍是合法的 B/I/E/S BIO 序列。
- 抽取核心使用纯 Python 确定性排序和浮点计算；`numpy` 按任务环境要求安装，但不参与影响摘要的随机过程。
