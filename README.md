# SumEngine：本地确定性文本摘要与改写引擎

SumEngine 从零实现了一个可在 Windows/Linux/macOS 本地运行的新闻式长文档摘要引擎。系统并行生成三路候选：

- 抽取式：句子打分 + 最大边际相关（MMR）选择。
- 压缩式：规则化删减修饰/从属片段 + 可配置同义短语替换。
- 模板式：抽取标题、正文要点、时间/地点槽位后按模板拼装。

三路候选在对数域按要点覆盖、长度贴合、语言一致性和来源先验融合，最终按确定性排序规则裁决。全部算法、样本、预算、检查点和 HTTP/CLI 接口均在本地完成，不使用生成模型、GPU、外部服务、Docker 或在线样本。

## 快速开始

需要 Python 3.11 或更高版本。建议使用虚拟环境：

```powershell
# Windows PowerShell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m sumengine.cli init
python -m sumengine.cli run --mode parallel --out run.json
python -m sumengine.cli export
```

```bash
# Linux / macOS / WSL
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m sumengine.cli init
python -m sumengine.cli run --mode parallel --out run.json
python -m sumengine.cli export
```

`init` 会检查并创建 `samples/`、`state/`、`summaries/` 和 `exports/`，并用固定种子 `20261122` 生成 5 条文档分片与 YAML 配置。工作区必须可写；启动时会做写入探针检查。

也可以从源码安装命令入口：

```bash
python -m pip install -e .
sumengine init
sumengine run --mode serial
```

## 演示与验证

完整演示会建立互相隔离的工作区，执行并行全量、串行全量、停止后恢复、编辑后增量重建和错误码检查：

```bash
python -m sumengine.cli demo
# 或
python scripts/run_demo.py
```

报告写入 `artifacts/demo_report.json`，摘要集位于 `artifacts/demo/*/exports/final_summaries.json`。当前实现已验证：

- 并行与串行运行的导出摘要文件逐字节一致。
- 串行处理 2 个文档后收到停止请求，完成在途单元后退出；恢复后的导出与完整运行逐字节一致。
- 人工编辑版本从 1 单调递增，修改输入/配置后编辑文本保留且被标记为 `needs_review: true`。
- 替换表变化时复用 `extract`/`template`，只重建 `compress`、`fusion`、`final`。
- 至少一条超长样本因全局评估预算被记录为未处理，给出 `E_GLOBAL_BUDGET` 和预估消耗。
- `E_INPUT_TOO_SHORT`、`E_LATENCY_BUDGET`、`E_STALE_CHECKPOINT` 均返回结构化错误对象。

运行全部测试：

```bash
python scripts/run_tests.py
# 或
python -m unittest discover -s tests -t . -v
```

实际验证环境：WSL2/Linux、Python 3.14.4、NumPy 2.5.5。源码仅使用 Python 3.11 起可用的语法和标准库接口；进程池显式使用 `spawn` 启动方式，以覆盖 Windows 语义。

最近一次本地验证（2026-11-22）：

- `python scripts/run_tests.py`：27 项测试全部通过。
- `python -m sumengine.cli demo`：12 项检查全部为 `true`。
- 并行、串行和停止后恢复的导出摘要摘要值均为 `129e70107405ff1a9fc8783867cfef57413d6c2ffa186ae7e6bd0d3f0122e2b7`。
- 超大分片 `news-zh-oversize` 被记录为未处理：`E_GLOBAL_BUDGET`，预估 279 次候选句评估。

## 样本

`samples/documents.jsonl` 中每行包含：

```json
{"doc_id":"...","title":"...","body":"...","language":"zh|en|mixed","source":"wire|desk|social","target_length":"short|medium|long"}
```

5 条样本包括中文长文、英文中等长度文、中英混排短文、中文中等长度文，以及一条会超过预算的超长分片。`samples/config.yaml` 配置目标长度档、评估预算、工作进程配额、延迟预算、MMR 参数、压缩规则、同义短语表、模板和融合权重。

配置读取器是内置的受限 YAML 子集解析器，不依赖 PyYAML。支持块级映射/列表、引号字符串、数字、布尔值、`null` 和行内 JSON 集合。

## CLI

```text
python -m sumengine.cli init [--force]
python -m sumengine.cli run [--mode serial|parallel] [--stop-after N] [--full] [--out FILE]
python -m sumengine.cli resume [--mode serial|parallel] [--stop-after N] [--full]
python -m sumengine.cli status [--full]
python -m sumengine.cli budget
python -m sumengine.cli candidates --doc-id DOC_ID
python -m sumengine.cli edit --doc-id DOC_ID --text TEXT
python -m sumengine.cli edit --doc-id DOC_ID --text-file FILE
python -m sumengine.cli stop [--reason REASON]
python -m sumengine.cli export [--print]
python -m sumengine.cli import-docs --file documents.jsonl
python -m sumengine.cli config --file config.yaml
python -m sumengine.cli serve [--host 127.0.0.1] [--port 8765]
python -m sumengine.cli demo
```

全局参数 `--workspace`、`--docs`、`--config` 可指定其他工作区或输入文件。`run --stop-after N` 用于确定性演示：每完成一个不可拆分文档单元后检查停止标记，达到 N 后不再提交新文档；已在进程池中的文档单元完成后再退出。

如果工作区已存在 `state/stop.request`，普通 `run` 会尊重该停止请求并返回 `stopped`；`resume` 会先清除停止标记再继续。

## HTTP API

启动本地服务：

```bash
python -m sumengine.cli serve --port 8765
```

主要端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查与工作区信息 |
| GET | `/status` | 各文档阶段状态、版本指纹、缓存/失效阶段 |
| GET | `/budget` | 最近一次运行的预算消耗、未处理原因与延迟信息 |
| GET | `/candidates?doc_id=...` | 三路候选、句子分数、融合明细和淘汰原因 |
| POST | `/run` | 异步运行，返回 `run_id`；JSON 可选 `mode`、`stop_after_docs` |
| GET | `/runs/{run_id}` | 查询异步运行状态和结果 |
| POST | `/stop` | 写入优雅停止请求 |
| POST | `/resume` | 清除停止请求并异步恢复 |
| POST | `/edit` | 提交 `{"doc_id":"...","text":"..."}` |
| GET | `/export` | 返回当前可导出的摘要集 |
| POST | `/export` | 原子写入导出文件并返回摘要/路径 |
| POST | `/import` | 提交 `{"documents":[...]}` 或本地 JSONL 的 `{"path":"..."}` |
| PUT | `/config` | 提交配置对象，或 `{"yaml":"..."}` |

所有响应为 UTF-8 JSON；预期错误返回稳定的 `error.code`。服务基于标准库 `http.server.ThreadingHTTPServer`，停止请求可在运行期间由另一请求线程写入。

示例：

```bash
curl -s -X POST http://127.0.0.1:8765/run \
  -H 'Content-Type: application/json' -d '{"mode":"parallel"}'
curl -s http://127.0.0.1:8765/runs/run-1
curl -s http://127.0.0.1:8765/candidates?doc_id=news-zh-002
curl -s -X POST http://127.0.0.1:8765/edit \
  -H 'Content-Type: application/json' \
  -d '{"doc_id":"news-zh-002","text":"人工复核后的摘要"}'
```

## 算法与确定性

### 句子与抽取

1. 扫描原文，在中英文句末标点处切分，记录原文 `[start, end)`；句间空白不属于句子。
2. 对句子做 NFKC 与小写化，英文按词、中文按字形成 token，并过滤停用词。
3. 句子分数由四部分加权：词频、位置衰减、标题重叠、目标长度贴合。所有分量和最终分保留固定小数位。
4. 使用 NumPy 构建句子 token 集合的相似度矩阵，按 `lambda * relevance - (1-lambda) * max_similarity` 做 MMR；选择受目标字符档上限约束，并在需要时越过正常句子数上限以补足最小字符档。
5. 输出每个句子的下标、原文偏移、原文片段、分数与四个分量，以及 MMR 选择序。

空文档或去空白后正文小于 `runtime.min_body_chars` 时抛出带 `doc_id` 的 `E_INPUT_TOO_SHORT`。

### 压缩与模板

压缩路从抽取结果逐句保留原文，按配置正则删除从句/修饰片段。每次删除和替换都先做覆盖度试算；若会低于 `compression.min_coverage` 或超过删除比例上限，则拒绝该操作并记录原因。输出：

- `removed_fragments`：实际删除片段、所属句下标与规则。
- `skipped_removals`：因覆盖度/比例保护未删除的片段。
- `replacements`：同义短语替换依据与次数。
- `fallbacks` 与 `fallback_count`：缺少替换表条目时保留原文的显式记录。
- 原句数、保留句数、每句改写结果和 `silent_sentence_drop: false`。

模板路从标题和正文中抽取 `who`、`what`、`when_where` 槽位，按 `zh`/`en`/`mixed` 模板拼装；持续加入高排名句子直到达到覆盖下限或长度上限。槽位、模板名、覆盖点和未覆盖点均进入产物。

### 融合

融合分在对数域计算：

```text
score = Σ weight_i * ln(feature_i + epsilon)
features = {coverage, length_fit, language, source_prior}
```

长度越界、候选语言与文档标签明显不符、覆盖不足或空候选会保留在结果中，但标记 `available: false` 和具体 `unavailable_reasons`，而不是删除。可用候选按“融合分降序、路由优先级、candidate_id”确定性仲裁；其他候选记录 `LOWER_FUSION_SCORE`、平局仲裁或不可用原因。

## 预算与并行调度

每文档预估消耗：

```text
sentence_count * 3 + fusion_candidate_evaluations
```

调度器先按预估代价升序决定接纳顺序，再依次受以下约束：

- 全局候选句评估次数预算。
- 单逻辑工作进程累计配额；已被接纳的文档分配到负载最低且仍有余量的逻辑进程。
- `max_workers` 并发上限。
- 请求延迟预算：使用固定开销与每次评估预估毫秒数做接纳投影，运行时再用单调时钟硬截止。

被拒绝文档进入 `schedule.unprocessed`，包含错误码、原因、正文长度、句子数、目标档、预估评估次数和预估延迟。实际执行使用 `multiprocessing` 的 `spawn` 进程池，最多启动调度器允许的进程数。主进程按 `doc_id` 排序后归约；汇总/导出不含完成顺序、PID、时间戳或运行模式，因此串行和并行逐字节一致。

运行期间超过硬延迟期限时终止未完成工作并返回 `E_LATENCY_BUDGET`。停止请求只阻止提交新文档；已在途的不可拆分文档单元会完成并持久化。

## 检查点、编辑与增量

工作区状态：

```text
state/checkpoints.json      原子替换的检查点日志
state/products/<doc>.json   每文档各阶段完整产物与指纹
state/edits.json            人工编辑当前值和单调历史
state/stop.request          优雅停止标记
state/last_run.json         最近一次运行预算与未处理原因
state/*.lock                跨进程文件锁
summaries/                  导出镜像
exports/                    权威导出
```

每个重新计算阶段写入一条记录，包含：

```json
{
  "sequence": 1,
  "stage": "fusion",
  "doc_id": "news-zh-002",
  "input_fingerprint": "...",
  "stage_fingerprint": "...",
  "config_version": "...",
  "schedule_fingerprint": "...",
  "edit_version": 0,
  "product_digest": "...",
  "needs_review": false
}
```

日志在同一目录临时文件中写入、`fsync` 后通过 `os.replace` 原子替换；文件锁在 POSIX 使用 `fcntl.flock`，Windows 使用 `msvcrt.locking`。发布时比较单调序号；落后、重复、序号跳跃或 `expected_doc_id` 跨文档错位均返回 `E_STALE_CHECKPOINT`。

阶段指纹构成：

- 文档正文、标题、语言、来源、目标档或预算配置变化：全部内容阶段失效。
- 同义替换表变化：`compress`、`fusion`、`final` 失效，抽取与模板复用。
- 模板变化：`template`、`fusion`、`final` 失效。
- 融合权重或来源先验变化：`fusion`、`final` 失效，三路候选复用。
- 人工编辑：只产生新的 `final` 指纹和编辑版本。

编辑摘要保留原文而不自动覆盖候选。若编辑之后输入或上游配置变化，重建会保留编辑文本，并在终稿和导出中设置 `needs_review: true`、`review_reason: INPUT_OR_CONFIG_CHANGED_SINCE_EDIT`。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_INPUT_TOO_SHORT` | 正文为空或低于最小长度，包含 `doc_id` |
| `E_INPUT_NOT_FOUND` | 分片文件或 `doc_id` 不存在 |
| `E_INVALID_INPUT` | 文档/请求字段不合法 |
| `E_CONFIG` | 配置读取、YAML 语法或配置约束错误 |
| `E_LATENCY_BUDGET` | 接纳投影或实际执行超过延迟预算 |
| `E_GLOBAL_BUDGET` | 文档预估超过/耗尽全局评估预算，作为未处理原因 |
| `E_WORKER_QUOTA` | 文档预估或累计分配超过单工作进程配额 |
| `E_NOT_READY` | 编辑/查询所需的上游阶段尚未生成 |
| `E_NO_AVAILABLE_CANDIDATE` | 三路候选均不可用 |
| `E_STALE_CHECKPOINT` | 检查点落后、重复或跨文档错位 |
| `E_STATE_CORRUPT` | 状态文件结构损坏 |
| `E_WORKER_FAILURE` | 工作进程内的非预期异常，按文档隔离 |
| `E_BUSY` | HTTP 工作区已有运行中的任务 |
| `E_WORKSPACE_NOT_WRITABLE` | 工作区或输出目录不可写 |

## 代码结构

```text
sumengine/
  candidates.py   三路候选、MMR、覆盖度与删除/替换依据
  config.py       受限 YAML 解析、默认值与校验
  demo.py         固定种子端到端演示和报告
  engine.py       调度、串并行执行、缓存、编辑、导出
  fusion.py       对数域融合与确定性裁决
  http_api.py     标准库 HTTP 接口与异步任务
  samples.py      固定种子样本/配置生成器
  scheduler.py    预算接纳、逻辑工作进程分配
  store.py        原子检查点、产品、编辑和文件锁
  text.py         句子切分、token、语言与要点覆盖
  worker.py       进程池入口与单文档阶段流水线
tests/            unittest 单元、集成、HTTP 与并发发布测试
scripts/          样本生成、演示和测试入口
```

所有摘要均由本地确定性规则产生；同一输入、配置、编辑版本和预算下，重复运行的导出文件保持逐字节一致。
