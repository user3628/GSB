# 多路召回自适应融合实验台

这是一个完全本地、确定性的搜索/推荐召回实验台。它从固定随机种子生成三档 JSONL 样本，执行词法倒排、类目协同、向量近邻三路召回，按全局候选预算与单路配额自动降档，再通过加权求和或 RRF 融合、去重和截断输出最终排序。系统同时提供原子检查点、版本失效传播、并发发布保护、优雅停止/恢复、CLI 与本地 HTTP API。

运行环境已锁定为 Python 3.11 + NumPy 1.26.4 + PyYAML 6.0.2；不使用 Docker、在线服务或 GPU。

## 快速开始

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 生成固定种子样本（仓库内已经生成了一份 data/）
.venv/bin/python -m recall_lab generate --out data --seed 20261120

# 导入清单与默认配置
.venv/bin/python -m recall_lab --workspace .lab init \
  --manifest data/manifest.jsonl --config configs/fusion.yaml --force

# 一次跑完三档的召回、融合、汇总并导出摘要
.venv/bin/python -m recall_lab --workspace .lab run --scales all --until summary

# 查看状态、预算取舍理由、版本指纹、融合明细
.venv/bin/python -m recall_lab --workspace .lab status
.venv/bin/python -m recall_lab --workspace .lab decision --scale large
.venv/bin/python -m recall_lab --workspace .lab version --scale small
.venv/bin/python -m recall_lab --workspace .lab fusion-detail --scale small --query-id q-small-0000

# 本地 HTTP 服务
.venv/bin/python -m recall_lab --workspace .lab serve --host 127.0.0.1 --port 8765
```

聚合摘要写入工作区 `exports/experiment-summary.json`。当前仓库还保留了本次实际验证的 `artifacts/experiment-summary.json` 与 `artifacts/verification-report.json`。

也可以使用顶层包装器与 Make 目标：`./recall-lab --workspace .lab status`、`make init`、`make run`、`make test`、`make demo`。

## 目录结构

```text
recall_lab/
  config.py        YAML 校验、规范化、完整配置指纹
  data.py          Manifest/JSONL 加载与输入指纹
  generate.py      固定种子三档样本与参考标注生成
  budget.py        全局预算、单路配额、自动降档与淘汰理由
  recall.py        倒排/类目协同/余弦向量三路召回
  fusion.py        weighted_sum/RRF、去重、来源贡献明细
  summary.py       命中、P@K、候选覆盖、平均排名偏差
  checkpoint.py    原子检查点、历史链、文件锁、序号发布
  engine.py        阶段编排、版本失效、停止恢复、摘要导出
  workspace.py     样本/清单/配置导入
  cli.py / http_api.py
configs/           默认加权、RRF、紧预算三份配置
data/              固定种子生成的小/中/大三档 JSONL 与 manifest
tests/             12 项端到端与边界测试
scripts/demo.py    三档、预算注入、权重失效、停止恢复演示
```

## 输入格式

`data/manifest.jsonl` 每行声明一个规模档：

```json
{"scale":"small","items_file":"small/items.jsonl","queries_file":"small/queries.jsonl","item_count":200,"query_count":12,"expected_candidate_depth":240,"expected_fused_depth":10,"seed":20261120}
```

物品行包含 `item_id`、`category`、`title` 和 8 维 `features`。查询行包含 `query_id`、`query_text`、`locale`、`channel`、`history_categories`、`query_features` 与 `reference_item_ids`。参考标注只用于汇总计算，不参与召回或融合。样本中包含中英混排查询，并天然产生跨路重复候选。

默认三档规模为小档 200 物品/12 查询、中档 2000/24、大档 12000/48。清单中声明的数量会与实际 JSONL 行数交叉校验。

## 召回与预算决策

三路召回都返回 `rank`、`score` 和公式级 `detail`，并严格按 `(-score, item_id)` 排序：

- **词法倒排**：NFKC 归一化后切分英文词与中文字符，按匹配词项的 IDF 之和打分，候选明细含每个匹配词项及 IDF。
- **类目协同**：用全部查询的历史类目频次形成类目先验，按查询历史位置权重乘以 `log1p(类目频次)` 打分，并附物品热度因子。
- **向量近邻**：查询特征与物品隐因子的余弦相似度，明细记录点积、两侧范数和公式。

若查询文本为空或物品库为空，阶段失败并返回 `E_EMPTY_INPUT`，`details.query_id` 给出查询标识；任何非有限特征或分数返回 `E_NUMERIC`，不会把异常结果包装成成功。

### 预算降档规则

1. 每路先取 `min(配置深度, 物品数, floor(单路配额 / 查询数))`。
2. 按配置优先级与 `minimum_depth` 选择可容纳的最多路数；若更少优先级的路无法满足最小深度，则淘汰该路并记录 `DISABLED_GLOBAL_BUDGET`。
3. 剩余预算以“容量比例 + 最大余数 + 固定优先级”分配为整数深度，不使用并行归约或非确定顺序。
4. 每路计划评估数为 `effective_depth × query_count`，不得超过其单路配额；三路总和不得超过全局预算。融合截断位也会记录 `CONFIGURED_TOP_K` 或 `BOUNDED_BY_UNION_CANDIDATES` 理由。

默认 5000 预算下，实际决策为：

| 档位 | 启用路 | 深度（词法/类目/向量） | 使用预算 |
|---|---|---|---:|
| small | 三路 | 100 / 80 / 60 | 2880 |
| medium | 三路 | 84 / 69 / 55 | 4992 |
| large | 词法 + 类目 | 54 / 50 / 0 | 4992 |

`configs/budget-tight.yaml` 将全局预算降为 3000，此时大档自动只保留词法路，深度 62、使用 2976；这是预算注入演示。

## 融合、去重与指标

同一 `item_id` 只保留一条融合结果。每条结果记录 `source_count`、`best_rank`、`fused_score`，以及各路 `source/rank/score/normalized_score/weight/contribution`。贡献按固定路序 `lexical → category → vector` 收集，并用 `math.fsum` 求和，避免并行归约带来的浮点顺序差异。

- `weighted_sum`：各路分数先在该查询内做 min-max 归一化，再乘权重。
- `rrf`：`weight / (rrf_k + rank)`。
- 最终排序固定为 `(-fused_score, best_rank, item_id)`，再按自适应截断位截断。

权重、策略或截断位变化会改变完整配置指纹，并使融合及下游失效；词法/类目/向量召回参数或预算变化则从召回阶段开始失效。

汇总指标包括：每路参考命中数、有命中查询率、候选覆盖率、P@K、Recall@K、有命中查询比例和平均排名偏差。所有指标先量化到固定精度，再生成按配置指纹与各档随机种子绑定的稳定 JSON。

## 检查点、版本与并发

工作区布局：

```text
.lab/
  input/                         导入后的 manifest 与数据
  config/fusion.yaml             导入配置副本
  artifacts/<scale>/<stage>/<sha256>.json   内容寻址产物
  checkpoints/<scale>/state.json             当前档位状态
  checkpoints/<scale>/history/00000001.json  每次发布的完整原子快照
  checkpoints/<scale>/publish.pending.json   发布中断恢复标记
  checkpoints/<scale>/lock                   flock 文件锁
  control/stop.request / stop-report.json
  exports/experiment-summary.json
```

每次发布先在档位文件锁内校验历史链、当前序号、上游指纹、记录摘要与产物摘要，再原子写入 pending 标记、历史快照和当前状态。任意时刻中断最多留下可识别的 pending/孤立内容寻址产物，不会产生半条检查点记录。多个工作进程对同一档位只能有一个连续序号发布成功；并发的重复发布返回 `E_STALE_CHECKPOINT`。

阶段输入指纹依赖如下：

- `recall`：物品内容 + 查询内容 + 三路召回参数 + 预算配置 / 算法版本。
- `fusion`：召回输入指纹 + 召回产物摘要 + 融合策略/权重/去重/截断。
- `summary`：融合输入指纹 + 融合产物摘要 + 随机种子 / 指标版本。

因此只改权重时，召回检查点继续复用，融合与汇总重建；改物品、查询、召回参数或预算时，从召回开始重建。发布还会校验 worker 读取时的基准序号，后到的并发 worker 不能静默覆盖先到结果。检查点篡改会按 `details.reason` 区分，例如 `RECORD_DIGEST_MISMATCH`、`STATE_DIGEST_MISMATCH`、`SEQUENCE_ROLLBACK`、`SCALE_MISMATCH`、`WORKSPACE_REPLACEMENT`、`ARTIFACT_DIGEST_MISMATCH`、`DUPLICATE_PUBLICATION`、`PUBLICATION_BASIS_STALE`，但统一返回稳定业务码 `E_STALE_CHECKPOINT`。

停止请求写入 `control/stop.request`。运行器在每个不可拆分阶段完成后检查并退出，同时写 `stop-report.json`；`resume` 清除停止标记并复用已完成检查点。强制终止不承诺生成退出报告。

## 稳定错误码

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_EMPTY_INPUT` | 400 | 空查询或空物品库，附 `query_id` |
| `E_INVALID_STAGE` | 409 | 非法阶段跳转、缺失/无效前置阶段或产物 |
| `E_STALE_CHECKPOINT` | 409 | 记录替换、序号回退、摘要/产物不匹配、重复发布 |
| `E_NUMERIC` | 422 | 特征或召回/融合分数为 NaN/Inf |
| `E_INVALID_INPUT` | 400 | YAML/JSONL/Manifest/参数非法 |
| `E_NOT_FOUND` | 404 | 档位、查询或路由不存在 |
| `E_BUSY` | 409 | 同一工作区的运行级锁被占用 |

## HTTP API

服务仅监听本地地址，全部响应为 JSON：

| 方法 | 路径 | 功能 |
|---|---|---|
| GET | `/health`、`/ready` | 健康检查/工作区就绪 |
| POST | `/generate`、`/import` | 生成样本、导入清单与配置 |
| POST | `/run`、`/stage` | 触发批量或单阶段执行 |
| POST | `/stop`、`/resume` | 优雅停止、恢复 |
| POST | `/config` | 切换融合配置以触发版本失效 |
| GET | `/status`、`/scales/{scale}/status` | 档位状态、阶段与指纹 |
| GET | `/scales/{scale}/decision` | 预算取舍、理由与淘汰路 |
| GET | `/scales/{scale}/version` | 内容/召回/融合/预算版本指纹 |
| GET | `/scales/{scale}/fusion?query_id=...` | 融合来源、排名、分数与贡献 |
| GET | `/scales/{scale}/artifact/{stage}` | 完整阶段产物 |
| GET | `/export`、`/summary` | 导出/读取聚合实验摘要 |

示例：

```bash
curl -s http://127.0.0.1:8765/scales/large/decision
curl -s -X POST http://127.0.0.1:8765/run \
  -H 'Content-Type: application/json' \
  -d '{"scales":["small","medium","large"],"until":"summary"}'
curl -s -X POST http://127.0.0.1:8765/stop -d '{"reason":"demo"}'
curl -s -X POST http://127.0.0.1:8765/resume -d '{}'
```

## 实际验证结果

验证日期：2026-11-20。使用 `data/` 固定种子 `20261120`，执行 `scripts/demo.py` 得到：

- 三档完整运行成功，共 9 个阶段产物；聚合摘要 SHA-256 为 `0e4f0582f5b9d6e6332b700e5fcd29bfed5a6a850116013fee561c871c38d616`。
- 小/中/大档 P@10 分别为 `0.5083333333`、`0.3291666667`、`0.0354166667`；候选覆盖率分别为 `0.965`、`0.8015`、`0.172`。
- 大档在默认预算下淘汰向量路；紧预算注入下只保留词法路。
- 小档融合前跨路重复贡献合并数为 `687`，融合后所有查询均无重复 `item_id`。
- 将词法权重从 `0.75` 改为 `2.5`、类目权重改为 `0.1` 后，最早失效阶段为 `fusion`，召回序号保持 `1`，融合序号 `2 → 4`，12/12 个小档查询的 Top-10 排序发生变化。
- 在 `medium:fusion` 边界优雅停止后恢复，最终摘要与从零完整运行逐字节相同，均为上述 `0e4f…d616`。
- HTTP 实际启动后用 `curl` 验证 `/health`、三档 `/status`、`/decision`、融合明细和 `/export` 均成功；融合明细返回 96 个唯一候选、66 次重复来源合并、10 条截断结果。
- `python -m unittest discover -s tests -v`：12 项测试全部通过，覆盖空输入、NaN、预算/配额、数据/预算/权重失效、去重、RRF、摘要稳定性、检查点篡改/回退、跨档替换、并发发布和 HTTP。

可重复执行：

```bash
.venv/bin/python scripts/demo.py
.venv/bin/python -m unittest discover -s tests -v
```

确定性的边界说明：摘要不包含时间戳、工作区路径或运行序号；相同数据、配置、种子和 Python/NumPy 环境下，重复运行及跨工作区运行应得到逐字节一致的导出摘要。
