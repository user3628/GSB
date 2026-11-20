# 合同风险条款等级判定工作台（crwbench）

本地、确定性、无外部网络与真实模型依赖的合同条款风险分级工作台。输入 UTF-8 JSONL 条款分片与
YAML 判定配置，按四个阶段产出 0–3 风险等级、策略取舍记录、检查点与可复现摘要。

## 环境准备（本地虚拟环境，非容器）

需要 Python 3.11。本机通过 `uv` 安装独立解释器并创建 `.venv`：

```bash
uv python install 3.11
uv venv --python 3.11 .venv
uv pip install -r requirements.txt
uv pip install -e .
```

验证：`.venv/bin/python --version` → `Python 3.11.x`，仅依赖 NumPy 与 PyYAML。

## 目录结构

- `config/default_config.yaml`：五类别关键词权重、否定/程度规则、阈值、全局预算与单类别配额。
- `config/variants/config_v110.yaml`：规则变更配置，用于增量重算演示。
- `config/reference_diagnostics.json`：本地确定性参考诊断值（可由脚本重建）。
- `src/crwbench/`：四个阶段、检查点、策略、管线、生成器、CLI 与 HTTP 接口。
- `tests/`：20 个 unittest 用例，覆盖业务要求与稳定错误码。
- `scripts/run_demo.py`：端到端确定性演示（含一次规模切换、停止恢复、增量校验、HTTP 冒烟）。
- `data/demo_*/`：固定种子生成的 3 分片 JSONL（中英混排、五类别、空文本边界样本）。
- `work/`：检查点、原子产物与导出（默认 git 忽略，可随时重建）。

## 四阶段流水线

1. 规范化与切分 `normalize.py`：NFKC、全半角统一、空白折叠、分句并保留字符偏移与语言标记；
   空文本/仅标点产出带 `clause_id` 的 `E_EMPTY_TEXT` 诊断且不丢弃。
2. 特征抽取 `features.py`：类别关键词命中（偏移、来源语言、权重）、否定翻转极性、程度修饰强度；
   类别缺关键词表时写入显式 `UNKNOWN_KEYWORD_TABLE` 标记，判定不静默丢弃。
3. 打分与等级 `scoring.py`：对数域 `logsumexp(正向) - logsumexp(否定)` 合成五类分数，
   按可配置阈值映射 0–3；全部低于阈值输出默认低风险并记录依据。
4. 汇总与配比 `strategy.py`：在精确全量、关键词优先截断、类别分档预算三策略间做可解释取舍，
   严格满足全局预算与单类别配额，输出候选打分明细、淘汰原因与等级清单。

## 确定性与逐字节一致

- 所有产物经 `util.canonical_dumps`（排序键、固定分隔符、LF、UTF-8）序列化后原子落盘。
- 检查点记录含阶段名、输入分片摘要、配置版本/指纹、产物摘要与单调序号，自带记录摘要。
- 增量重算按阶段内容摘要指纹识别最早失效阶段，只重算其下游；与干净全量运行逐字节比对，
  不一致返回 `E_INCREMENTAL_MISMATCH` 并附差异摘要。

## 快速开始

```bash
# 生成确定性演示数据（3 分片，固定种子，可重复生成）
.venv/bin/crwbench generate

# 运行四阶段（自动选择策略、写检查点与导出）
.venv/bin/crwbench --workdir work/demo run data/demo/shard_01.jsonl data/demo/shard_02.jsonl data/demo/shard_03.jsonl

# 查看状态、指纹、策略理由、单条等级依据
.venv/bin/crwbench --workdir work/demo status
.venv/bin/crwbench --workdir work/demo fingerprints data/demo/shard_01.jsonl
.venv/bin/crwbench --workdir work/demo strategy
.venv/bin/crwbench --workdir work/demo evidence --clause-id C000001

# 停止 / 恢复 / 导出
.venv/bin/crwbench --workdir work/demo stop
.venv/bin/crwbench --workdir work/demo resume
.venv/bin/crwbench --workdir work/demo export --output work/demo/levels.json

# 增量结果与全量结果逐字节校验
.venv/bin/crwbench verify-incremental --reference work/clean --candidate work/incr

# 本地 HTTP 接口（仅监听 127.0.0.1，端口 0 时由系统分配空闲端口）
.venv/bin/crwbench serve --port 8790
```

## HTTP 接口

`POST /shards/import`、`POST /pipeline/run`、`GET /stages/status`、`GET /stages/fingerprints`、
`GET /strategy/rationale`、`GET /levels/evidence`、`POST /stop`、`POST /resume`、
`GET /levels/export`、`GET /health`。统一返回规范化 JSON，业务错误返回稳定错误码信封。

## 稳定业务错误码

| 错误码 | 触发场景 |
| --- | --- |
| `E_EMPTY_TEXT` | 条款为空或仅标点，保留在诊断输出 |
| `E_BAD_INPUT` | JSONL 非法、缺字段、clause_id 重复 |
| `E_CONFIG` | YAML 配置缺失或标量类型错误 |
| `E_STALE_CHECKPOINT` | 落后/重复/跨阶段错位或非连续序号发布 |
| `E_INVALID_STAGE` | 非法阶段跳转或缺前置产物 |
| `E_STALE_SNAPSHOT` | 序号回退或产物摘要不匹配（整体快照被替换，原产物保留） |
| `E_INCREMENTAL_MISMATCH` | 增量与全量结果逐字节不一致，附差异摘要 |
| `E_BUDGET_EXCEEDED` | 预算不变量被破坏的防御性错误 |

## 测试与演示

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/build_reference.py   # 重建参考诊断
.venv/bin/python scripts/run_demo.py          # 端到端演示
```

演示覆盖：3 个分片、中英混排、五个风险类别、小/中/大三档规模分别命中精确全量、
关键词优先截断、类别分档预算策略，一次优雅停止+恢复，一次规则变更增量重算，
并将固定摘要写入 `work/demo_fixed_summary.json`。

更多设计细节见 `docs/TECH_NOTES.md`。
