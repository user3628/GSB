# XRel 跨语言相关性打分与误差传播工作台

本项目从零实现一个完全本地、确定性的跨语言检索打分工作台。输入为固定随机种子生成的 JSONL 查询-文档分片、本地桥接词典和 YAML 打分配置；输出包括四阶段产物、四档相关性标签、分桶/混淆校验、逐 pair 数值误差诊断、可查询报告，以及编辑后的增量重建与全量逐字节一致性校验。

不依赖真实检索服务、翻译模型、外部站点、容器或付费资源。

## 快速开始

```bash
# 1. 离线虚拟环境（使用本机已有、可离线访问的包）
python3 -m venv --system-site-packages .venv
.venv/bin/pip install --no-index -r requirements.txt

# 2. 重建固定种子数据
.venv/bin/python scripts/generate_demo.py --out data --seed 20261121 --force

# 3. 运行完整演示：全量基线 + 停止/恢复 + 词典编辑增量重建 + 影子全量比对
.venv/bin/python -m xrel_workbench demo --seed 20261121

# 4. 测试
.venv/bin/python -m pytest -q

# 5. 启动本地交互式工作台（仅监听回环地址）
.venv/bin/python -m xrel_workbench serve \
  --workspace artifacts/demo/workspace --host 127.0.0.1 --port 8765
```

Windows 下将 `.venv/bin/python` 替换为 `.venv\Scripts\python`。文件锁在 POSIX 使用 `fcntl.flock`，在 Windows 使用 `msvcrt.locking`。

## 演示数据

生成器固定种子为 `20261121`，实际种子写入 `data/manifest.json`。当前数据包含 3 个分片、每片 8 条，共 24 条：

- 中询英档：`p0001`、`p0006`、`p0007`、`p0009`、`p0010`、`p0013`、`p0017`、`p0021` 等。
- 英询中档：`p0002`、`p0005`、`p0014`、`p0018`、`p0022` 等。
- 同语言对：中文 `p0003`、`p0012`、`p0019`；英文 `p0004`、`p0011`、`p0020`、`p0024`。
- 跨分片重复文本：`p0001`、`p0007`、`p0009` 使用同一查询与文档文本。
- 超长文本：`p0016` 的中文长文档、`p0024` 的英文长文档。
- 空查询/空文档：`p0008` 为空查询；`p0023` 同时为空查询与空文档，均产生 `E_EMPTY_TEXT` 诊断而不丢失样本。
- 人工等级：覆盖 0-3；`p0022` 刻意形成人工等级与模型档位偏移，用于触发待复核。

重建数据会得到完全相同的 JSONL、桥接词典、YAML 配置和 SHA-256 摘要。

## 四阶段流水线

### 阶段 1：规范化与词项抽取

- 顺序执行 Unicode NFKC、空白折叠、全半角统一和小写化。
- 中文连续区段输出单字及相邻二字组合；拉丁字母数字输出连续串。
- 每个词项包含 `term`、`term_id`、`start`、`end`、`lang`、`script`。偏移相对规范化后的文本。
- 空查询或空文档输出 `E_EMPTY_TEXT` 诊断，同时保留 pair 继续处理。
- 所有 JSON/JSONL 使用 `sort_keys=True`、固定分隔符、UTF-8、LF 和 `allow_nan=False`，保证字节稳定。

### 阶段 2：跨语言映射与定点量化

对每个查询词项：

1. 先在文档词项中查找同形直接匹配。
2. 再通过本地桥接词典查找同语向/反向候选；词典目标词按空白拆分并匹配文档词项。
3. 计算查询词与目标词的字符 n-gram Dice 重叠度 `overlap`。
4. 合成映射权重：

```text
mapping_weight = lexical_coefficient * overlap
               + bridge_coefficient  * entry_weight
```

BM25 类文档词项权重按分片统计计算：

```text
idf         = log(1 + (N - df + 0.5) / (df + 0.5)) + smooth_strength
tf_norm     = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl))
raw_weight  = mapping_weight * idf * tf_norm * query_tf
```

定点量化使用有符号位宽 `bits`、`qmax = 2^(bits-1)-1`、2 的幂缩放因子和 `round_half_even`。每个词项记录 `q`、`scale_factor`、`step`、舍入方式、量化值、实际误差和解析误差上界 `0.5 / scale_factor`。负桥接权重不会被最大值合并静默吞掉，而是显式传播到阶段 3。

### 阶段 3：对数域打分

量化权重还原后，每个词项的对数贡献为 `log1p(weight_quantized)`，最终分数为：

```text
score_unclipped = Σ log1p(weight_quantized,i)
score           = min(score_unclipped, score_clip)
```

lexical 与 bridge 来源贡献按两源权重占比精确分摊，二者之和等于该词项的对数贡献。全程不做概率连乘，只在对数域累加。

若出现负权重、NaN、Inf、量化缩放因子为零、量化值/缩放因子不一致或非有限聚合分数，返回：

```json
{"code":"E_NUMERIC","details":{"pair_id":"...","term":"...","stage":"stage3_log_domain_score","reason":"..."}}
```

不会静默产出零分或空结果。空查询/空文档的零候选是显式 `E_EMPTY_TEXT` 业务诊断，不属于数值错误。

### 阶段 4：分档、校验与误差传播

- 按三个阈值分为：不相关、弱相关、相关、强相关。
- 与人工等级做 4×4 混淆统计、逐档占比偏移、经验边界偏差与越界样本清单。
- 任一分档占比偏差超过配置阈值、经验边界偏差超限或存在人工偏移样本时标记待复核并给建议。
- 每个 pair 使用 `Decimal` 50 位精度的独立 `ln1p` 参考路径，与定点量化路径比较。
- 解析误差上界由显式缩放因子导出：`Σ 0.5/scale_factor`。另与词项记录中的误差界逐项交叉核对。
- 输出 `observed_quantization_error`、`analytical_error_bound`、不确定区间、有效位数、逐来源对数贡献。
- `effective_digits = floor(-log10(bound / max(|reference_score|, 1e-30)))`，并受配置上限约束。
- 语料级按分片累计规模做最小二乘趋势估计，同时输出最大误差界与平均误差界随规模的变化。

## 版本指纹与增量失效

每个阶段使用内容摘要组合成指纹，而不是时间戳：

| 变化输入 | 最早失效阶段 |
|---|---|
| 分片文本/人工等级 | `stage1` |
| 桥接词典相关条目、量化位宽、平滑强度、BM25 参数 | `stage2` |
| 分数截断上限 | `stage3` |
| 相关性阈值、参考分布、诊断配置 | `stage4` |

桥接词典按分片实际出现的中英词项筛选“相关词典视图”，未使用条目的变化不会重算打分。原始输入摘要和配置版本进入聚合指纹，因此即使改动不影响分数，汇总摘要也会刷新。

流水线只复用具有相同指纹、产物摘要存在且检查点有效的上游产物。任一编辑后默认启动影子工作区从零全量构建，逐字节比较：

- 所有分档结果 `state/artifacts/stage4/*.jsonl`
- `reports/error_report.json`
- `reports/validation.json`
- `reports/summary.json`
- `exports/scores.jsonl`

不一致返回 `E_INCREMENTAL_MISMATCH`，包含差异路径、双方摘要和首个差异字节偏移。

## 检查点、停止与并发保护

- 检查点以独立 JSON 文件原子写入，包含 `seq`、阶段、分片、输入摘要、配置版本、产物路径/摘要、阶段指纹和前一记录摘要链。
- 临时文件只在同一目录以隐藏 `.tmp` 后缀存在，原子 `os.replace` 后才成为可见记录；中断不会留下半写 JSON 记录。
- 所有发布在同一 `state/checkpoint.lock` 下执行，序号必须严格 `last_seq + 1`。
- 重复发布、落后序号、跨阶段输入摘要错位返回 `E_STALE_CHECKPOINT`。
- 缺失前置检查点/产物或前置产物摘要不符返回 `E_INVALID_STAGE`。
- 停止请求在每个不可拆分分片单元边界检查；当前单元完成后进入 `stopped`，恢复时复用已完成检查点，序号连续推进。
- 构建本身另有 `state/build.lock`，避免同一工作区并发执行两条构建流水线。

## 工作区布局

```text
workspace/
  inputs/
    shards/*.jsonl
    bridge.json
    scoring.yaml
  state/
    artifacts/stage1|stage2|stage3|stage4/*.jsonl
    checkpoints/00000001-stage1-part-001.json
    manifest.json
    run_status.json
    stop.request.json
    build.lock
    checkpoint.lock
    full_baseline_summary.json
  reports/
    summary.json
    validation.json
    error_report.json
  exports/
    scores.jsonl
```

## 命令行

```bash
.venv/bin/python -m xrel_workbench --help

# 初始化/运行
python -m xrel_workbench init-workspace --data data --workspace artifacts/workspace --force
python -m xrel_workbench run --workspace artifacts/workspace
python -m xrel_workbench run --workspace artifacts/workspace --full

# 状态、指纹、报告、导出
python -m xrel_workbench status --workspace artifacts/workspace
python -m xrel_workbench fingerprints --workspace artifacts/workspace
python -m xrel_workbench report --workspace artifacts/workspace --kind validation
python -m xrel_workbench report --workspace artifacts/workspace --kind grades --shard part-001
python -m xrel_workbench export --workspace artifacts/workspace --out scores.jsonl

# 编辑并自动增量重算/校验
python -m xrel_workbench edit-bridge --workspace artifacts/workspace \
  --entry-id br-003 --weight 0.71
python -m xrel_workbench edit-config --workspace artifacts/workspace \
  --path mapping.quantization_bits --value 14
python -m xrel_workbench edit-pair --workspace artifacts/workspace \
  --shard part-001 --pair-id p0001 --query '跨语言检索排序'

# 导入、停止、恢复、比对
python -m xrel_workbench import-shard --workspace artifacts/workspace \
  --shard part-001 --file data/shards/part-001.jsonl
python -m xrel_workbench import-bridge --workspace artifacts/workspace --file data/bridge.json
python -m xrel_workbench stop --workspace artifacts/workspace
python -m xrel_workbench resume --workspace artifacts/workspace
python -m xrel_workbench verify --workspace artifacts/workspace
```

## HTTP 工作台

启动后打开 `http://127.0.0.1:8765/`。页面支持查看状态/指纹/分档/校验，编辑桥接条目、配置路径、单 pair 文本和人工等级，批量导入分片/词典，触发行全量/增量重算、停止、恢复、导出。

主要接口：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/health` | 健康检查 |
| GET | `/api/status` | 阶段、指纹、检查点、运行状态 |
| GET | `/api/fingerprints` | 版本指纹与输入摘要 |
| GET | `/api/inputs` | 分片名、词典、配置 |
| GET | `/api/shards/<name>` | 查询分片原始 pair |
| GET | `/api/grades?shard=&pair_id=` | 分档结果 |
| GET | `/api/errors?pair_id=` | 误差传播报告 |
| GET | `/api/validation` | 分桶/混淆/边界校验 |
| GET | `/api/summary` | 固定汇总摘要 |
| GET | `/api/export/scores` | 下载 JSONL |
| POST | `/api/import/shards` | JSON 或 multipart 导入分片 |
| POST | `/api/import/bridge` | 导入桥接词典 |
| POST | `/api/edit/bridge` | 编辑条目，默认重算并校验 |
| POST | `/api/edit/config` | 编辑点路径配置 |
| POST | `/api/edit/pair` | 编辑单 pair 文本/等级 |
| POST | `/api/run` | 全量/增量构建 |
| POST | `/api/stop` / `/api/resume` | 停止/恢复 |
| POST | `/api/verify` | 影子全量逐字节比对 |
| POST | `/api/checkpoint` | 手动原子发布检查点 |

服务默认仅绑定 `127.0.0.1`，不访问外部网络。

## 实际验收结果

详见 `docs/VALIDATION.md` 与 `artifacts/demo/verification_report.json`。当前运行结果摘要：

- 测试：`29 passed`，含 Windows `msvcrt` 锁路径模拟与 4 进程并发发布竞争。
- 初始全量：3 分片、24 pair、检查点 `1..13` 连续，固定 `build_id = d4ece83229f613bfca9f2b26110c71a0ea5792843487d79980b042342f9e138d`。
- 停止/恢复：第 4 个单元后停止，恢复后 `build_id` 与初始全量相同，检查点 `1..13` 连续。
- 编辑 `br-003` 权重 `0.92 -> 0.71`：3 个阶段 1 产物复用，重建 10 个下游单元，最早失效阶段均为 `stage2`；与影子全量比较 0 差异。
- 词典编辑后最终 `build_id = 312fe77bebd629cbd5eb8b7367412c0d16de083c998485c4958a35a8feadeb99`，检查点 `1..23` 连续。
- 分档计数：`[3, 2, 3, 16]`，24 pair 全部有档位。
- 量化误差交叉核对失败数：`0`；误差界路径不一致数：`0`。
- 最大解析误差上界：`0.01953125`；分数有效位数范围 `2..12`。
- 越界/待复核样本：`p0005`、`p0008`、`p0022`、`p0023`、`p0024`。其中 `p0022` 是刻意注入的人工等级偏移，空查询样本和边界样本也按要求保留并给出建议。
- `p0012` 分数达到配置上限 `6.0`，截断路径已被覆盖。

## 说明与边界

- BM25 文档统计按分片计算，使单分片文本修改仅影响该分片链；语料级报告再聚合所有分片。
- 字符偏移基于规范化文本，而不是原始输入文本；这是为了保证词项编号和边界完全可重建。
- 停止粒度为分片阶段单元，支持在单元之间停止，不在一个分片产物写入中途停止。
- `--full` 在已有同输入工作区重复执行可能触发 `E_STALE_CHECKPOINT`，这是重复发布保护；从零基线和影子构建均在全新工作区运行。
- 演示按统一参考分布评分，因此会因语料刻意偏斜而标记分布待复核；这属于校验发现，不是构建失败。
- 全部随机性仅来自生成器的固定种子；排序、序列化、编号、浮点运算顺序和报告字段均固定。
