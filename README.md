# mtlab：本地确定性机器翻译实验系统

这是一个完全离线、CPU 可运行的短语机器翻译实验系统，用来回答一个具体问题：**在数据规模从约 200 句对增加到约 12,000 句对时，对齐粒度、短语表规模和解码束宽应如何取舍。**

系统不依赖真实平行语料、预训练模型、GPU、Docker 或网络数据。三档语料由固定种子的本地生成器产生；五个处理阶段均保存原子检查点，支持停止恢复、版本失效传播、并发发布保护与逐字节可复现的固定摘要。

## 快速开始

要求 Python 3.11+ 与 NumPy。当前交付产物由 Python 3.11.18 / NumPy 2.4.6 生成，并在该解释器下完成全部单元测试与验收；另用 Python 3.14.4 / NumPy 2.5.5 从零复核，固定摘要与 `run_summary.json` 均逐字节一致。

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

python -m mtlab doctor
python -m mtlab generate --workspace . --seed 20261123
python -m mtlab run --workspace . --data-dir data
python -m mtlab status --workspace . --data-dir data
python -m mtlab verify-policy --workspace . --data-dir data
```

单元测试与完整验收：

```bash
python -m unittest discover -s tests -v
python scripts/acceptance.py
```

`scripts/acceptance.py` 会从零运行两套三档实验，检查检查点复用、档间停止恢复、SIGTERM 优雅停止、错误注入、版本失效传播、并发发布和摘要字节一致性，并把实际结果写入 `artifacts/acceptance_report.json`。

## 目录

```text
mtlab/                     核心实现
  generator.py             固定种子语料与分档清单生成器
  tokenizer.py             NFKC、空白折叠、全半角统一、token/BPE
  split_stage.py           切分阶段（E_EMPTY_SENTENCE 诊断）
  alignment_stage.py       IBM-1 对数域估计与精确/近似对齐
  policy_stage.py          规模感知的算法取舍与可复算打分
  translation.py           短语表训练与对数域集束搜索
  evaluation_stage.py      逐句 BLEU 近似、词精确率与置信区间
  checkpoints.py           文件锁、原子检查点账本、失效删除
  pipeline.py              阶段编排、恢复、状态查询、固定摘要
  cli.py                   命令行接口
scripts/
  acceptance.py            集成验收入口
  publish_probe.py         多进程检查点发布探针
tests/test_unit.py         核心单元测试
data/                      三档语料、参考译文、分档清单
artifacts/                 检查点、阶段产物、固定摘要、验收报告
```

## 数据与异常

固定种子为 `20261123`。三档句对数为 200 / 2,000 / 12,000，中英双向均衡。每条语料记录包含：

```json
{
  "pair_id": "small-000001",
  "source_lang": "en",
  "target_lang": "zh",
  "lang_pair": "en-zh",
  "source": "a good river read the student.",
  "target": "一好河读的生。",
  "domain": "daily",
  "noise_flags": []
}
```

分档清单声明句对数、平均句长、原始词表规模、token 上界、语言对/域/噪声计数、语料与参考文件 SHA-256。参考译文独立存放于 `data/references/<tier>.jsonl`，是评测的唯一真值来源。

生成器覆盖四类异常：多余空白、全半角混排、零长度比例、纯标点句子。规范化后为空或仅含标点的句对不会静默消失，而是写入 `split/diagnostics.jsonl`，带 `code=E_EMPTY_SENTENCE` 与 `pair_id`，并在评测中显式计入跳过数。

## 分阶段算法

### 1. 规范化、切分与 BPE

1. Unicode NFKC、兼容宽度与全角 ASCII 统一、空白折叠、`lower()`。
2. 中文按单字切分；拉丁连续串整体成为一个预切分 token；标点与符号单独标记。
3. 使用本档语料统计字节对合并规则。规则只合并偏移相邻、语言相同的中文单元，按 `(count 降序, lang, left, right)` 确定性选序；合并后的 token 保留起止字符偏移、`lang`、`merge_source`（原始单元）和 `merge_rules`（规则序号）。
4. 有效句对按原语料顺序写入 `tokens.jsonl`；异常写入 `diagnostics.jsonl`。

`start`/`end` 是基于规范化后文本的字符偏移（左闭右开），不是原始输入字符串偏移；需要映射回原文时应先应用同一套 NFKC、空白折叠、宽窄统一与小写化流程。

BPE 上限随档位增大：小档 `40`、中档 `80`、大档 `160`（由 `bpe_merges=40` 乘以 1/2/4 得到），因此大档拥有更大的子词词表。

### 2. 对数域词对齐

每个语言对独立统计 token 共现矩阵，使用带可配置平滑 `alpha` 的 IBM-1 EM：

```text
log p(t|s) = log(count(s,t) + alpha) - log(sum_t count(s,t) + alpha*|V_t|)
alignment score = log p(t|s)
posterior = exp(score - logsumexp_t(score))
```

概率更新全部在对数域完成；期望计数使用稳定的归一化指数。`discard_threshold` 裁剪低概率表项，但每一源词至少保留最优目标项，若整表会被裁空则返回 `E_NUMERIC`。

对齐结果同时保存两种粒度：

- `word_exact`：在预切分词单元上使用对数转移分数与单调位置惩罚做一最优对齐，再投影到 BPE token 偏移。
- `subword_approx`：只用长度归一化的单调位置进行低成本对齐，再投影到 BPE token 偏移。

`alpha <= 0`、缺失语言对、负计数、非有限概率或空对齐表均返回带 `lang_pair` 与词项位置的 `E_NUMERIC`。

### 3. 规模感知取舍

候选组合为两种对齐粒度 × 短语表上限 `{128,256,512,1024,2048,4096}` × 束宽 `{2,4,6,8,10}`。固定打分公式：

```text
alignment_exact  = 0.74 + 0.18*min(1,n/1200) + 0.02*min(1,avg_len/10)
alignment_approx = 0.80 + 0.06*min(1,n/12000) + 0.01*min(1,avg_len/10)
phrase_quality   = 0.10*log2(1+phrase_limit)/log2(1+4096)
beam_quality     = 0.06*log2(1+beam_width)/log2(1+10)
work             = n*max(avg_len,1)*max(beam_width,1)*(1+log2(1+phrase_limit))
cost_penalty     = 0.05*work/budget
score            = alignment_quality + phrase_quality + beam_quality - cost_penalty
```

尺度硬约束与预算必须同时可行：

| 句对数 | 最大短语表 | 最大束宽 |
|---:|---:|---:|
| `< 500` | 256 | 4 |
| `500 .. 4999` | 1024 | 6 |
| `>= 5000` | 4096 | 10 |

所有候选的分数、成本、淘汰原因均写入 `policy/policy.json`。`decision_fingerprint` 是结论的规范化 JSON SHA-256；`python -m mtlab verify-policy` 会独立重算并比对。

### 4. 短语训练与确定性解码

从选中对齐粒度抽取源/目标连续短语，短语概率采用加性平滑，候选按稳定键排序并裁剪至所选上限。解码按源位置执行对数域动态束搜索：

- 固定短语顺序、固定去重键、固定字典序 tie-break；
- 每个目标句输出 `candidate_tokens`、`candidate_text`、`log_score`、长度归一化分数与搜索轨迹摘要；
- 束宽为零、NaN/Inf、无完整路径、归一化分数为零时返回 `E_DECODE_FAILED`，附 `pair_id` 与 `step_index`。

### 5. 指标与误差带

逐句 BLEU 近似使用最高 4 阶、add-one 平滑与 brevity penalty；词级精确率为候选/reference 多重集合交集除以候选 token 数。按档计算：

- 有效句对数、总输入句对数、跳过句对数与每个跳过原因；
- 均值、样本标准差、标准误；
- `mean ± z * stdev / sqrt(n)` 的正态近似置信区间，裁剪到 `[0,1]`；
- 越界并发生裁剪的次数。

`pair_metrics.jsonl` 为每条输入句对保留 `ok` 或 `skipped` 记录，异常句对不会被静默丢弃。

## 检查点、锁与失效传播

每档严格按 `split → align → policy → decode → evaluate` 推进，每阶段一条记录，包含档位、阶段、序号、输入描述与指纹、取舍结论指纹、产物树摘要和路径。记录文件使用临时文件 + `fsync` + `os.replace` 原子发布；阶段产物先写入独立 staging 目录，再在同一文件系统内原子改名。

发布时使用 `fcntl.flock` 的真实互斥锁，检查：

- 档位内序号必须恰好为 `current + 1`；
- 不允许重复阶段、落后序号或缺口；
- 不允许跳过前置阶段；
- artifact `_meta.json` 的档位/阶段必须与发布目标一致。

违反序号或跨档位发布返回 `E_STALE_CHECKPOINT`；缺失前置阶段或非法跳转返回 `E_INVALID_STAGE`。

每次状态查询按阶段重建当前输入指纹，最早不匹配的阶段及其全部下游被列为失效；未受影响上游继续复用。失效原因包括 `corpus_content_changed`、`bpe_rules_config_changed`、`smoothing_changed`、`policy_budget_changed`、`decode_width_changed` 等。实际运行时通过账本锁删除失效阶段及其下游记录和产物，再从该阶段连续重建。

SIGTERM/SIGINT 只设置停止标志，在所有句对/合并单元边界检查；当前不可拆分单元完成后退出，退出码 `75`。`--stop-after-tier` 用于稳定的档间停止演示。强制 `SIGKILL` 不承诺退出报告；原子写入保证不会留下半条记录。

## 错误码

| 错误码 | 含义 |
|---|---|
| `E_EMPTY_SENTENCE` | 规范化后空句/纯标点，作为诊断保留并带 `pair_id` |
| `E_NUMERIC` | 平滑为零、语言对语料不足、非正/非有限计数、空对齐表 |
| `E_DECODE_FAILED` | NaN/Inf、零束宽、无完整路径、零归一化分数 |
| `E_STALE_CHECKPOINT` | 重复/落后/跳号/跨档位发布 |
| `E_INVALID_STAGE` | 缺失前置检查点、非法阶段跳转或指标无法完整对账 |
| `E_POLICY_FAILED` | 预算与硬约束下不存在可行取舍组合 |

业务错误以稳定 JSON 输出：`{"error":{"code":...,"message":...,"details":...}}`。

## 可复现性与实际验收结果

主工作区实际执行：

```text
python -m mtlab generate --seed 20261123
python -m mtlab run --data-dir data
```

固定结论：

| 档位 | 有效/总句对 | 对齐 | 短语上限 | 束宽 | BLEU 均值 [95% CI] | 词精确率均值 |
|---|---:|---|---:|---:|---|---:|
| small | 193/200 | subword_approx | 256 | 4 | 0.775032 [0.742277, 0.807788] | 0.888035 |
| medium | 1947/2000 | word_exact | 1024 | 6 | 0.988978 [0.986781, 0.991175] | 0.992893 |
| large | 11687/12000 | word_exact | 4096 | 10 | 0.999895 [0.999826, 0.999963] | 0.999904 |

固定摘要：

```text
summary_digest           = d724199c6723b1df91c49d0879be0823fb6fa24fcf8574bdcd2bce246452a298
run_summary.json SHA-256 = cdbe2b946b405563e6e3db82591bbef584d4bb98e7ad64b6b1cdd9e91daf7b43
```

实际验证结果：

- 同参数重跑：所有 15 个阶段全部复用，`run_summary.json` 字节不变。
- 小档后停止再恢复：退出码 75；恢复后三档 15 个阶段产物树摘要、检查点账本与一次完整运行逐字节一致；摘要仍为上述固定值。
- 小档 `smoothing=0`：`E_NUMERIC`，附 `lang_pair=en-zh` 与词项位置。
- 小档 `beam_width=0`：`E_DECODE_FAILED`，附 `pair_id=small-000001`、`step_index=0`。
- 平滑强度改变：最早失效阶段为 `align`，保留 `split`，下游为 `align/policy/decode/evaluate`。
- BPE 配置改变：最早失效为 `split`；预算或解码宽度改变：最早失效为 `policy` 并保留 `split/align`。
- 两个进程同时发布同一档位第 1 号检查点：恰好一个成功，另一个返回 `E_STALE_CHECKPOINT`，账本只保留一条连续记录。
- 非法首阶段、重复发布、跳号、跨档位发布分别返回 `E_INVALID_STAGE` / `E_STALE_CHECKPOINT`。
- SIGTERM 在阶段内触发：完成当前单元后退出码 75，写入停止报告。

完整机器可读结果见 `artifacts/run_summary.json`、`artifacts/acceptance_report.json` 与 `artifacts/checkpoints/ledger.json`。

## 说明与边界

- 样本是确定性合成数据，用于规模取舍实验，不代表真实语料分布或生产翻译质量。
- 短语模型在训练语料上解码，属于封闭集实验；BLEU 主要验证可复现性和规模效应，不应据此宣称真实泛化能力。
- 评测仅使用本地 reference 文件，不使用源句、目标句或生成器内部映射作为真值。
- 不提供 GPU、神经网络预训练或外部下载路径。

