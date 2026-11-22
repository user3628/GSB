# 技术设计

## 1. 目标与不变量

实现只依赖本地 CPU、Python 标准库、NumPy 和 jsonlines。以下不变量贯穿各阶段：

1. 同一输入记录、同一配置与同一阶段版本产生完全相同的规范 JSON 字节。
2. 所有阶段记录按 `sample_id` 升序保存。
3. 浮点分数在输出前统一量化到 12 位小数；汇总指标量化为六位定点整数。
4. 顺序、并行、增量、停止恢复后的阶段产物必须逐字节一致。
5. 任何公开写操作先写同目录临时文件、`fsync`，再 `os.replace`；读者不会看到半写 JSON。
6. 所有 `set`/`dict` 导出前排序或通过规范 JSON 的排序键序列化，不能依赖 Python 哈希随机化。

## 2. 目录与运行状态

```text
run-dir/
  config.json                    # 当前完整配置
  state.json                     # 输入/配置/阶段摘要与原子状态
  input/records.json             # 导入后的规范记录集合
  stages/stage1.json             # 规范化与切分
  stages/stage2.json             # 逐样本判定
  stages/stage3.json             # 分层汇总
  stages/stage4.json             # 标注一致性验证
  checkpoints/<20位序号>.json     # 每阶段单调检查点
  checkpoints/.publish.lock       # fcntl.flock 文件锁
  cache/query_cache.json         # 查询结果字节缓存
  control/stop.requested         # 停止请求
  .run.lock                      # 长操作互斥锁
```

每个阶段产物都是纯 payload，不含自身摘要；阶段摘要由文件字节的 SHA-256 计算，避免自引用。

## 3. 规范 JSON、指纹与缓存键

规范 JSON：

```python
json.dumps(value, ensure_ascii=False, sort_keys=True,
           separators=(",", ":"), allow_nan=False) + "\n"
```

指纹类型：

- `content_fingerprint = sha256(canonical(record))`：单样本原始内容。
- `input_fingerprint = sha256(canonical({"schema_version", "records"}))`：整个输入集合。
- 阶段输入指纹：`sha256(stage, upstream_fingerprint, relevant_config_fingerprint, schema_version)`。
- 阶段产物指纹：阶段 JSON 文件字节的 SHA-256。
- 配置版本：完整配置对象 SHA-256；阶段配置指纹只含该阶段相关字段。

查询缓存键：

```text
sha256({
  kind, content_fingerprint, config_version,
  stage_fingerprint, scope
})
```

样本查询使用样本 `content_fingerprint` 与阶段 2 产物摘要；汇总查询使用输入内容指纹与阶段 3 产物摘要。缓存值保存规范响应字符串，命中时直接取回；清空缓存后的重算走同一序列化路径，因此字节一致。

## 4. 阶段 1：规范化与切分

处理顺序：

1. `unicodedata.normalize("NFKC", text)`：完成全半角与兼容字符统一。
2. `.lower()`。
3. `\s+ -> " "` 并裁剪首尾空白。

分词器逐字符扫描规范化文本：

- CJK 统一表意字符、扩展区、假名和韩文音节各成一个 token；
- 拉丁/字母数字连续串保留为一个 token，内部允许 `_ - ' .`；
- 标点不产生 token；
- token 的 `start`/`end` 是规范化文本中的半开字符区间。

切分器在句号、问号、感叹号、分号、中文逗号和英文逗号处分句。数字之间的小数点不被切分，`U.S.` 这类后续无空白的点不被切分。句子内的 token 偏移加上了句子绝对起点。

输出或任一证据为空时，记录仍保留，状态为 `error`，`errors` 内包含带 `sample_id` 的 `E_EMPTY_FIELD`。阶段 2 对这类记录不伪造自动判定，而是保留 `hallucination=null` 并标记复核。

## 5. 阶段 2：一致性分数与幻觉判定

对每个输出句和每个证据句计算四个对数域分量：

```text
term  = |tokens(output) ∩ tokens(evidence)| / |tokens(output)|
ngram = |char_ngrams(output) ∩ char_ngrams(evidence)| / |char_ngrams(output)|
hash  = cosine(hashed_token_and_char_ngram_vector(output),
               hashed_token_and_char_ngram_vector(evidence))
prior = evidence_weights[source] 或 evidence_weights["__default__"]
```

哈希向量把 token 和字符 n-gram 用 SHA-256 散列到固定桶数，避免 Python 进程随机哈希。基础分数：

```text
log_score = Σ weight[c] * ln(component[c] + 1e-12) / Σ weight[c]
base_score = exp(log_score)
```

确定性规则因子：

- 输出出现证据中不存在的数字：乘 `0.50`，标记 `NUMERIC_MISMATCH`；
- 输出与证据否定极性不同：乘 `0.55`，标记 `NEGATION_POLARITY_MISMATCH`；
- 最终分数 = `base_score * rule_factor`，裁剪到 `[0,1]`。

每个输出句选择最高分证据句。仲裁键固定为：

```text
(score_微分为整数, -证据索引, -证据句索引) 取最大
```

因此同分时总选择最小证据索引和最小证据句索引。

- 句子分数 `< sentence_support_threshold` → 无支撑句。
- `无支撑句数 / 输出句数 > hallucination_ratio_threshold` → 幻觉。
- 样本一致性分数是各输出句分数的对数域几何平均。

## 6. 阶段 3：分层汇总

分层集合是“输入中出现的 `(source, lang)`”与配置 `declared_strata` 的并集。因此可以显式保留零样本层。

平均一致性不使用原始分数算术平均，而是按 `sample_id` 升序逐项执行：

```python
log_sum = -inf
for sample in sorted(samples, key=sample_id):
    log_sum = np.logaddexp(log_sum, log(score))
avg = exp(log_sum - log(n))
```

定点输出：

- `hallucination_rate_fp = round_half_up(rate * 1_000_000)`；
- `avg_consistency_fp = round_half_up(avg * 1_000_000)`；
- 字符串形式固定六位小数，例如 `0.411765`。

空层保留完整字段，计数为 0，一致性字符串为 `0.000000`，`log_consistency_sum=null`。

## 7. 阶段 4：人工标注一致性

只对“自动判定存在且人工 `is_hallucination` 非空”的样本计算混淆矩阵：

- TP：自动幻觉、人工幻觉；
- TN：自动非幻觉、人工非幻觉；
- FP（误判）：自动幻觉、人工非幻觉；
- FN（漏判）：自动非幻觉、人工幻觉。

误判和漏判清单包含样本 ID、自动/人工标签，以及 `consistency_score`、`unsupported_ratio`、逐句分数、规则命中和最佳证据位置。

每个 source 统计：

- 人工标注覆盖率 = 有布尔人工标签的样本数 / 该 source 样本数；
- 一致率 = `(TP+TN) / 可比样本数`；
- 缺失标注样本 ID 列表；
- 一致率低于 `consistency_min` 或覆盖率低于 `min_annotation_coverage` 时 `review_required=true`；
- 给出确定性中文复核建议。

标注缺失不会被跳过：缺失样本进入 `samples_requiring_review`，其 source 的覆盖率会下降，缺失 ID 出现在建议中。

## 8. 增量重算

阶段相关配置字段：

| 阶段 | 字段 |
| --- | --- |
| 1 | `normalization_version` |
| 2 | `scoring_version`、句/样本阈值、评分权重、证据权重、`ngram_size`、`hash_buckets` |
| 3 | `declared_strata` |
| 4 | `consistency_min`、`min_annotation_coverage` |

`Config.changed_stage(previous)` 返回最小变化阶段。输入内容指纹变化总是从阶段 1 开始。运行器复用 `stage < earliest` 且摘要有效的产物，只计算该阶段及下游。

每次运行结束前，默认用 `parallel_workers=1` 从零计算完整流水线并与已发布 payload 按规范字节比较。差异返回：

```json
{
  "code": "E_INCREMENTAL_MISMATCH",
  "details": {
    "different_stages": [2],
    "different_sample_ids": ["zh-001"]
  }
}
```

这使增量实现可以保留阶段复用性能，同时持续审计顺序全量等价性。

## 9. 延迟预算与查询

查询在 daemon 线程中执行，主线程通过 `queue.get(timeout=budget_ms/1000)` 等待。超时后立即返回 `E_LATENCY_BUDGET`，不继续同步阻塞。

响应包含：

- `query_id`：调用方提供或自动生成；
- `budget_ms`；
- `elapsed_ms` 与 `elapsed_us`。

预算为 0 时在启动查询前立即返回错误。成功响应不包含随机 query ID，避免破坏缓存字节一致性。

## 10. 检查点与并发发布

检查点文件名为 20 位零填充序号。发布流程：

1. 打开 `checkpoints/.publish.lock`；
2. `fcntl.flock(LOCK_EX)`；
3. 扫描最大已发布序号；
4. 显式序号 `<= last` 时返回 `E_STALE_CHECKPOINT`；
5. 自动序号取 `last + 1`；
6. 原子写入 `{sequence:020d}.json`；
7. 释放锁。

每条检查点记录包含 `stage`、`input_fingerprint`、`config_version`、`artifact_digest`、`sequence` 和发布时间。临时文件使用点前缀与 `.tmp` 后缀，扫描时忽略；即使进程被杀，也不会把半写 JSON 当检查点。

双进程同时显式发布序号 1 时，只有第一个成功，第二个得到 `E_STALE_CHECKPOINT`。自动化测试实际启动两个子进程验证了该行为。

## 11. 停止与恢复

- 阶段之间检查停止文件；已完成的阶段已原子发布。
- 阶段 1/2 的不可拆分单元是单条样本；顺序模式下每完成一条检查一次停止请求。
- 阶段 3/4 是单次原子汇总单元，完成后才响应停止。
- 停止不会发布不完整阶段；状态记录 `stopped` 与已完成单元数。
- `resume` 删除停止文件后，从未完成阶段继续。恢复结果默认再次通过从零全量字节审计。

并行模式下，停止会在已完成的有序结果数量达到一个单元边界后生效；已提交但未使用的后台任务不会发布。需要严格逐样本停止边界时可设置 `parallel_workers=1`。

## 12. 已知边界

- 判定器是词面/字符级规则，不做语义推理；语义等价但词面差异大的表述可能被标为无支撑。
- 空证据被视为不可自动判定，而不是默认为幻觉；它会进入复核清单。
- 只有通过 `declared_strata` 声明的零样本层才能在首次全量运行中保留；未声明的空分层无法从数据本身推断。
- 增量一致性审计会额外执行一次顺序全量计算，适合本地评测数据；超大语料可显式关闭审计，但会失去本次运行的自动等价证明。
