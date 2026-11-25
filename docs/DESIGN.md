# OCRLab 设计说明

## 1. 阶段与数据流

```text
manifest + PNG
      │
      ▼
  detect ──► detect product / digest
      │
      ▼
 recognize ─► recognition product / digest
      │
      ▼
  strategy ──► strategy product / digest
      │
      ▼
 diagnostic ─► per-image metrics / digest
      │
      ▼
  deterministic report + bundle
```

每个分片拥有四个独立检查点。阶段输入摘要只包含该阶段确实依赖的内容：

| 阶段 | 主要输入摘要 |
|---|---|
| detect | 引擎版本、PNG 内容 SHA-256、二值化/连通域/合并配置、所选检测策略、无效框探针 |
| recognize | 检测产物摘要、真值字符模板库摘要、识别归一化配置、所选识别策略、空模板探针、PNG 内容 |
| strategy | 检测/识别产物摘要、所选两阶段策略、规模档观测、实际候选数、策略成本配置、计算预算 |
| diagnostic | 清单真值摘要、三个上游产物摘要、计算预算与诊断配置 |

因此：

- 仅像素、二值化参数、连通域或合并规则变化时，最早失效阶段为 `detect`。
- 仅真值文本（模板库）或识别参数变化时，`detect` 继续复用，最早失效阶段为 `recognize`。
- 仅预算/策略成本变化且不改变既有选择时，只失效 `strategy` 与 `diagnostic`；若改变检测/识别选择，则相应阶段也失效。
- 延迟预算只影响查询，不进入阶段指纹。

## 2. 检测

1. 标准库 `image/png` 解码 RGB/RGBA PNG。
2. 使用整数亮度 `(299R + 587G + 114B) / 1000` 灰度化。
3. 阈值 `>0` 时直接使用；`0` 时使用确定性 Otsu。`d-heavy` 在 Otsu 阈值上增加固定 6 个灰度级。
4. `d-light` 使用 4 邻域连通域；`d-balanced`/`d-heavy` 使用 8 邻域。
5. 按垂直重叠和中心距离聚成文本行，再按字符宽度中位数和可配置水平间隙比切分候选框。
6. 输出 6 位小数的归一化框、置信度、来源策略、组件数和墨迹像素数。
7. 框越界或宽高为零时记录 `E_BOX_INVALID`，包含 `image_id` 和框序号，并保留在诊断报告 `issues` 中。

## 3. 识别

### 模板库

对每个分片，从清单真值框中选择中心落入框内的连通字形，按 x 坐标排序并映射到真值字符。若连通字形数与真值字符数不一致，则按真值框做确定性等分单元回退，避免从缺失信息猜测字符。

每个字形归一化为 `NormWidth × NormHeight` 二值向量，去重键为 `(character, SHA-256(vector))`。模板库摘要覆盖模板字符、向量哈希和排序后的字符集。

### 最近邻

- `r-hash-light`：优先按归一化向量 SHA-256 精确查找；未命中时回退到 Hamming 最近邻。
- `r-hash-balanced`：全部模板上的归一化 Hamming 距离。
- `r-hash-dense`：Hamming 距离与 `Grid × Grid` 单元密度距离的加权组合。

输出文本、置信度、平均匹配距离、使用模板数、字形数以及是否在 `max_distance` 内。模板库为空、候选过小或无完整字形时，每个受影响候选均产生 `E_TEMPLATE_MISS`，结果列表仍保留该候选。

## 4. 策略取舍

检测侧三档：`d-light`、`d-balanced`、`d-heavy`。识别侧三档：`r-hash-light`、`r-hash-balanced`、`r-hash-dense`。

每个档位的估算成本：

```text
cost = base_cost + per_char_cost * ceil(total_chars / shards) + per_box_cost * boxes
```

`budget_per_shard = compute_budget_units / shard_count`。评分：

```text
score = quality * quality_weight - cost_weight * (cost / budget_per_shard)
```

规则：

- 小档默认禁止 heavy 档，并在策略产物中写明淘汰原因。
- 超出单分片预算的档位标记不可行并记录原因。
- 所有档位均不可行时选择成本最低档并保留超预算利用率，保证仍可复算和诊断。
- 策略产物记录每个档位的质量、成本、利用率、得分、是否选中及淘汰原因。

## 5. 指标

检测框与真值框按 IoU ≥ 0.5 做一对一贪心匹配，IoU 相同按真值索引、候选索引排序。

- 检测召回率 = 匹配真值框数 / 真值框数。
- 识别准确率 = 匹配且文本完全相等的候选数 / 候选数。
- 字符准确率使用 Levenshtein 编辑距离。
- 平均匹配距离使用所有已识别候选的 `match_distance`。
- 95% 比例误差带使用 Wilson 区间；平均距离使用普通 95% 区间。
- 分母为零时输出 `applicable=false, value=null`，累计 `not_applicable_count`，不使用 NaN。

分组与总计指标由每分片诊断产物按显式计数聚合；预算消耗为所选策略成本之和。

## 6. 缓存与查询

缓存键：

```text
SHA-256({
  kind, image_id/group,
  content_sha256,
  config_version,
  stage_version,
  argument_digest
})
```

`stage_version` 对候选框查询是检测产物摘要，对识别查询是识别产物摘要，对聚合查询是相应多个阶段摘要的规范化哈希。查询缓存 payload 与摘要同时保存；命中时再次校验 payload 摘要。

结果层比较以规范化 `result` 字节和 `result_sha256` 为准；`elapsed_ms`、`query_id`、`cache_hit` 仅属于响应信封，不参与结果一致性。

查询在 goroutine 中执行并通过可取消上下文受 wall-clock 预算约束。超时立即返回 `E_LATENCY_BUDGET`，带查询 ID 和耗时，不等待后台计算。

## 7. 检查点与并发发布

SQLite 表：

```text
checkpoints(image_id, stage) PRIMARY KEY
  seq, input_version, config_version, product_digest,
  artifact_path, product_json, published_at

shard_state(image_id) PRIMARY KEY
  next_seq

query_cache(cache_key) PRIMARY KEY
  image_id, stage, result_json, result_digest, created_at
```

发布流程：

1. 获取 `state/publish.lock`：Unix 使用 `flock`，Windows 使用 `LockFileEx`。
2. 事务内读取 `next_seq`。
3. 校验序号票据 `SHA-256(image_id, seq)`，跨分片票据立即返回 `E_STALE_CHECKPOINT/cross_shard_sequence`。
4. 校验期望前序、连续性、同阶段重复、后序阶段冲突、所需上游阶段。
5. 校验产品摘要与字节一致。
6. 原子写阶段文件（temp → fsync → rename）。
7. 插入检查点，并以 `WHERE next_seq = expected` 条件更新分片序号。
8. 提交事务。

失败原因区分 `sequence_rollback`、`sequence_gap`、`cross_shard_sequence`、`duplicate_checkpoint`、`checkpoint_fingerprint_changed`、`checkpoint_digest_mismatch`、`artifact_missing` 等。只有连续、无冲突的正常推进被接受。

读取检查点时会同时比较 SQLite payload、记录摘要和磁盘产物摘要；被替换的检查点返回 `E_STALE_CHECKPOINT`。

## 8. 增量与全量校验

`verify` 在当前工作区读取并规范化四阶段产物和诊断报告，然后在临时工作区复制配置、清单与 PNG，重新导入并执行 `Rebuild` 全量构建。比较对象包括：

- 每个 `(image_id, stage)` 的完整产品字节与摘要；
- 诊断报告完整字节与 SHA-256；
- 首个差异项，例如 `large-007/recognize`。

任一差异返回 `E_INCREMENTAL_MISMATCH`，并在错误中附首个差异项。

## 9. 确定性约束

- 样本 PRNG 种子固定，PNG 编码确定。
- 所有切片顺序显式排序；不使用 map 遍历构造产物。
- 浮点输出在产物边界固定到 6 或 9 位小数。
- 阶段产物与报告不含时间戳、实际耗时、绝对路径、进程 ID 或随机查询 ID。
- SQLite `seq` 是内部并发控制字段，不进入全量比较语义；产品本身和报告必须一致。
