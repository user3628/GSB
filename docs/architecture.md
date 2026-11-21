# 架构与确定性不变量

## 数据流

```text
UTF-8 JSONL shards
        |
        v
[1] normalize + parse  -- stage1 digest --> text offsets, number/unit/calendar fragments
        |
        v
[2] decimal unit + calendar normalization -- stage2 digest --> exact canonical value
        |
        v
[3] entity/category grouping + log confidence + arbitration -- stage3 digest
        |
        v
[4] evidence links + versioned manifest -- chain digest --> JSON manifest
```

每个实体的四阶段产物分别写入带单调序号的检查点。批量输出按 `entity_id`、`claim_id` 和类别排序，聚合顺序不影响裁决。

## 阶段 1：规范化与解析

`normalize_text` 生成 NFKC 文本及逐输出字符到原始字符区间的映射。空白折叠时，合并后的空格保留首尾原始边界。解析结果中的每个片段都有：

```json
{
  "field": "raw_value",
  "text": "1.38",
  "start": 0,
  "end": 4,
  "source_start": 0,
  "source_end": 4
}
```

`start/end` 是规范化文本中的码点偏移，`source_start/source_end` 是原字段中的半开区间。数值片段与嵌入比例、单位片段分别记录。任何空数值、无法解析或混合分隔符不能被合理解释时，抛出 `E_PARSE_AMBIGUOUS`，并携带声明与偏移。

月份值分别识别 ISO 年月、中文 `YYYY年M月`、英文 `Month YYYY` 和 `YYYY Month`。区间边界记录零基月序号、开闭标记、原始括号和区间类型；`至今/since` 等开放式语义显式保留。

## 阶段 2：精确单位与日历

所有数值均以字符串形式的 `Decimal` 处理，计算上下文精度为 60。每个类别的量化单位来自配置：

- 人口：`1`，规范单位 `person`
- 面积：`0.000000000001`，规范单位 `km2`
- 年份：`1`，规范单位 `year`
- 任期：`1` 个月，规范单位 `month_index`

换算诊断示例：

```json
{
  "coefficient": "1.38",
  "factor": "100000000",
  "factor_precision": 0,
  "factor_trace": [
    {"component": "unit", "factor": "1", "factor_precision": 0},
    {"component": "embedded_scale", "token": "亿", "factor": "100000000"}
  ],
  "quantum": "1",
  "precision_digits": 0,
  "rounding": "ROUND_HALF_EVEN",
  "recompute": {"expression": "quantize(1.38 * 100000000, 1)", "expected_result": "138000000"}
}
```

`factor_trace` 中的每个因子和精度可单独复算。面积平方英里因子固定为 `2.589988110336`，精度 12。缺失/未知单位、负值、倒置区间或开闭冲突产生 `review_issues`，声明仍保留在阶段输出与证据链中。

## 阶段 3：确定性裁决

同一 `entity_id` 下先按 `category` 分组，再按比较精度量化后的精确值分组。比较键包含类别、规范单位和精确值；月区间键包含起止月序号与开闭语义。浮点数不参与解析、换算或相等判断。

可信度在对数域计算：

```text
log_score = ln(source_prior)
          + ln(language_weight)
          + ln(0.5 ** (age_days / half_life_days))
          + ln(annotation)
score = exp(log_score)
```

输出保留每个分量的原值、对数、年龄天数和最终分数。默认仲裁顺序为：

1. `confidence_desc`
2. `source_priority_desc`
3. `language_priority_desc`
4. `freshness_desc`
5. `claim_id_asc`

仲裁顺序本身属于阶段 3 配置指纹。相同归一值的非获胜声明标记为 `EQUIVALENT_SUPPORT`；不同值的非获胜声明标记为 `CONFLICT_LOWER_CONFIDENCE`；异常归一结果标记 `REVIEW_REQUIRED`。

## 阶段 4：证据链

每条声明包含：

- `source_claim`：原始声明和摘要；
- `stage1`：规范化文本、数值/单位/日历片段、解析来源和摘要；
- `stage2`：规范值、换算因子链、精度、量化单位和摘要；
- `stage3`：裁决、置信度分解、比较值和仲裁理由；
- `links`：相邻阶段的摘要链接；
- `chain_digest`：整条声明证据链摘要。

实体证据记录裁决摘要和 `chain_digest`。清单根记录配置快照、配置摘要、语料摘要、四个阶段指纹、阶段内容摘要和清单摘要。

重导入校验会逐级从内容重算所有摘要，检查阶段间引用、链接、链摘要和清单摘要，并从 `stage3` 重建采纳/降级摘要。任一层缺失或摘要不符均返回 `E_EVIDENCE_BROKEN`，优先指出缺失阶段。

## 版本指纹

```text
stage1 = H(parser_version, normalization, parse, category_hints,
           numeric_scales, corpus_digest)
stage2 = H(stage1, unit_dictionary, quantization, calendar, fallback)
stage3 = H(stage2, language_weights, language_priority, source_priors,
           source_priority, freshness, comparison, arbitration)
stage4 = H(stage3, evidence)
config_fingerprint = H(stage1..stage4)
content_fingerprint = H(stage1..stage4 output content digests)
```

语料摘要基于按 `claim_id` 排序的规范 JSON 行，因此分片顺序和文件切分不影响指纹；内容变化一定影响。每次完整运行还会把各阶段输出内容摘要写入版本对象，并生成 `content_fingerprint`；清单校验会同时复算匹配配置/输入指纹与输出内容指纹。配置变更时按最先变化的阶段决定失效边界，旧检查点保留为 `invalid` 历史，新检查点替换当前有效项。

## 检查点协议

检查点文件是规范 JSON：

```json
{"seq": 17, "stage": "stage2", "entity_id": "ENT-ATLAS", "fingerprint": "...", "payload": {}}
```

写入顺序为：分配并持久化下一个 `seq`、原子写检查点文件、原子更新状态清单。清单更新失败时可能出现未索引的孤儿检查点，但不会被复用；索引存在而文件缺失或摘要不符时，检查点被标记失效并重建。停止请求文件也在实体边界以原子方式观察，不在实体四阶段中间截断。

若检查点文件不可读、包指纹不符或内容摘要不符，该阶段及同一实体的全部下游检查点立即失效。版本查询中的 `content_invalidations` 记录实体、断点阶段、原因与级联失效阶段；重算后的最终证据链仍与完整运行逐字节一致。

## 确定性与一致性

- 所有数值与配置权重以十进制字符串进入计算；浮点仅用于日志实现的 Python 3.10 兼容回退。
- 输出 JSON 使用 `sort_keys=True`、稳定分隔符、UTF-8 且关闭 ASCII 转义。
- 所有集合在序列化前显式排序；冲突分组不依赖输入分片或归约顺序。
- 证据清单不包含时间戳、绝对状态路径、检查点序号或运行序号等易变信息。
- 批量摘要不包含复用计数等执行状态；复用信息只出现在 `version_status()`。
- 停止/恢复会复用已提交检查点，新写入检查点序号继续单调递增，但最终裁决和证据与一次完整运行逐字节一致。
