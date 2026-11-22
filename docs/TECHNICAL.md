# 技术设计

## 1. 模块边界

| 模块 | 职责 |
| --- | --- |
| `config.py` | YAML 归一化、阈值/预算校验、点号键修改 |
| `generator.py` | 固定种子样本、manifest 和默认 YAML |
| `dataset.py` | manifest 解析、CSV 读取、声明冲突校验、分档全局 schema |
| `encoding.py` | 独热/目标/哈希编码、缺失填充、目标编码诊断 |
| `selection.py` | 方差、互信息、RFE 三种选择器和评分 |
| `budget.py` | 成本估算、三重预算接纳、lane 分配、预算账本 |
| `checkpoints.py` | 原子 JSON 记录、真实文件锁、序号/重复/依赖校验 |
| `engine.py` | 四阶段编排、进程池波次、停止恢复、失效复用、导出 |
| `cli.py` / `http_server.py` | 本地交互入口 |
| `util.py` | canonical JSON、摘要、原子写、跨平台锁、数组摘要 |

## 2. 数据契约与类型推断

manifest 的每个文件项至少包含 `file`、`tier`、`rows`、`columns`、`label_column`、`missing_columns`、`categorical_columns`。生成器还写入 `column_names`、`column_types` 和内容 SHA-256。

类型推断分两步：

1. 按声明读取每列类型，并检查行数、列数、列名、类别基数和标签 0/1。
2. 读取每个分片的所有非空值，检查数值可解析性、类别观测基数、类别域和缺失统计。

数值声明遇到非数值时返回 `E_COLUMN_MISMATCH`，错误详情包含 `column`、`declared: "numeric"` 和实际观测值。类别基数冲突同样返回双方的值与基数。同一档内各分片必须列布局一致。

推断结果是“档级全局 schema”：类别域与缺失统计来自该档所有分片。这样即使某个分片恰好没有某类缺失值，独热列仍与同档其他分片完全对齐。

## 3. 编码与目标编码防泄漏

### 缺失填充

- 数值：`mean`、`median`、`mode`、`zero`、`constant`。
- 类别：`mode`、`constant`。
- 每个分片摘要记录填充方法与实际填充个数。

### 独热

类别列按全局排序域展开为 `ohe:<列>=<值>`。若该档存在缺失，则填充常量作为额外域值参与编码。

### 哈希

使用固定参数 `blake2b(value, person=b"featwb")` 生成桶号，避免 Python 字符串哈希随机化。每个类别列固定产生 `hash.n_features` 列。

### 目标编码

每列按 `fold_seed` 生成固定排列，再均分为配置折数。对每一折：

1. 训练集是除该验证折外的所有行。
2. 类别估计为 `(sum_label + smoothing * prior) / (count + smoothing)`。
3. `prior` 是当前训练折的全局标签均值，不含验证折标签。
4. 训练折不可见的类别回退到该 `prior`。
5. 若任一训练折只有单一类别，返回 `E_FOLD_INVALID`。

每行只使用其验证折对应的训练统计。诊断中保留折分配、每折先验、每类别在各折的计数/标签和/编码值，因此同一类别跨折差异可以直接检查。诊断中的回退字段名为 `current_training_fold_global_mean`，表达的是“当前训练折的全局均值”，从而避免当前行标签泄漏。

## 4. 特征选择

- `variance`：总体方差 `var(X, ddof=0)`，`score >= threshold` 保留。
- `mutual_info`：对特征做确定性的至多 16 桶离散化，再计算与二分类标签的经验互信息。小样本时桶数受 `sqrt(n)` 限制，避免每个样本独占一桶造成虚假高 MI。
- `rfe`：对标准化特征和中心化标签构造 Gram 矩阵，使用带极小 ridge 的线性系数绝对值作为重要性，每一步淘汰最小者，直到保留 `ceil(threshold * feature_count)` 个。

选择发生在编码后、所有已接纳分片按 `shard_index` 拼接后的特征空间。`selected`、`eliminated` 和 `ranking` 均保留编码列顺序或稳定排序规则。导出矩阵只含 `selected` 列。

阈值规则：

- 方差、互信息：有限且 `>= 0`。
- RFE：`0 < threshold <= 1`。
- 越界返回 `E_INVALID_THRESHOLD`。

## 5. 预算调度

每个分片预估：

```text
rows * max(1, numeric_columns + sum(categorical_cardinality))
```

接纳顺序固定为全局 `shard_index`。每个分片先检查单进程配额，再检查全局余额，然后按 first-fit 装入 lane；lane 数不超过 `max_workers`。不满足约束的分片不消耗预算，其理由写入 `unprocessed`。

`BudgetLedger.charge()` 是全局预算的最后防线：任何导致累计值超过 `global_feature_evaluations` 的申请都会返回 `E_BUDGET_EXCEEDED`，且不修改账本。

进程池的并发波次大小取 `min(max_workers, 实际 lane 数, 待处理数)`。每个波次完成后按分片序号写检查点，避免完成顺序引入非确定性。

## 6. 检查点与恢复

记录文件示例：

```json
{
  "sequence": 7,
  "record_id": "encoding:STAGE:<stage-version>",
  "stage": "encoding",
  "scope": "STAGE",
  "pipeline_version": "<full-version>",
  "stage_version": "<stage-version>",
  "input_fingerprint": "<digest>",
  "config_version": "<full-version>",
  "artifact_digest": "<digest>",
  "dependencies": [{"stage":"encoding","stage_version":"...","scope":"shard:0"}]
}
```

完整从零运行产生 11 条记录：类型推断 1、分片编码 5、编码阶段 1、分层选择 2、选择阶段 1、聚合 1。停止后恢复只追加缺失单元，序号继续单调递增。

原子性：

- 临时文件位于目标目录。
- 写入后 `flush` + `fsync`。
- `os.replace` 原子替换。
- POSIX 额外 `fsync` 父目录。

发布保护：

- `CheckpointStore.publish()` 在 OS 文件锁内扫描当前最大序号。
- 传入的 `expected_sequence` 不匹配、记录 ID 重复、依赖缺失或同版本阶段回退均返回 `E_STALE_CHECKPOINT`。
- 引擎每次运行只从一个预期序号开始；外部发布者抢先写入时，本次发布失败而不是覆盖。

## 7. 版本映射

阶段摘要按依赖链计算：

```text
type_inference = H(manifest + 所有分片内容 SHA-256)
encoding       = H(type_inference + encoding 配置 + seed + 实际接纳分片集合)
selection      = H(encoding + selection 配置)
aggregate      = H(selection + budget 配置 + 完整调度计划)
full_version   = H(四个阶段摘要)
```

因此：

- 样本内容变化：最早失效 `type_inference`。
- 编码策略、缺失填充、目标平滑或折种子变化：最早失效 `encoding`。
- 选择方法或阈值变化：最早失效 `selection`。
- 只改变查询延迟等不影响接纳集合的预算字段：上游全部复用，最早失效 `aggregate`。
- 预算变化导致接纳分片集合变化：最早失效 `encoding`。

`versions.json` 保存最近一次完整聚合的基线。运行状态只查询与当前 `stage_version` 匹配的记录，因此旧版本记录保留审计价值但不会进入当前结果。

## 8. 错误码

| 代码 | HTTP | 触发场景 |
| --- | ---: | --- |
| `E_COLUMN_MISMATCH` | 422 | 声明与观测的类型、列名、类别基数等冲突 |
| `E_FOLD_INVALID` | 422 | 目标编码训练折只有单一类别或折数大于行数 |
| `E_INVALID_THRESHOLD` | 422 | 选择阈值越界 |
| `E_BUDGET_EXCEEDED` | 409 | 预算账本拒绝超额申请 |
| `E_STALE_CHECKPOINT` | 409 | 落后/重复/跨阶段错位/依赖缺失的发布 |
| `E_WORKER_FAILED` | 500 | 工作进程异常；详情保留成功分片 |
| `E_CONFIG_INVALID` | 422 | 配置结构或枚举无效 |
| `E_INPUT_INVALID` | 422 | CSV/manifest/body 等输入无效 |
| `E_STATE_INVALID` | 409 | 检查点或产物损坏/缺失 |
| `E_RUN_IN_PROGRESS` | 409 | HTTP 服务已有后台运行 |

## 9. 已知边界

- 哈希编码使用桶内独热，不做签名哈希的计数合并。
- 互信息是确定性的经验估计，不等同于某个第三方库的特定实现。
- 目标编码按分片独立生成折；统计只作用于本分片。
- 服务默认无认证，只应绑定本机回环地址。
- 类型推断需要读取全部声明分片的非空值；这不计入“特征评估预算”，因为预算面向编码/选择单元。

