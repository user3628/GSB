# 架构与不变量

## 模块边界

| 模块 | 职责 |
|---|---|
| `data.py` | 读取并校验权重/校准张量，生成内容指纹 |
| `config.py` | YAML 归一化、层覆盖、预算/归因参数校验 |
| `quantization.py` | 单张量仿射量化、反量化、钳制审计 |
| `engine.py` | 逐层前向、误差采集、双路归因、预算、增量复用与核验 |
| `store.py` | 哈希链审计记录、阶段归档、锁、事务恢复、回滚 |
| `workspace.py` | 导入、校准编排、查询、历史与活动版本 |
| `cli.py` / `demo.py` | CLI、确定性样本、完整演示与故障注入 |

## 前向与误差

权重按 `(out, in)` 存储。第 `l` 层：

```text
ref_out[l]  = ref_in[l]  @ W[l].T  + b[l]
local_out[l]= ref_in[l]  @ Wq[l].T + bq[l]
quant_out[l]= quant_in[l]@ Wq[l].T + bq[l]
local_noise[l] = local_out[l] - ref_out[l]
output_noise[l]= quant_out[l] - ref_out[l]
```

每层记录绝对平均/最大误差、相对 L2、相对最大误差、局部误差以及显式绝对上界。局部参数噪声上界使用样本绝对值矩阵计算；传播上界按量化权重行绝对和递归累积：

```text
bound_local = max_n || |dW| @ |ref_in[n]| + |db| ||_inf
bound_prop  = bound_local + ||Wq||_inf * bound_prev
```

所有计算只依赖已加载的全精度参考权重/偏置、量化参数和本地校准样本，不做缺失值推断。

## 归因

一阶敏感度将局部噪声沿下游全精度权重传播。扰动重放定义前缀输出 `P[k]`：前 `k` 层使用量化参数，其余层使用全精度参数。于是：

```text
P[0]   = full precision end-to-end output
P[L]   = fully quantized end-to-end output
D      = P[L] - P[0]
delta[k] = P[k+1] - P[k]
D      = sum(delta[k])                  # 望远镜闭合
```

系统分别计算一阶与重放 `delta` 的 Frobenius 范数、排名和分数。排序不同的层标记 `needs_review`，并说明一阶方法忽略该层噪声与下游量化的高阶交互。端到端 L2 偏差按重放分数分摊，`sum(contribution_l2) == end_to_end_l2_error`。一阶或重放的向量闭合相对间隙超过 `closure_tolerance` 时返回 `E_ATTRIBUTION_GAP`。

## 预算

每层误差指标由 `budget.metric` 决定，占用率为 `error / budget`。总预算对端到端指标做同样判定。最近预算层优先选择“未越界且占用率最高”的层；若全部越界，则选择占用率最低的层，并输出正/负余量。越界层若上游归因贡献占比达到阈值，诊断会明确说明单纯提高该层位宽不能解决，并建议先处理上游或校准集。

## 增量指纹

一次运行的数值依赖拆分为：

```text
calibration_fingerprint = digest(selected sample bytes + shape)
layer_fingerprint[i]    = digest(weight bytes + bias bytes + bits + mode + zero_point)
config_fingerprint      = digest(normalized full YAML semantics)
budget_fingerprint      = digest(normalized budget section)
```

若层序或校准选择内容变化，最早受影响层为第 0 层；否则取首个层指纹不一致的索引。预算/归因参数变化不影响前向数值，可复用全部层结果但重新判定预算。增量运行从上一版本阶段归档加载前缀张量，仅执行最早变化层及下游。为满足审计不变量，引擎始终在内存中执行一次从零全量影子运行，比较每层结果与关键张量摘要；差异层返回 `E_INCREMENTAL_MISMATCH`，该核验不能被配置关闭。

## 持久化状态机

```text
stages/{version}.npz     确定性 NPZ 阶段张量
records/{version}.json   完整记录 + record_hash 哈希链
HEAD.json                最大版本、最新记录哈希、活动版本
TX.json                  发布事务标记
lock                     跨进程排他锁
../audit.anchor.json     工作区根目录外部锚点
```

发布顺序：

1. 在锁内验证历史与基线版本。
2. 原子写阶段 NPZ。
3. 原子写事务标记。
4. 原子写记录。
5. 原子写 HEAD 与外部锚点。
6. 删除事务标记。

恢复规则：事务存在且记录缺失时丢弃事务；记录完整但 HEAD 尚未推进时，验证阶段摘要后完成提交；HEAD 已推进时补齐锚点并清理事务。没有事务却出现超出 HEAD 的记录、缺少历史版本或 HEAD 落后外部锚点，均判定为版本回退。

哈希链约束：

- 每条记录的内容哈希包含除 `record_hash` 外的全部字段。
- 下一记录的 `prev_record_hash` 必须等于上一记录的 `record_hash`。
- HEAD 的 `record_hash` 必须等于最新记录哈希。
- 外部锚点版本与记录哈希必须与 HEAD 一致；允许“HEAD 比锚点新一版且前序哈希匹配”的中断恢复。

预算越界记录仍原子发布，但 `usable=false`；HEAD 的活动版本回退到历史中最近一个 `usable=true` 的版本。历史最大版本不回退，因此后续校准仍连续递增。手动回滚只移动活动指针，不删除或重写历史结果。

## 确定性

- 样本生成仅使用固定 `numpy.random.default_rng(seed)`。
- JSON 使用排序键、紧凑分隔符和禁止 NaN/Inf。
- NPZ 使用排序键、固定 ZIP 时间和 `ZIP_STORED`。
- 浮点数组以 float64、C 顺序计算与摘要。
- 记录不放墙钟时间或绝对工作区路径，结果/HISTORY 摘要可重复。

