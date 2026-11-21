# 技术说明：确定性 CTR/CVR 训练与实验工作台

## 1. 设计目标

系统围绕四个可审计性质设计：

1. **可复现**：样本、遍历顺序、归约顺序、定点模型和摘要均确定。
2. **可中断**：每一轮训练结束都有原子进度记录，停止恢复不改变最终产物。
3. **可定位**：所有业务错误带稳定错误码、阶段、分片、行号和 `impression_id`。
4. **不静默修复**：标签异常、漏斗异常和数值异常不会被截断、覆盖或吞掉。

## 2. 模块边界

| 模块 | 职责 |
|---|---|
| `generator.py` | 固定种子生成 JSONL 分片与清单，仅 stdlib + numpy |
| `data.py` | 清单解析、只读观测、行解析、标签/漏斗标签校验、真值比对 |
| `features.py` | NFKC/空白/小写归一化、固定哈希、数值分箱、缺失指示、特征指纹 |
| `config.py` | YAML 配置、参数校验、行为配置版本指纹 |
| `scheduler.py` | 清单驱动的分片接纳、预算与配额约束 |
| `model.py` | 对数域双目标 LR、ESMM 联合项、定点量化、worker 与确定性归约 |
| `training.py` | 训练矩阵、哈希遍历顺序、打分记录、目标指标与 AUC |
| `funnel.py` | 标签关系与模型分数漏斗校验、诊断产物 |
| `checkpoints.py` | 单调序号、原子写入、阶段签名、失效与恢复记录 |
| `pipeline.py` | 四阶段编排、缓存复用、停止恢复和实验摘要 |
| `cli.py` / `server.py` | 本地 CLI 与 HTTP 工作台 |

## 3. 特征装配

类别值归一化：

```text
NFKC -> 所有连续空白折叠为一个空格 -> 去首尾空白 -> lower()
```

固定哈希：

```text
token_bytes = f"{field}={normalized_value}" 或 f"{field}#bin={index}"
index = int(BLAKE2b(token_bytes, digest_size=8, person="ctrcvr01")) mod feature_dimension
```

数值上界规则为 `bisect_right`：值等于边界时进入右侧分箱。每个数值字段还记录命中的 `bin_index`。若期望字段缺失或数值非法，追加：

```text
__MISSING__|<field>
```

因此缺失不会被当成“值不存在”。哈希碰撞会产生同 index 的计数累加；特征矩阵每行是稀疏 token 的稠密计数向量。每行指纹覆盖 schema、维度、indices、values、bins、missing 和 token 顺序；分片和全产物再逐层聚合 SHA256。

## 4. 对数域双目标模型

CTR 与 CVR 各自是逻辑回归：

```text
z_ctr = w_ctr · x + b_ctr
z_cvr = w_cvr · x + b_cvr
log p_ctr = log_sigmoid(z_ctr)
log p_cvr = log_sigmoid(z_cvr)
log p_conv = log p_ctr + log p_cvr
```

`log_sigmoid` 使用分支：

```text
z >= 0: -log1p(exp(-z))
z <  0: z - log1p(exp(z))
```

`log(1-exp(a))` 使用 `log1mexp` 的稳定分支，不使用 `log(clip(p))`。

目标函数（每个分片的局部和，最终按全局样本量取均值）：

```text
L_ctr  = BCE(y_ctr, log p_ctr)                 # 所有曝光
L_cvr  = BCE(y_cvr, log p_cvr)                 # 仅 ctr=1 且 CVR 有观测
q      = p_ctr * p_cvr
L_join = BCE(y_conv, log(q))                   # 负曝光目标 0
L      = mean(L_ctr) + mean(L_cvr) + λ * mean(L_join)
```

对于联合项，`q` 不是单个 sigmoid，因此梯度显式计算。令 `t` 为转化目标：

```text
t = 1: coefficient = -1
t = 0: coefficient = q / (1-q)
∂L_join/∂z_ctr = coefficient * (1-p_cvr)
∂L_join/∂z_cvr = coefficient * (1-p_ctr)
```

worker 分别返回 CTR 梯度、CVR 梯度、联合项对两个头的梯度；中心按照各自样本量求均值后组合，避免把不同分母的损失错误混算。

## 5. 定点量化

每轮更新后执行对称 round-half-away-from-zero：

```text
q = sign(v) * floor(abs(v) * scale + 0.5)
```

最终模型只保存 `int64` 权重/偏置和整数缩放因子。轮次进度也保存定点参数，因此停止恢复从完全相同的参数继续。默认 `scale=1_000_000`。

## 6. 确定性遍历与归约

每个 epoch 的样本顺序：

```text
key = SHA256(f"{seed}|{epoch}|{shard_id}|{impression_id}")
按 (key, 原位置) 排序
```

该顺序不依赖 Python 随机哈希、numpy 随机洗牌或操作系统调度。

训练与推理均按分片提交本地进程池。中心收集每个分片结果后按 `shard_index` 排序，再以固定顺序累加：

- 损失局部和；
- 梯度局部和；
- 曝光/CVR 观测/联合项样本数。

顺序模式调用同一 worker 函数，仅不创建进程池，因此归约结果与并行模式一致。

worker 内所有异常都转换为 `E_WORKER_FAILED` 对象；中心把已完成分片的损失、梯度、计数完整序列化到 `worker_errors.json`，保留已完成结果。

## 7. 分片接纳

接纳只读取清单声明的行数，不先扫描样本：

```text
cost_i = min(declared_rows_i, quota) * epochs
accept if consumed + cost_i <= global_budget
```

被拒绝分片不消耗预算，记录 `unprocessed` 原因。`workers_effective = min(max_workers, accepted_count)`。

## 8. 漏斗一致性

校验项：

1. `cvr_label` 不得出现在 `ctr_label=0`。
2. `log p_cvr <= log p_ctr`。
3. `log p_conv` 必须有限、`<= 0` 且 `<= log p_ctr`。
4. 阈值一致性：`log p_conv >= log threshold` 时必须有 `log p_ctr >= log threshold`。

违规对象包含 `code`、`check`、`violated_value`、`shard_id`、`row_number`、`impression_id`。诊断先原子落盘，再抛出业务错误，因此失败运行也保留审计证据。

## 9. 检查点模型

检查点文件名是 8 位单调序号。每个最终记录由临时文件 `fsync` 后一次 `os.replace` 产生，随后尽力 `fsync` 目录。记录字段：

```text
seq, kind, stage, status, stage_signature, config_version,
shard_input_fingerprint, artifact_digest, artifact_path, details
```

`stage_signature` 控制复用粒度：

```text
features = H(format, schema_fingerprint, data_content_fingerprint)
train    = H(feature_artifact_digest, seed, training_config, budget, quota, accepted_shards)
funnel   = H(model_digest, feature_digest, threshold, probability_policy)
infer    = H(model_digest, feature_digest, funnel_digest, threshold, score_format)
```

`config_version` 是整个行为配置的指纹，但阶段复用以 `stage_signature` 为准，因此只改学习率不会重做特征。失效记录包含最早阶段和全部下游阶段；后续完成记录序号更大时自动恢复 valid。

## 10. 停止与恢复

停止请求只设置协作标志。训练 worker 不会被强杀，当前 epoch 完成后：

1. 写 `stage_progress` 产物（状态 `stopped`）；
2. 记录已完成 epoch 和定点参数；
3. 返回 `stopped`，不继续漏斗/推理。

恢复时若进度签名与当前 train 签名一致，从下一 epoch 继续。因为每轮参数已定点量化并持久化，最终模型/损失/打分数值与不间断运行一致。

## 11. 可复现摘要

`experiment_summary.json` 只包含行为相关输入和确定性产物摘要，不包含时间、输出绝对路径、进程数或检查点序号。同配置、同数据、同代码下，摘要字节完全一致。AUC 使用排序后 Mann–Whitney 秩和并对同分样本取平均秩。

## 12. 需求映射

| 任务要求 | 实现与验证 |
|---|---|
| 特征归一化、哈希、分箱、显式缺失、指纹 | `features.py`；单测 `test_normalization_and_missing...` |
| 对数域双目标训练、CVR 曝光掩码、ESMM 联合 | `model.py`、`training.py`；损失曲线与模型产物 |
| 漏斗一致性与不修正/不截断 | `funnel.py`；漏斗合成违规单测 |
| 预算、配额、并发三重约束 | `scheduler.py`；预算单测与默认未处理分片 |
| 并行确定性归约、worker 结构化错误 | `model.execute_tasks`；并行一致性与 worker 单测 |
| 异常注入与稳定错误码 | `Pipeline.injection`、CLI/HTTP；6 类注入测试 |
| 原子检查点、配置版本、增量失效 | `checkpoints.py`、`pipeline.py`；序号/无临时文件与失效传播测试 |
| CLI/HTTP、导入、停止恢复、导出、摘要 | `cli.py`、`server.py`、静态页面；HTTP 集成测试与演示 |
| 两次运行与停止恢复字节一致 | `scripts/verify.py`、`artifacts/demo/report.json` |
