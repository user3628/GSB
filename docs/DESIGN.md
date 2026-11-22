# 技术设计

## 模块边界

- `models.py`：计划规范化、版本指纹、档位与阈值校验。
- `fixedpoint.py`：精确小数解析、整数定点量化、相对容差比较。
- `quantiles.py`：最近秩与线性插值的精确有理数实现。
- `verdict.py`：吞吐、错误率、分位、阈值裁决、P99 双方法差异、舍入账本。
- `sampler.py`：有界并发 HTTP 请求、独立超时与连接关闭。
- `engine.py`：单写者轮次推进、重测与拐点决策。
- `repository.py`：SQLite 事务、结论版本、不可变结论、意图记录、启动恢复。
- `server.py`：本地面向用户的 JSON HTTP API。
- `deterministic_service.py`：固定种子延迟生成、错误注入、挂起超时、并发统计。

## 计划与版本

每个 `plan_id` 每次成功提交生成递增 `version`。计划语义与版本、提交时间、SHA-256 指纹一起持久化。合法提交才会创建新版本；校验失败在事务之前返回 `E_PLAN_INVALID`，不会修改已有版本。

比例阈值和档位中的比例输入由 `numeric_contract.ratio_decimals` 声明定点小数位，服务端库存为整数，例如错误率 `0.010000` 以 `10000` 存储。延迟和吞吐分别由 `latency_decimals`、`throughput_decimals` 声明。

## 轮次状态机

```text
PENDING --sampling intent committed--> SAMPLING
PENDING --abort/recovery-------------> ABORTED
SAMPLING --atomic sample+conclusion--> CONVERGED
SAMPLING --failure/recovery----------> ABORTED
CONVERGED / ABORTED                  terminal
```

轮次记录 `plan_id`、`plan_version`、`plan_fingerprint`、`run_id`、`stage_key`、`attempt_no`、`kind`、`quantile_method`。状态迁移记录也携带 `plan_id` 和 `plan_version`。同一计划重测通过新的 `run_id` 和 `round_id` 区分。

`CONVERGED` 不允许通过通用迁移 API 写入，只能由样本聚合与结论在同一事务中提交。`round_samples` 和 `round_conclusions` 均有 SQLite 触发器禁止更新和删除。引擎重放已通过档位时只读取原结论。

## 事务与并发

开始轮次时，事务内依次写入 `write_intents(IN_PROGRESS)`、`PENDING` 轮次、`SAMPLING` 迁移，然后提交。采样发生在内存中；完成后另开 `BEGIN IMMEDIATE` 事务，原子写入：

1. 全部样本与聚合；
2. 判定结论和全局递增 `conclusion_version`；
3. 轮次 `CONVERGED`；
4. 意图 `SUCCEEDED`。

因此读者不可能看到“样本已提交、结论未提交”的中间态。`rounds(plan_id) WHERE status='SAMPLING'` 部分唯一索引保证每个计划至多一个采样轮次。进程内同一 `run_id + stage_key` 使用事件协调：首个调用执行，重复调用等待并复用同一持久化结果。其他档位在采样中返回 `E_ROUND_ACTIVE`。

只读接口使用单个 SQLite 读事务和 WAL 快照。`CONVERGED` 轮次返回全局 `conclusion_version`、样本摘要、完整样本、指标、阈值检查、插值差异和计算指纹。

## 分位和阈值

延迟样本以整数微秒落库。最近秩和线性插值都使用 `Fraction`，量化为声明小数位的定点整数后才参与裁决。

```text
nearest_rank_rank = ceil(p * sample_count)
linear_position   = (sample_count - 1) * p
```

P99 至少需要 `100` 个样本。`min_samples` 可进一步提高要求。样本数不足返回 `E_INSUFFICIENT_SAMPLES`，轮次和运行被标记为 `ABORTED`。

阈值比较：

```text
allowance = ceil(threshold_fixed * relative_tolerance_ppm / 1_000_000)
upper_boundary = threshold_fixed + allowance
lower_boundary = threshold_fixed - allowance
```

延迟和错误率要求实际值不高于上界；吞吐要求实际值不低于下界。比较输入均为整数。

比例账本按批次执行：

1. `round(errors_i / samples_i * scale)`；
2. `round(samples_i / total_samples * scale)`；
3. `round(rate_fixed_i * weight_fixed_i / scale)`；
4. 累计定点贡献，并与“总错误/总样本只舍入一次”的等效参考比较。

账本输出每步输入、舍入方式、输出定点值、舍入差、累计误差定点值与 ppm。主裁决采用精确比例的“一次量化”定点值，相对容差吸收边界量化差。

## 吞吐定义

为消除线程调度和本地网络抖动对裁决翻转的影响，裁决吞吐采用闭环派生值：

```text
throughput_rps = successful_requests * concurrency * 1_000_000 / sum_latency_us
```

实际墙钟秒数保存在诊断字段中，但不出现在判定计算指纹中。重复相同计划、档位、种子和样本规模可得到逐条一致样本、相同吞吐与相同判定。

## 决策与拐点

- `STOP`：首个否决档即停止。
- `RETURN_LAST_PASS`：停止并回退到最后通过档。
- `STEP_DOWN_RETEST`：在最后通过并发与否决并发之间按 `downshift_fraction_ppm` 计算整数并发，重测一次。

重测轮次使用独立的 `stage_key`、`attempt_no` 和 `round_id`。重测通过时拐点为 `BETWEEN_PASS_AND_REJECT`；重测否决时按规则停止并指向最后通过档或首否决档。完整决策依据保存在 `runs.decision_basis_json`。

## 恢复

启动时扫描所有 `write_intents.status='IN_PROGRESS'`。对关联的 `PENDING/SAMPLING` 轮次执行到 `ABORTED` 的合法迁移，并把运行标记为 `ABORTED`（错误码 `E_PROCESS_INTERRUPTED`）。恢复报告持久化在 `recovery_reports`，包含：

- 未完成意图数量；
- 被中止轮次及中止前状态；
- 已持久化样本行数、样本数和摘要；
- 已持久化结论；
- 可重算轮次列表；
- `unsaved_samples_recovered=false` 与明确披露。

强制终止时仍在采样线程内存中的样本没有数据库依据，报告不会声称恢复。恢复只读取 SQLite 已提交事实。

## 数据表

- `plans` / `plan_versions`
- `runs`
- `rounds`
- `round_samples`
- `round_conclusions`
- `round_transitions`
- `write_intents`
- `recovery_reports`
- `meta`

