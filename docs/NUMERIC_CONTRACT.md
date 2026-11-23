# 数值契约

## 1. 表示

- 成功率、绝对差异、相对差异、置信区间半宽和误差上界均以 ppm 整数表示，`1 ppm = 1e-6`。
- 展示字符串固定 6 位小数，例如 `970000 -> "0.970000"`。
- 耗时单位固定为微秒整数，分位只输出最终微秒整数。
- 审计中的残差和累计误差使用精确有理数 `numerator/denominator` 保存，并附 9 位小数展示值。
- 所有最终舍入为 `round_half_even`。

## 2. 不重用舍入值

规范路径：

```text
integer counts -> exact rational ratios -> one final half-even rounding per output
```

成功率、差异、相对差异由原始整数计数计算。响应中的 `success_rate_ppm` 只用于展示，不会被再次相减或相除。门禁比较使用由计数直接计算并封存在同一快照中的估计值。

## 3. 成功率与差异

```text
success_rate = round_half_even(successes / samples * 1_000_000)
absolute_difference = round_half_even((cs/cn - ss/sn) * 1_000_000)
relative_difference = round_half_even(((cs/cn - ss/sn) / (ss/sn)) * 1_000_000)
```

其中 `cs/cn` 为 canary 成功数/样本数，`ss/sn` 为稳定组成功数/样本数。

- `absolute_difference_ppm` 与 `relative_difference_ppm` 保留符号：正值表示 canary 更好。
- `abs_success_drop_ppm = max(0, -absolute_difference_ppm)`。
- `relative_success_drop_ppm = max(0, -relative_difference_ppm)`。
- 门禁使用下降方向的上界，避免 canary 优于稳定组时误报。

## 4. 置信区间与误差上界

置信水平 `confidence_level_ppm` 例如 `9500` 表示 95%。`z_ppm` 是双侧正态分位数，使用固定有理多项式近似并最终舍入到 6 位。

```text
ci_half_width = round_half_even(z * sqrt(pc(1-pc)/nc + ps(1-ps)/ns) * 1_000_000)
error_bound   = round_half_even(z * sqrt(0.25/nc + 0.25/ns) * 1_000_000)
abs_drop_upper = abs_success_drop + ci_half_width
relative_ci_half_width = round_half_even(ci_half_width / stable_rate * 1_000_000)
relative_drop_upper = relative_success_drop + relative_ci_half_width
```

`error_bound_ppm` 不是常量：每次统计都根据 `nc`、`ns` 和 `z_ppm` 重新计算。平方根使用整数比较和 `big.Int.Sqrt` 完成半偶舍入，不依赖平台浮点实现。当前 200/200 样本、95% 置信水平的结果为 `97998 ppm`。

每组少于 2 个样本时无法给出比例置信区间；线性插值分位也需要至少 2 个样本。此类情况返回 `E_INSUFFICIENT_SAMPLES`，不会输出静默零值。

## 5. 分位

排序键固定为：

```text
latency_us, stage, timestamp_ms, instance_id, group, batch_id, seq
```

### nearest_rank

```text
sorted[ceil(q_per_mille * n / 1000) - 1]
```

### linear

```text
pos = q_per_mille * (n - 1)
lower = floor(pos / 1000)
rem = pos mod 1000
value = sorted[lower] + (sorted[lower+1] - sorted[lower]) * rem / 1000
```

最终 `value` 以精确有理数半偶舍入为微秒整数。

跨阶段累计分位使用所有阶段原始延迟样本，按上述稳定键归并后计算；因此与批次提交顺序无关。

## 6. 计数累计

阶段按阶段号升序处理。每组样本数、成功数使用受检整数累计；`CompensatedCount` 保留 Neumaier 补偿槽，整数计数无溢出时补偿恒为 `0`。任何 int64 溢出都会使整批无效。

## 7. 门禁比较

阈值比较使用整数定点，不使用裸浮点：

```text
tolerance_applied = round_half_even(threshold * relative_tolerance_ppm / 1_000_000)
limit = threshold + tolerance_applied
crossed = measured > limit
```

等于容差边界时视为未越过门槛。固定比较次序：

1. `samples.min_observation` -> `E_OBSERVE_MORE`
2. `confidence.max_ci_half_width` -> `E_OBSERVE_MORE`
3. `success.absolute_drop` -> `E_GATE_FAILED`
4. `success.relative_drop` -> `E_GATE_FAILED`
5. `latency.drop` -> `E_GATE_FAILED`

前两个条件表示证据不足，不会误报为确定性回归。条件相同时由固定次序打破平局。

## 8. 审计输出

每个计算版本包含：

- `success_rate_rounding_step` / `quantile_rounding_step`：输入有理数、scale、输出单位、输出值、残差和单步上界。
- `success_rate_audit` / `quantile_audit`：该指标族的舍入步骤、累计比例误差与累计延迟误差。
- `stage_error_contract` / `cumulative_error_contract`：样本规模、z 值、公式、运行时 CI 半宽、运行时误差上界、门禁阈值和全量审计。

同一观测集合和同一版本参数重复计算会生成逐字节相同的快照 JSON（时间戳参数也相同时）。

## 9. 当前样本结果

| 指标 | 阶段 1 | 阶段 2 |
|---|---:|---:|
| canary 样本/成功 | 200 / 194 | 200 / 155 |
| stable 样本/成功 | 200 / 190 | 200 / 190 |
| canary 成功率 | 0.970000 | 0.775000 |
| stable 成功率 | 0.950000 | 0.950000 |
| 绝对成功率下降 | 0 | 175000 ppm |
| 相对成功率下降 | 0 | 184211 ppm |
| CI 半宽 | 38357 ppm | 65281 ppm |
| 绝对下降上界 | 38357 ppm | 240281 ppm |
| 运行时误差上界 | 97998 ppm | 97998 ppm |
| 裁决 | `PROMOTE` | `E_GATE_FAILED` |
