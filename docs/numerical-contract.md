# 数值契约与误差传播

## 1. 表示与舍入

所有统计与配额计算使用 `Decimal` 局部上下文，precision 为 50，舍入模式为 `ROUND_HALF_EVEN`。JSON 中延迟、成功率、因子、概率均为字符串，避免 JSON 浮点重新引入二进制误差。

| 字段 | 小数位 |
|---|---:|
| 延迟微秒 | 6 |
| 成功率 | 12 |
| 配额因子 | 9 |
| 概率/置信度 | 9 或 6 |

配额缩放使用整数定点，`1_000_000` 表示 1.0；熔断权重使用 basis point，`10_000` 表示 1.0。

## 2. 窗口模型

- 窗口锚定 Unix epoch 毫秒。
- 固定长度 `window_length_ms`，滑动步长 `step_ms`。
- 边界为 `[start_ms, end_ms)`：下界包含，上界不包含。
- 时间戳分辨率 1 ms，耗时分辨率 1 μs。
- 对时间戳 `t`，所有满足 `s <= t < s + window_length_ms` 且 `s = k * step_ms + offset` 的窗口都包含该事件。
- 窗口只有在 `end_ms <= route_watermark_ms` 后才发布复合版本；水位为该路由已摄入事件的最大时间戳。
- 样本排序固定为 `timestamp_ms, batch_id, sequence, event_index`，重放稳定。

## 3. 分位

样本按耗时升序排列，`n` 为样本数。

最近秩：

```text
rank = ceil(p * n)       # 一基索引
value = x[rank - 1]
```

线性插值（R type 7）：

```text
position = (n - 1) * p
lower    = floor(position)
upper    = min(n - 1, lower + 1)
frac     = position - lower
value    = x[lower] + frac * (x[upper] - x[lower])
```

最终值按 6 位小数、`ROUND_HALF_EVEN` 输出。系统同时计算 P50/P95/P99 的两种方法，并输出：

- 每个分位的线性减最近秩差值；
- 每个分位的绝对偏差；
- 三者中的最大绝对偏差 `max_abs_deviation_us`。

## 4. 成功率与补偿求和

对事件 `i` 定义良好事件指标 `g_i ∈ {0,1}`。成功计数使用 Neumaier 补偿求和：

```text
sum, compensation = Neumaier(g_i)
success_rate = success_count / sample_count
```

0/1 指标在整数与 Decimal 下可精确表示，因此补偿项为 0；实现仍保留补偿项和误差界，以明确契约而非依赖隐式精度。成功率传播时读取 `exact_fraction = success_count/sample_count`，不会回读 12 位展示值。

成功率 95% 区间使用 Wilson score interval，所有平方根与除法在 Decimal 50 位上下文内完成。输出区间只说明比例估计不确定性，不替代业务 SLO。

## 5. 分位置信与误差界

对每个分位 `p`，系统输出 95% DKW 分布无关带。设 `α = 1 - confidence`：

```text
epsilon = sqrt(ln(2 / α) / (2n))
lower_probability = max(0, p - epsilon)
upper_probability = p + epsilon
lower_us = empirical_quantile(lower_probability)
upper_us = empirical_quantile(upper_probability)
```

当 `upper_probability >= 1` 时，上界取配置的耗时支持上界与当前最大样本中的较大值，并标记 `configured_support_max`。

每个分位输出：

- `statistical_width_us = upper_us - lower_us`；
- `method_spread_us = |linear - nearest_rank|`；
- `numeric_rounding_bound_us = 10^-6 μs`；
- `combined_upper_bound_us = statistical_width + method_spread + numeric_rounding`。
- `relative_method_spread` 与 `relative_combined_upper_bound`：以最近秩值为参考的相对量，用于比较不同耗时量级的窗口。

这是保守工程上界。它明确区分统计不确定性与数值舍入，不能解释为 SLA 承诺。

## 6. 样本不足

默认最小样本数：

| 分位 | 最小样本数 |
|---|---:|
| P50 | 2 |
| P95 | 20 |
| P99 | 100 |

不足时，窗口仍可提交并记录原始样本，但对应分位的 `value_us` 为 `null`，并附 `E_INSUFFICIENT_SAMPLES` 的业务码说明。严格统计查询或准入裁决直接返回该业务码，不会静默使用极不稳定的估计值。

## 7. 多窗口传播

配额推导读取最新最多三个已关闭窗口，默认权重为新到旧 `0.5, 0.3, 0.2`，不足三个时归一化实际权重。

- 成功率：对精确分数做加权 Neumaier 求和。
- 延迟：取所选窗口 primary 方法的 P95 最大值，保守传播。
- 每个被传播窗口都记录窗口版本、版本 ID、样本数、成功率、P95 与权重。

## 8. 自适应配额

定义：

```text
success_factor   = clamp(weighted_success_rate, 0, 1)
latency_factor   = clamp(target_latency_us / propagated_p95_us, 0, 1)
budget_factor    = clamp(budget_watermark, 0, 1)
quality          = 0.45*success_factor + 0.35*latency_factor + 0.20*budget_factor
base_quota       = floor(max_quota * quality)
concurrency_factor = clamp(1 - current_concurrency / target_concurrency, 0.10, 1)
latency_scale      = clamp(target_latency_us / propagated_p95_us, 0, 1)
scale_fixed        = round_half_even(clamp(concurrency_factor * latency_scale, 0, 1) * 1_000_000)
scaled_quota       = floor(base_quota * scale_fixed / 1_000_000)
breaker_adjusted   = floor(scaled_quota * breaker_weight_bp / 10_000)
```

熔断权重：

- `CLOSED = 10_000 bp`
- `HALF_OPEN = 2_500 bp`
- `OPEN = 0 bp`

半开探针目标至少为 1（若最大配额允许）。最终阻尼：

```text
delta = breaker_adjusted - previous_quota
if abs(delta) <= 1: result = previous_quota
else: result = previous_quota + round_half_even(delta * 2500 / 10000)
result = clamp(result, min_quota, max_quota)
```

每个中间步骤都输出 exact 值、显示值、边界和是否裁剪；`scale_fixed` 额外校验在 `0..1_000_000` 内。

## 9. 确定性检查

- 同一窗口样本集合在相同配置下重算，`stats_json` 规范化 JSON 逐字节一致。
- 输入顺序被打乱后，样本 digest 与统计结果不变。
- 配额相同输入重复推导，结果 JSON 完全一致。
- 窗口、熔断、配额版本均为不可变行；指针仅在同一事务内更新。

## 10. 熔断时间边界

系统评估时刻取已关闭窗口的 `end_ms`，或在限流裁决中使用请求的 `as_of_ms`。

- `CLOSED -> OPEN`：连续失败数达到阈值，或样本数达到下限且失败率达到阈值。
- `opened_at_ms` 取该窗口内最后一次失败事件的时间戳；没有失败时间时退回评估时刻。
- `next_probe_at_ms = opened_at_ms + open_duration_ms`。
- `OPEN -> HALF_OPEN` 使用包含下界：`evaluated_at_ms >= next_probe_at_ms`。
- `HALF_OPEN -> CLOSED`：探针时间后出现不少于阈值个连续成功，且窗口尾部没有失败。
- `HALF_OPEN -> OPEN`：探针时间后窗口尾部出现失败。

窗口边界仍是 `[start_ms, end_ms)`；熔断冷却边界是闭区间 `[next_probe_at_ms, +∞)` 的开始点。两者都在响应中显式输出。

