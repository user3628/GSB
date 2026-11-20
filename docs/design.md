# 设计与算法说明

## 1. 处理流水线

一个批次在单写者闸门内按以下顺序执行：

1. 规范化事件并按 `(timestamp_ms, event_seq, request_id)` 排序。
2. 从同一个已提交快照 `read_version` 加载令牌注册表、客户端配额组、窗口事件、配额状态。
3. 鉴权。只有鉴权通过的事件进入窗口模型；鉴权失败只写裁决，不写窗口事件。
4. 对通过鉴权的请求按事件时间顺序执行短窗与长窗估计、动态配额和限流裁决。
5. 先提交 `PENDING` 意图记录，再在单个 SQLite 事务中提交窗口事件、桶、配额、裁决、批次状态和新快照版本。
6. 查询通过读写闸门读取某个完整提交版本；多个只读请求可并发。

## 2. 鉴权规则

固定本地密钥为 HMAC-SHA256 密钥，签名输入是以下 claims 的规范化 JSON：

```text
token_id, client_id, quota_group, expires_at_ms, active, issued_at_ms
```

判定顺序固定为：

1. 令牌存在；
2. 令牌处于 active 状态；
3. 令牌所属客户端与事件客户端一致；
4. HMAC 摘要常量时间比对一致；
5. 客户端存在配额组归属；
6. 令牌配额组等于客户端配额组；
7. `timestamp_ms <= expires_at_ms`。

因此：

- `timestamp_ms == expires_at_ms` 有效；
- `timestamp_ms == expires_at_ms + 1` 返回 `E_TOKEN_EXPIRED`；
- 摘要错误、未知令牌、撤销令牌、客户端或配额组不匹配返回 `E_AUTH_DENIED`；
- 每条失败裁决保留 `TOKEN_NOT_FOUND`、`SIGNATURE_MISMATCH`、`TOKEN_INACTIVE`、
  `TOKEN_CLIENT_MISMATCH`、`CLIENT_GROUP_NOT_FOUND`、`QUOTA_GROUP_MISMATCH` 或
  `TOKEN_EXPIRED` 等具体原因。

鉴权失败不会产生 `window_events`、桶增量和配额调整。

## 3. 多窗口与边界

| 窗口 | 长度 | 桶步长 | 精确上限默认值 |
|---|---:|---:|---:|
| short | 1000 ms | 100 ms | 512 个窗口内事件 |
| long | 60000 ms | 1000 ms | 2048 个窗口内事件 |

时间边界显式定义：

- 滑动窗口为左开右闭：`(at_ms - length_ms, at_ms]`。
- 桶为左闭右开：`[bucket_start, bucket_start + step_ms)`。
- 事件桶起点为 `floor(timestamp_ms / step_ms) * step_ms`。
- 对同一时刻的事件，已提交事件先于当前批次事件；当前批次内按 `event_seq` 排序。
- 迟到事件按其事件时间插入规范顺序，早于其时间的已提交事件可见，晚于其时间的事件不可见。
- 每个事件在每种窗口粒度中只进入一个桶。

## 4. 精确与采样路径

对一次窗口查询，先取规范前缀范围 `[left, right)`：

- `right` 为当前裁决事件的前缀终点，未来事件不参与；
- `left` 为第一个满足 `timestamp_ms > at_ms - length_ms` 的事件；
- 窗口内实际事件数为 `n = right - left`。

### 精确路径

当 `n <= exact_limit` 时，使用整数权重前缀和：

```text
W_exact = prefix_weight[right] - prefix_weight[left]
```

估计值、上下界都等于 `W_exact`，采样率为 `1/1`。只使用整数加法和减法，因此与求和顺序无关。

### 采样路径

当 `n > exact_limit` 时，批次内对每个 route/client/window 预选一个二的幂分母 `d`：

```text
d = min { 2^k | max_window_events <= exact_limit * 2^k }
p = 1 / d
```

事件哈希为固定密钥、请求标识、时间戳和权重的 BLAKE2b-64。哈希最高 `log2(d)` 位全零时入样。
这等价于确定性 Bernoulli 蓄水池采样，入样概率为 `p`。

Horvitz-Thompson 估计：

```text
W_hat = d * sum(weight_i for sampled i)
Var_hat = (d - 1) * d * sum(weight_i^2 for sampled i)
margin = ceil(z * sqrt(Var_hat) / 1e6)
CI = [max(0, W_hat - margin), W_hat + margin]
```

默认 `z = 2.576`，即近似 99% 正态区间。

裁决使用 `W_hat`，`exact_weight` 仅作为可核对真值保留在裁决和查询输出中。精确路径和采样路径使用相同阈值比较。
采样裁决同时输出 `sample_rate_num`、`sample_rate_den`、`method`、`variance`、`confidence_low`、
`confidence_high` 和 `exact_limit`。

## 5. 自适应配额

所有比例都用整数定点表示，比例单位为 ppm，`1_000_000 = 1.0`。

```text
P_short = W_hat_short / short_base_limit
P_long  = W_hat_long  / long_base_limit
P       = max(P_short, P_long)
A       = clamp(admitted_short / target_weight_per_sec, 0, 2)
raw     = clamp(1 + gain * (A - P), min_scale, max_scale)
scale   = previous_scale + damping * (raw - previous_scale)
quota_w = short_or_long_base * scale
```

默认 `gain=0.25`、`damping=0.20`、`min_scale=0.5`、`max_scale=1.5`。
配额与实际计数同为整数权重。任一窗口满足 `W_hat > quota` 即拒绝；等于配额仍放行。
`trigger_window` 为 `short`、`long` 或 `short+long`。

`checked_mul_div` 在每次乘法前检查 int64 安全范围，最终配额再次钳制到合法缩放区间。

## 6. 版本与跨模块一致性

- `snapshots.version` 每个令牌种子或批次提交递增 1。
- 批次意图固定 `base_version` 与 `target_version=base_version+1`。
- 同一请求的鉴权与限流都读取 `base_version`；裁决中写为：

```text
read_version = window_version = token_version = base_version
commit_version = target_version
```

- 所有新窗口事件和裁决在同一事务中可见；桶增量与配额缩放同时写入按版本保留的历史表。
- 多个查询共享读闸门；批次写者持有独占闸门。查询不会跨两个提交版本拼装结果。
- `/v1/windows?version=N` 的历史计数、桶与配额均从 `commit_version <= N` 的事实重建。
- 鉴权成功但被限流的裁决同时保留 `auth.ok=true`、`AUTH_OK` 与
  `decision_code=E_RATE_LIMITED`。

## 7. 并发和幂等

- 单进程只允许一个批次写者。写者执行期间到达的其他批次等待，恢复执行时作为下一批次，不会被丢弃或混入当前批次。
- 同一 `batch_id` 与同一规范化内容再次提交，直接返回已提交裁决，`replayed=true`。
- 同一 `batch_id` 内容不同返回 `E_CONFLICTING_REQUEST`。
- 同一 `request_id` 只裁决一次；跨批次重复请求复用原裁决。
- 同一 `request_id` 内容不同返回 `E_CONFLICTING_REQUEST`。
- 同一批次内重复 `event_seq` 返回 `E_CONFLICTING_REQUEST`。
- 同批次内相同请求内容可以在不同 `event_seq` 出现；只计一次，其他位置标记 `reused=true`。
- SQLite 使用 WAL、`BEGIN IMMEDIATE`、`synchronous=FULL` 和 `busy_timeout`。

## 8. 意图持久化与恢复

提交分两阶段：

1. 事务 A 写入 `batches(status=PENDING)` 与 `intents(status=PENDING)` 并提交；
2. 事务 B 写入窗口事件、桶、配额、裁决、批次完成状态和快照，并原子提交。

进程在 A 之后、B 之前被强制终止时，重启扫描 `PENDING` 意图：

- 将批次标记为 `ABORTED`；
- 将意图标记为 `ROLLED_BACK`；
- 保持最后完整 `state_version` 不变；
- 写入只包含已持久化事实的恢复报告；
- 报告明确 `complete_exit_report=false`，不假装进程还有完整退出报告。

如果终止发生在事务 B 内，SQLite 回滚整个 B；若 B 已提交，则不存在 PENDING 意图。

## 9. 复杂度与规模

- 规范化排序：`O(N log N)`。
- 每个 route/client 的前缀数组与采样前缀：`O(N)`。
- 单个窗口边界查找：`O(log N)`；整数和与采样估计：`O(1)`。
- 单批次写入：SQLite 批量事务，裁决与窗口事件以生成器喂给 `executemany`。
- 当前实测百万批次峰值约 4.4 GiB、SQLite 文件约 814 MiB；若机器内存较小，应降低批量或分多个批次提交。
