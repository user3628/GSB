# LeaseFair 设计说明

## 1. 分层

- `fixedpoint.py`：Q9 整数、有理数舍入、最大余数归一化、Kahan/Decimal 补偿求和、非规范浮点审计。
- `time_contract.py`：毫秒到期计算、TTL 秒/分/时归一化、余量与时钟偏移报告。
- `metrics.py`：占比、目标占比、余量、占用率、加权公平度、再平衡与候选投影公平度。
- `storage.py`：SQLite 模式、WAL/事务边界、租约与意图行操作。
- `service.py`：单写者业务裁决、版本检查、幂等、仲裁、缓存刷新与启动恢复。
- `api.py`：本地 FastAPI 接口与业务错误码映射。
- `samples.py`、`scripts/`：固定样本和真实强制终止演示。

## 2. 数据库事实模型

`lock_state` 保存每个锁键当前版本；`leases` 以 `(lock_key, version)` 唯一保存授予时刻、到期时刻、TTL、续约次数、权重文本/Q 值与状态。`expiry_index` 只索引当前活跃租约。`requests` 保存规范化请求哈希与完整响应，`intentions` 保存镜像和完成状态，`metric_cache` 保存最近一次写事务生成的指标快照，`lease_events` 保存可审计轨迹，`recovery_runs` 保存启动恢复报告。

持有者权重同时存十进制文本与 Q9 整数。前者用于还原输入精度与计算量化误差，后者用于规范指标路径。

## 3. 写事务协议

每次授予、续约、释放执行两阶段持久化：

1. **意图准备事务**：`BEGIN IMMEDIATE`；检查幂等请求；读取锁键镜像；插入或复用 `requests(status='prepared')` 与 `intentions(phase='prepared')`；提交。
2. **业务提交事务**：再次 `BEGIN IMMEDIATE`；复查幂等状态；处理到期；校验版本/持有者；修改租约与到期索引；刷新指标缓存并递增 revision；把意图改为 `committed`；写入完整幂等响应；提交。

任何一步在事务中途被终止，SQLite 都会原子回滚。业务提交后，意图完成状态与响应已与租约处于同一提交边界，不存在“续约已提交但缓存仍按旧到期时刻”的可见状态。

指标重算也先提交一条 `metrics_recompute` 意图，再在第二个事务中扫描过期、重算缓存并提交意图。

## 4. 强制终止恢复

启动时扫描 `phase='prepared'` 意图。若意图属于仍存活的 PID（包括当前进程中的另一个服务实例），则跳过并保留。对死进程意图：

- 授予：若存在与意图版本和请求标识完全匹配的持久化租约，删除该未完成租约，恢复旧 `lock_state.version` 并删除到期索引；锁重新可用。
- 续约：若持久化续约次数与到期时刻匹配意图，恢复前镜像中的到期时刻、TTL、续约次数与到期索引。
- 释放：若租约已标记释放且结束时刻匹配，恢复活跃状态与旧到期索引。
- 未发现可补偿业务行：记录 `no_op`，并明确说明业务事务从未提交。

恢复事务同时把未完成请求标为 `rolled_back`、刷新指标缓存、写入 `recovery_runs`。报告包含意图、操作、依据、恢复前后完整锁镜像和锁是否可用；不推断内存状态，也不承诺强制终止前已发送但未提交的响应存在。

## 5. 并发

SQLite WAL 允许并发读者；写者由 `BEGIN IMMEDIATE` 串行化。每次操作使用独立连接，不跨线程共享连接。锁键裁决、版本递增和插入租约在同一写事务内，因此并发候选只能有一个成功。

同一 `request_id` 的两个请求即使同时到达，也共享 `requests` 行和 `prepared` 意图。首个业务事务成功后，第二个事务看到 `completed` 并返回已存响应；内容哈希不一致立即返回 `E_CONFLICTING_REQUEST`。

## 6. 定点与舍入顺序

规范 Q 值均为整数：

```text
Q = 1_000_000_000
q_from_decimal(x) = round_half_even(x * Q)
q_mul(a,b) = round_half_even(a*b / Q)
q_div(a,b) = round_half_even(a*Q / b)
share_i = normalize_largest_remainder(W_i, sum(W))
F = 1 - Kahan_sum_i(round(target_i * abs(share_i-target_i)))
```

最大余数分配按 `(remainder desc, holder_id asc)`，公平度项按 `holder_id asc`，因此映射遍历顺序和候选提交顺序不影响规范结果。

每项误差以 Q 单位记录。归一化单项绝对误差小于 1Q；Q 乘法最终舍入单项不超过 0.5Q；输入量化单项不超过 0.5Q。占比/公平度管线报告采用保守上界 `4*n_holders + n_leases` Q 单位；余量、单租约占比、占用率与再平衡相对偏差的累计误差另行输出。规范 Kahan/整数求和的累计求和误差为 0。

非规范审计使用 Python `sum`、NumPy `sum` 与 Kahan 浮点作对照，并给出：

```text
naive bound = gamma_(n-1) * sum_i |x_i|, gamma_n = n*u/(1-n*u), u=2^-53
Kahan bound = 2*u*sum_i |x_i| + O(n*u^2)
```

这些浮点值不会进入占比、公平度、授予或再平衡判断。

## 7. 时间契约

- 存储和计算：整数毫秒。
- 到期：`grant + ttl_ms`；续约：`now + ttl_ms`。
- 显示秒：固定 3 位、截断；显示分：固定 6 位、截断；显示小时：固定 9 位、截断。
- 截断误差按原始单位输出；不把截断后秒/分/时写回持久化状态。
- `FixedClock` 用于测试，`Clock.offset_ms` 或 `LEASE_CLOCK_OFFSET_MS` 用于时钟偏移注入。正偏移缩短剩余寿命与余量，到期后 clamp 为 0；历史授予/到期时间不被改写。

## 8. 已知设计选择

- SQLite 的单写者特性使所有锁键写操作全局串行；这强于“每锁键单写者”，换取跨进程一致性与简单可证明性。
- 实际租约占比按“活跃租约权重之和”计算，目标是持有者申报权重在活跃持有者集合内归一化；TTL 主要影响续约余量，不直接改变占比。
- 候选优先级采用加入候选后的投影公平度，而不是当前偏差排序；这样输出可以直接比较双方公平度。
- 锁键总量由 `lock_registry` 定义，获取时会自动注册；若需要固定分母，应预先注册全部锁键。

