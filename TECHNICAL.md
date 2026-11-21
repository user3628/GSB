# 技术设计说明

## 1. 分层与不变量

- `models.py` 只负责输入模型和严格校验。
- `numerics.py` 只负责确定性权重归一化、Decimal 求和、Kahan/朴素求和诊断和容差比较。
- `db.py` 负责 SQLite 事务、意图日志、不可变结论和启动恢复。
- `service.py` 负责组级单写者、严格等待、迟到队列、聚合编排和查询。
- `server.py` 只绑定回环地址，做路由、JSON 和业务码映射。

关键不变量：

1. 非法上报批次在进入数据库事务前完成结构校验；事务中发生冲突则整批回滚。
2. 同一副本在同一有效轮次最多一条有效上报。
3. 一个轮次最终最多一个 `round_conclusions` 行；查询只读取该完整行。
4. 聚合必须先提交 `PREPARED` 意图，才能进入耗时估算和最终结论事务。

## 2. 权重与定点数值

全流程使用 Python `decimal.Decimal`，上下文精度 80。配置权重从 JSON 的最短十进制文本构造，避免先转成二进制浮点再反推。

令副本全集为 `R`，已上报集合为 `P`，方案为 `m`。原始归一化权重为：

```text
ALL_EQUAL:      w_i = 1 / |R|,            i ∈ P；缺失副本为 0
REPORTED_RATIO: w_i = c_i / Σ_{j∈P} c_j, i ∈ P；缺失副本为 0
MISSING_ZERO:   w_i = c_i / Σ_{j∈R} c_j, i ∈ P；缺失副本为 0
```

其中 `c_i` 是配置权重。原始权重按 `ROUND_HALF_EVEN` 量化到 18 位小数；残余量化单元按“最大余数优先、同余数按副本标识字典序”分配。结论输出原始权重、归一化权重、分母、残差分配和累计归一化误差。

组级估算：

```text
weighted_blocks = Σ w_i * block_count_i
estimated_total = weighted_blocks * block_size_bytes
estimated_synced = Σ w_i * synced_bytes_i
pending = round_0(estimated_total) - round_0(estimated_synced)
progress = round_12(round_0(estimated_synced) / round_0(estimated_total))
```

字节使用整数定点，进度固定 12 位小数。若 `progress` 超出 `[0,1]`，或估算总量为零而已同步量为正，直接返回 `E_PROGRESS_OUT_OF_RANGE`；禁止 clamp、NaN 或无穷值。

## 3. 求和稳定性与误差上界

规范值是 80 位 Decimal 补偿计算：所有项先按 `(replica_id, report_id)` 排序。Kahan 和朴素 float 求和只作为诊断，用于输出可量化误差。

设参加求和的浮点项为 `x_i`，机器单位舍入误差为 `u = 2^-53`，则输出：

```text
naive_bound = n * u * Σ |x_i|
kahan_bound = (2 + n*u) * u * Σ |x_i|
```

实际误差用 `|Decimal(float_sum) - Decimal_exact|` 计算，并报告是否落在对应上界内。固定病态样本 `samples/numerics_cases.json` 中朴素顺序求和与 Kahan 都会受到大幅抵消影响；因此最终领域结论不依赖任一 float 路径，而依赖 Decimal 规范值。重复读取同一轮次时直接复用已提交 JSON，连重算都不发生。

固定样本 `REPORTED_RATIO` 加权块计数的实际诊断值：

```text
naive_abs_error = 0.0000000393545684348125
naive_error_bound = 0.000000317891439320187437876729053432
kahan_abs_error = 0.0000000393545684348125
kahan_error_bound = 0.00000031789143932018747316976857988229
```

这量化了 float 路径误差，同时规范值由 Decimal 保持确定性。

## 4. 收敛与容差契约

比较对象均为最终固定 12 位进度：

```text
delta = abs(actual - target)
scale = max(abs(actual), abs(target))
allowed = tolerance * scale
converged = delta <= allowed
```

乘法和比较全部使用 Decimal。`delta == allowed` 是包含边界，判为收敛。报告同时输出 actual、target、tolerance、scale、delta、allowed、边界标志和固定参与者顺序。容差判定不读取 elapsed time、上报顺序或未经排序的容器。

## 5. 迟到、封版与严格等待

迟到条件为三者之一：

1. `reported_at > deadline`；
2. 服务接收时刻 `received_at > deadline`；
3. 轮次已有 `sealed_at` 或状态不是 `OPEN`。

迟到项写入 `replica_reports`，`late=1`，并在存在“下一个未封版轮次”时立即设置其 `eligible_round_id`；否则保持待领养状态。轮次创建时原子领养该组所有待领养迟到项。迟到项的原始提交轮次和迟到原因永久保留。

非严格聚合可以在截止前封版，结论文档标记 `closed_early=true`。严格聚合在组锁之外等待截止时刻，避免阻塞上报写入；截止后若仍有缺失副本且 `grace_ms>0`，再等待宽限期。输出实际延迟来源、毫秒数和逻辑起止时刻。

## 6. 并发与可见性

- HTTP 使用 `ThreadingHTTPServer`，不同查询可并发。
- 每个 `group_id` 有一个进程内 `RLock`，封版、意图、估算和提交串行；不同副本组可并行。
- SQLite 使用 WAL、`synchronous=FULL` 和 `BEGIN IMMEDIATE` 写事务。
- 查询结论只执行一次对 `round_conclusions` 完整行的读取，绝不把“上报已入库”当作结论。
- 同一轮次重复聚合若参数摘要一致，直接返回既有 `conclusion_json` 和摘要；不一致返回 `E_CONFLICTING_ROUND`。

该示例假设一个服务进程拥有一个 SQLite 文件。进程内组锁保证单写者；若未来多进程写同一文件，需要再引入数据库级租约/领导者选举，而不能只依赖本进程锁。

## 7. 意图与恢复状态机

```text
OPEN
  -> AGGREGATING (sealed_at, PREPARED intent committed)
  -> COMMITTED   (conclusion + intent COMMITTED in one transaction)
  -> ABORTED     (domain failure or startup recovery)
ABORTED
  -> AGGREGATING (same input fingerprint retry)
  -> COMMITTED
```

聚合步骤：

1. 校验归一化参数、版本和已有结论。
2. 在组锁内读取有效上报快照，按轮次、组版本、方案、报告摘要构造 input fingerprint。
3. 单独提交 `PREPARED` 意图，同时封版。
4. 执行 Decimal 聚合。演示环境可在此处暂停。
5. 在一个事务中写不可变结论、更新轮次 `COMMITTED`、更新意图 `COMMITTED`。

进程被 SIGKILL 时，步骤 5 要么完整存在，要么完全回滚。重启时扫描 `PREPARED`：

- 若完整结论已存在，意图改为 `COMMITTED`，轮次回到最后完整版本；
- 否则意图和轮次都标记为 `ABORTED`，并基于 SQLite 中的意图、轮次、报告计数和结论存在性生成恢复报告；
- 恢复报告写入 `recovery_reports`，API 只读取已持久化报告。

## 8. 测试覆盖

16 个 unittest 覆盖：

- 副本数、负权重、权重和为零、负整数、浮点整数、64 位溢出、未知副本；
- 非法批次整批原子拒绝；
- 三种归一化及其确定性；
- Kahan 上界与病态固定样本；
- 容差边界、真/假结论和输出比较值；
- 进度越界业务码；
- 迟到项只进入下一轮；
- 严格等待延迟来源；
- 同轮同参数并发聚合只执行一次、复用同一摘要；
- 同轮不同归一化并发冲突；
- HTTP 固定回环端口完整流程与并发查询；
- 启动扫描 PREPARED 意图、恢复报告、重试提交和重复复用。

另有两个真实子进程演示：`scripts/demo.py` 通过 HTTP 完成三种方案与迟到/容差/严格等待；`scripts/crash_demo.py` 在意图提交后、结论提交前使用 `Popen.kill()` 强制终止，再重启恢复并重试。

