# 设计与一致性说明

## 数据流

```text
fixed-seed generator
        |
        v
snapshot / ingest batch --> validation (whole batch)
        |                         |
        | invalid / mismatch      | valid
        v                         v
   no state write         durable PREPARED intent
                                  |
                                  v
                     dedupe + deterministic rebuild
                                  |
                                  v
                  bounded coordinator (workers + queue)
                                  |
                                  v
                   real loopback HTTP heal dispatch
                                  |
                                  v
                 one atomic SQLite commit(version N+1)
```

引擎用一把写锁串行化快照切换、批次重建与恢复。`store.Open` 对 `<db>.lock` 获取非阻塞 `flock`，因此同一数据库只有一个活跃写者进程；进程被 `SIGKILL` 后由内核释放锁。只读查询不拿写锁；SQLite WAL 和只追加的 `committed_states` 行让同一进程内多个读者读取同一完整版本。

## 时序重建

规范记录先按 `holder_id + report_seq` 归并。排序键为：

```text
(client_timestamp_ms, server_arrival_ms, report_seq)
```

`rebuilt_seq` 是排序后的 1 基位置。`interval_ms` 是当前与前一重建记录的客户端时间差。续约计时锚点初始为第一条记录的客户端时间；续约仍在租约内时，锚点推进到该续约时间。心跳不推进锚点。

每条记录输出：

- 重建序号
- 相对前一记录的时间间隔
- 距续约锚点的已用时间
- 安全续约点及其相对差
- `EARLY` / `ON_TIME` / `LATE`
- 是否仍在租约内、请求时剩余寿命
- 重复投递次数、本次漂移贡献

排序键都是记录字段，因此批次顺序、数据库行顺序和并发完成顺序都不会改变结论。

## 漂移判定

设安全点为 `S`、容差为 `T`。续约的容差上界为 `S + T`。只有迟到超过该上界，才累计迟到并产生漂移贡献：

```text
contribution = elapsed - (S + T)
```

满足任一条件即漂移：

1. 连续迟到续约数达到 `consecutive_late_threshold`
2. 迟到且间隔超过 `drift_interval_threshold_ms`
3. 租约在续约前过期

容差内始终不漂移。结果按持有者标识排序；预算延后清单按漂移幅度降序、持有者标识升序排序。

## 自愈

持有者声明策略，决策结果包含动作、触发依据、漂移幅度、原租约、动作参数和预计恢复时间。`AUTO` 的确定性规则：

- 续约次数耗尽、租约已失效或漂移幅度不小于整个租约：撤销
- 漂移幅度达到 `max(200ms, lease/5)`：缩短到原租约的 75%
- 其余：要求提前续约，新续约点为安全点的 60%

下发请求是 JSON POST。`loopback.Dispatcher` 为每次调用创建独立的 `http.Transport` 和连接追踪器，避免并发请求互相关闭连接。超时后先关闭该请求的全部底层连接，再把错误映射为 `E_HEAL_TIMEOUT`。回环端点可注入：

- `delay`：延迟后成功
- `reject`：HTTP 503
- `late`：忽略取消并晚响应
- `block`：等待客户端关闭连接，用于验证真实断连

## 去重状态

规范记录保存在 `State.Observations`，重复次数保存在同一行的 `duplicate_count`。归并规则：

- 同一序号下按服务端到达时间、客户端时间戳、摘要、类型选择确定性首条
- 后续相同摘要只增加计数
- 摘要冲突立即返回 `E_REPORT_MISMATCH`，确定性首条及既有状态不变

因此跨批重复投递不会放大观测数，也不会改变已重建序号。重放批仍触发全量漂移重算。

## 预算与并发

协调器的准入单位是整批：

```text
max_inflight = concurrency + queue_capacity
```

若当前已准入工作加上整批任务超过该值，整批返回 `E_QUEUE_FULL`，没有任何任务启动。准入后，每个任务竞争全局信号量；等待者构成有界队列。批次上下文带统一 deadline，等待或运行超过预算的任务进入延后集合。

写意图后会先做一次纯预分析，用于形成恢复可读的动作计划和延后排序；每个持有者真正入账的重建会在协调器任务内再次执行，因此它同样占用全局并发槽并受轮次 deadline 约束。若任务在预算耗尽前未完成，预分析结果仍会把观测、重建行和漂移结论写入状态版本；下一轮 `Reprocess` 或新批次会再次处理其自愈。

## 快照与版本

快照摘要覆盖 `version + generated_at_ms + dataset` 的规范 JSON。提交顺序：

1. 读取当前快照版本，陈旧版本优先返回 `E_STALE_SNAPSHOT`
2. 校验快照时间和整体摘要
3. 校验数据集
4. 预写 `snapshot_switch` 意图
5. 在单个事务中插入新不可变状态并标记意图已提交

新快照整体替换持有者、观测、结果与自愈状态。任何校验或提交失败都不会改动旧行。

`state_version` 与 `snapshot_version` 分离：前者覆盖所有重建/切换提交，后者只由外部快照推进。`Read(observed_version)` 比较的是重建状态版本。

## SQLite 意图与恢复

表结构：

- `committed_states(version PRIMARY KEY, snapshot_version, payload, created_at_ms)`：只追加
- `intents(... state, base_version, target_version, payload ...)`：`PREPARED` / `COMMITTED` / `ABORTED`
- `recovery_reports(...)`：每次恢复的事实快照

提交事务同时完成两件事：插入状态行、把对应意图改为 `COMMITTED`。所以二者要么一起可见，要么都不可见。

进程被 `SIGKILL` 后：

1. 新进程读取最大完整 `version`
2. 扫描 `PREPARED` 意图
3. 把意图标为 `ABORTED`
4. 保留最后完整状态行，不推进版本
5. 写入恢复报告，只陈述磁盘上可验证的意图、版本和差异

如果自愈 HTTP 已经发出但进程在提交前被杀，恢复无法声明该外部动作没有发生；意图 payload 保留了动作计划，报告会明确标为中止，而不是伪造成功。

## 恢复不变量

- 每个 `committed_states` 行都是完整 JSON 且版本与主键一致。
- `CommitIntent` 要求当前版本恰等于意图的 base 版本，防止并发覆盖。
- 恢复不修改历史状态行。
- 恢复后的新写入从最后完整版本的下一个版本继续。

