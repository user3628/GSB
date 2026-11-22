# 设计说明

## 1. 持久化模型

SQLite 使用 WAL 和 `BEGIN IMMEDIATE` 写事务。主要表：

- `function_specs`：每个函数可保留多个版本，`active=1` 为当前准入版本。
- `function_runtime`：活跃/可用实例数、冷/热启动和超时累计计数。
- `quota_usage`：`on_demand_used` 与 `reserved_used` 分开计量。
- `instances`：状态、容量、已占槽位、最近活跃时间。
- `calls`：请求指纹、裁决状态、绑定实例、额度桶、终态业务码和结果。
- `intents`：`ADMIT_CALL`、`CREATE_INSTANCE`、`EXECUTE_CALL`、`RECLAIM_INSTANCE` 等意图。
- `injection_events`、`recovery_reports`、`operation_log`：异常和审计事实。

`runtime_meta.commit_seq` 在每次写事务中递增。完整快照在一个只读事务中读取，因此所有表来自同一提交版本。

## 2. 状态机

```text
CREATING -> READY | BUSY | TERMINATED
READY    -> BUSY | DRAINING | TERMINATED
BUSY     -> READY | DRAINING
DRAINING -> TERMINATED
TERMINATED -> (none)
```

额外约束：

- `READY` 必须 `occupied_slots == 0`。
- `BUSY` 必须 `occupied_slots > 0`。
- `TERMINATED` 必须 `occupied_slots == 0`。
- 进入 `DRAINING` 后不再被实例选择器接受；最后一条在途调用完成时，终止实例、释放调用额度和更新计数在同一事务提交。

非法迁移在任何写入前检查。异常事务回滚，因此 API 报错时不会留下部分状态。

## 3. 额度模型

对每个函数：

```text
on_demand_capacity = function_concurrency_limit - reserved_quota
occupied_total     = on_demand_used + reserved_used
```

`AUTO` 先使用按需额度，按需满后可使用本函数声明的预留额度；`ON_DEMAND` 只使用按需额度，满后排队；`RESERVED` 只使用本函数预留额度，未声明或已满时返回 `E_QUOTA_DENIED` 并附当前占用。

## 4. 单写者准入与优先级队列

每个函数有一把进程内 `RLock + Condition`。准入、队列提升、实例选择和额度占用在同一写事务完成。等待者不持有写锁。

队列排序：

```text
priority DESC, retry DESC, arrival_ms ASC, created_seq ASC, call_id ASC
```

队列长度达到 `queue_capacity` 后返回 `E_CONCURRENCY_LIMIT`。只有队首能被提升；完成调用的线程提交后唤醒等待者。重新读取时只观察完整提交。

## 5. 冷启动、预热与初始化并发

- 没有可复用的 `READY/BUSY` 空槽实例时创建新实例，结果标记 `cold_start=true`。
- 复用已有实例槽位标记 `warm_start=true`。
- `init_concurrency` 用函数级信号量限制真实启动 HTTP 并发。
- 调用结果持久化：
  - `init_queue_wait_us`：等待初始化并发许可的耗时；
  - `start_http_elapsed_us`：真实 `/start` 请求耗时；
  - `start_elapsed_us`：两者之和，作为冷启动延迟观察值。
- 显式 `/prewarm` 在调用到来前创建 READY 实例，因此后续首调用为热启动。
- 每个函数最大实例数为 `ceil(function_concurrency_limit / per_instance_concurrency)`。

轨迹回放入口按每条记录的 `arrival_ms` 计算相对单调时钟目标时刻并提交调用，`time_scale` 可压缩或放大时间轴。回放不绕过正常准入、去重和持久化路径。相同 `call_id` 重放会复用已提交结果。

## 6. 真实超时

启动和执行都使用独立 `http.client` 连接：

1. 先提交 `CREATE_INSTANCE` 或 `EXECUTE_CALL` 的 PENDING 意图。
2. 使用独立 socket timeout 请求 loopback 端点。
3. 超时、拒绝或传输失败后，在 `finally` 中显式 `connection.close()`。
4. 在同一事务内更新调用终态、意图终态、实例占用、函数计数和额度。

不创建“负责超时的线程”，也不声称已终止远端代码。调用超时只证明本端连接被中止；远端测试端点可能继续执行。

## 7. 去重

`call_id` 为主键，请求指纹包含函数、执行时长、优先级、重试、额度类别和注入参数。并发重复提交时，后到者等待首个请求终态并返回同一结果，`deduplicated=true`。相同 ID 不同指纹返回 `E_CONFLICTING_CALL`，不新增调用行，也不额外执行。

## 8. 重启恢复

启动时在一个写事务中：

1. 读取所有 PENDING 意图和非终态调用。
2. 把 `QUEUED/ADMITTED/CREATING/EXECUTING` 调用改为 `FAILED/E_RECOVERED_INTERRUPTED`。
3. 把受影响的 `CREATING/BUSY/DRAINING` 实例改为 `TERMINATED`、占用槽位清零。
4. 以仅剩的非终态调用重新计算额度占用；正常恢复后为 0。
5. 从实例表重新计算活跃和可用实例数。
6. 将 PENDING 意图标记为 `RESCUED`。
7. 将前后额度差、终止实例、恢复调用和“无完整退出报告”声明写入 `recovery_reports`。

恢复不推断强制终止前远端是否执行成功，只报告本次重启可证明的持久化事实。

## 9. 不变量审计

`/v1/audit/invariants` 检查：

- 运行中调用必须绑定存在且非 TERMINATED 的实例；
- 运行中调用必须有额度桶；
- 每个非终止实例的 `occupied_slots` 必须等于其运行中调用数；
- TERMINATED 实例占用槽位必须为 0；
- 按需和预留占用不得超过各自上限，两者之和不得超过函数上限；
- `quota_usage` 必须等于运行中调用的实际桶计数。

