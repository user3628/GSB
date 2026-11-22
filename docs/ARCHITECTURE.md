# 架构与实现说明

## 1. 组件边界

```text
Producer / 验收脚本
        │ HTTP
        ▼
┌─────────────────────────────────────────────┐
│ Flask API (mqmw/api.py)                     │
│  enqueue / round / claim / ack / snapshot   │
└──────────────┬──────────────────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐   ┌───────────────────────────┐
│ Scheduler   │   │ SQLiteStore               │
│ thread pool │──▶│ queue locks + intents     │
└──────┬──────┘   │ WAL + request_results     │
       │          └─────────────┬─────────────┘
       │ HTTP POST              │
       ▼                        ▼
┌─────────────────┐      ┌──────────────┐
│ local consumer  │─────▶│ SQLite (WAL) │
│ success/refuse/ │ ack  └──────────────┘
│ hang/delay      │
└─────────────────┘
```

`SQLiteStore` 是所有业务状态的唯一入口；调度器只通过它领取租约、提交 HTTP 结果和确认。消费者可使用调度器推送的 `ack_url` 回调，也可由测试驱动显式调用 `/v1/acks`。

## 2. 数据模型

主要表：

- `queues`：优先级、`next_seq`、轮转游标。
- `messages`：队列内生产序号、消息标识、体摘要、可见性超时、重试上限、当前状态、尝试数、重投数、当前租约。
- `deliveries`：每次领取一条尝试记录，保存消费者、组、模式、租约令牌、截止时间、HTTP 结果。
- `acks`：确认记录、幂等键、状态 `APPLIED/COMMITTED/ROLLED_BACK`。
- `group_offsets`：消费组在队列上的连续终态位点。
- `consumers` / `consumer_queues`：消费者身份、组、本地 HTTP 端点和订阅关系。
- `intents`：业务写意图与阶段。
- `request_results`：请求标识、执行者令牌、运行/完成/失败/中止状态与结果。
- `rounds` / `round_traces`：轮次配置、统计、逐事件轨迹。
- `recovery_reports`：恢复报告。

SQLite 使用 WAL、`synchronous=FULL`、外键和 10 秒 busy timeout。每次读写操作使用独立连接。

## 3. 生产序号与幂等入队

队列写锁内开启 `BEGIN IMMEDIATE`：

1. 查询该 `(queue_name, message_id)` 是否已存在；存在则原样返回已有 `seq`。
2. 新消息使用 `queues.next_seq`，插入后在同一事务推进 `next_seq`。
3. 事务提交和回滚都不会留下序号缺口。
4. 入队本身不创建投递尝试，重复入队不会产生第二条投递记录。

批次中同一消息标识重复也返回同一个序号的 `duplicate=true` 项。

## 4. 状态机

消息状态合法迁移：

| 当前 | 合法目标 |
|---|---|
| `PENDING` | `DISPATCHED` |
| `DISPATCHED` | `ACKED`, `REDELIVERED`, `DEAD_LETTERED` |
| `REDELIVERED` | `DISPATCHED`, `DEAD_LETTERED` |
| `ACKED` | 无 |
| `DEAD_LETTERED` | 无 |

`validate_transition` 在任何持久化业务写之前执行。管理接口的非法迁移只返回 `E_INVALID_TRANSITION`，消息、投递、确认和位点均不变化。

投递尝试另有传输结果：`DISPATCHED`、`ACKED`、`REFUSED`、`TIMED_OUT`、`ERROR`、`EXPIRED`、`ROLLED_BACK`。消息状态是业务状态，投递尝试状态用于审计。

### 重试规则

- 领取时 `attempt_count += 1`。
- 失败或可见性超时后，如果 `attempt_count <= max_retries`，消息转 `REDELIVERED`，重投计数加一。
- 否则消息转 `DEAD_LETTERED`。
- 因此 `max_retries=0` 表示不重试；首次失败直接死信。
- 调度候选按队列内最小 `seq` 选择；消费组位点只跨过连续的 `ACKED/DEAD_LETTERED`。

## 5. 恰好一次确认

确认幂等键：

```text
(queue_name, message_id, consumer_id)
```

队列是消息标识的命名空间，因此同一消息标识可安全存在于不同队列。

确认事务同时：

- 把尝试从 `DISPATCHED` 改为 `ACKED`；
- 写/更新确认记录；
- 把消息改为 `ACKED` 并清空活动租约；
- 重算消费组连续位点；
- 推进 ACK 意图。

已 `ACKED` 的消息再次确认时，`exactly_once` 返回 `E_DUPLICATE_ACK`；至少一次返回 `E_ALREADY_ACKED`。两种情况下活动投递数和待确认数都不会再次变化。若使用相同 `request_id` 重放，则由请求结果表直接返回首次结果，不执行第二次确认。

## 6. 并行调度

### 候选规划

1. 先扫描过期租约。
2. 读取所有可投递消息。设置 `consumer_id` 时只考虑该消费者订阅的队列。
3. 按队列优先级严格分组；高优先级组先进入准入窗口。
4. 同优先级队列按持久化 `rotation` 再按队列名排序，消息按 `seq` 升序，在各队列间轮转取一条。
5. 每个优先级组首个被服务的队列在轮次结束时推进 `rotation`，使后续轮次轮转。
6. 准入窗口为 `global_concurrency + wait_queue_capacity`。窗口外消息保持原状态，并生成 `E_QUEUE_FULL` 降级项。
7. `strict_wait_queue=true` 时，窗口不足在领取前直接结束，HTTP 429。

候选顺序、优先级、轮转值、轮转排名、预算消耗时刻都写入 `queue_order` 或 `degradation_order`。

### 执行

- 每个全局槽位提交一个 `ThreadPoolExecutor` 任务。
- 领取前查询每队列当前 `DISPATCHED` 租约数；达到 `per_queue_inflight` 时把候选暂存回有界等待池，不改变消息状态；已有在途调用完成后重新检查并领取。只有在没有本轮在途调用可释放租约，或预算耗尽时，才把仍被阻塞的候选记为降级。
- 领取使用队列锁和 `BEGIN IMMEDIATE` 双重校验；`deliveries` 上的部分唯一索引保证同一消息最多一个活动租约。
- HTTP 任务使用独立 `requests.Session`。读超时后显式 `session.close()`，结果写出 `connection_closed=true`。
- 每次 HTTP 调用的实际超时为 `min(dispatch_timeout_ms, 轮次剩余预算)`。
- 截止时间后不再领取或提交新服务调用；已经派发的调用在预算相关超时内收敛。
- 单进程内同一时刻只允许一个轮次进入调度临界区；单条 `claim`/`ack` 仍可与轮次并发，并通过队列锁和事务重校验保持正确性。

### 降级与统计

降级原因：

- `E_QUEUE_FULL`：超出有界准入窗口。
- `E_TIME_BUDGET_EXHAUSTED`：准入截止前未领取。
- `E_PER_QUEUE_INFLIGHT_LIMIT`：队列租约已满。
- `E_NO_CONSUMER`：无启用的订阅消费者。
- `E_CLAIM_RACE`：并发写者先行领取。

被降级消息不做状态写入，保持 `PENDING` 或 `REDELIVERED`。统计区分：

- 正常派发成功、拒绝、连接错误、超时；
- 本轮首次投递与重投尝试；
- 失败后进入 `REDELIVERED` 的次数；
- 进入 `DEAD_LETTERED` 的数量；
- 已观察确认数、逐队列在途数、跳过数。

## 7. 意图日志与崩溃恢复

### 两阶段提交窗口

```text
BEGIN 意图 STARTED (独立提交)
BEGIN IMMEDIATE
  业务状态变更
  意图 -> APPLIED
COMMIT
  强制终止窗口
BEGIN IMMEDIATE
  意图 -> COMMITTED
COMMIT
```

投递和确认故意保留 `APPLIED -> COMMITTED` 的窄窗口，使强制终止可以被复现和验证。

### 恢复规则

- 扫描 `STARTED/APPLIED` 意图。
- `DISPATCH`：
  - 消息仍为 `DISPATCHED`：回滚为 `PENDING`，活动投递标记 `ROLLED_BACK`，立即可重排。
  - 消息已进入更高终态：保留终态，报告 `dispatch_preserved_superseded_state`。
- `ACK`：
  - 确认记录 `ROLLED_BACK`。
  - 尝试回退为 `DISPATCHED`。
  - 消息回退为未确认，恢复活动租约。
  - 重算消费组位点。
- `RUNNING` 请求置为 `ABORTED`，使同一请求标识可安全重试。
- 运行中轮次置为 `ABORTED`。
- 恢复结束后再执行一次可见性超时扫描。

报告中的 `durable_facts_after_recovery` 只来自 SQLite 当前持久化状态。

## 8. 并发与一致性

- **队列单写者**：进程内每队列一把 `RLock`；跨进程由 SQLite 单写事务串行化，并在事务内重新校验候选。
- **多读者快照**：WAL 允许读事务与写并发；`/v1/snapshot` 在单个 `BEGIN` 中读取消息、投递、确认和位点，返回同一提交版本。
- **独占租约**：写事务重查 + `uq_deliveries_active` 部分唯一索引。
- **请求幂等**：`request_results` 行先以 `RUNNING` 提交；并发同标识请求轮询等待，完成后复用结果。执行者死亡后由恢复置 `ABORTED`。
- **确认幂等**：数据库唯一确认键和终态重查。

## 9. 安全与运行边界

- 消费者 URL、ack base URL 必须是 `127.0.0.1`、`::1` 或 `localhost`。
- CLI 服务只允许绑定回环主机。
- 所有样本与测试端点均为本地进程。
- 不包含容器、云调用、远端注册中心或外部消息代理。
