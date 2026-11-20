# 技术设计

## 1. 分层

```text
FastAPI routes
  └── Workbench engine
        ├── validation.py   输入归一化、内容哈希、字段校验
        ├── ingest.py       整批内存模拟、状态机与窗口推进
        ├── scheduler.py    确定性轮次计划、加权预算、近似候选
        └── db.py           SQLite schema、快照事务、意图和恢复报告
```

所有业务路由使用同步函数，FastAPI 将其放入线程池执行；`Workbench` 为每个连接提供 `RLock`，所以同一连接的提交、调度、关闭严格串行。查询不拿连接锁，而是开启独立只读事务读取一致快照。

## 2. 提交事务边界

提交样本分为三个边界：

1. 纯内存构建 `IngestPlan`。任一副本、窗口或状态校验失败都不会写数据库。
2. 单独事务写入 INGEST 意图并提交。
3. 一个 SQLite `BEGIN IMMEDIATE` 事务同时写入连接、流、任务、迁移、违例、样本和调度前状态，并提交意图为 COMMITTED。

因此，非法帧或非法迁移不会留下连接/流/任务；进程在边界 2、3 之间退出时，只留下可被恢复扫描的 PENDING 意图。

`batch_size` 被记录为逻辑批次列表，但校验和状态推进使用同一份确定性序列。这个设计把“整批不推进”提升为“整样本原子推进”，同时证明不同批次划分不影响结果。

## 3. 窗口模型

连接级：

```text
send(bytes): connection_window -= bytes
WINDOW_UPDATE(inc): applied = min(inc, initial_connection_window - current)
                    connection_window += applied
```

流级：

```text
send(bytes): window -= bytes; sent_unacked += bytes
WINDOW_UPDATE(inc): applied = min(inc, initial_window - current)
                    window += applied
                    sent_unacked = max(0, sent_unacked - applied)
```

当 `applied < inc` 时写入 `WINDOW_UPDATE_OVERFLOW_TRUNCATED`；零增量写入 `WINDOW_UPDATE_ZERO`。截断规则只依赖当前窗口和初始上限，不依赖时间或数据库行顺序。

## 4. 状态机

状态集合固定为 `IDLE`、`OPEN`、`HALF_CLOSED_LOCAL`、`HALF_CLOSED_REMOTE`、`CLOSED`、`RESET`。

本地 DATA 只允许 `OPEN` 与 `HALF_CLOSED_REMOTE`。END_STREAM DATA 只有在该任务最后一个字节被调度发送后才迁移状态，避免“尚未发送却先关闭”。RST_STREAM 立即取消该流所有未发送任务。连接关闭取消所有任务，并把活动流转为 CLOSED。

HEADERS 支持可选 `direction: local|remote`；缺省为 `local`。方向只影响半关闭侧，本地 DATA 语义始终固定。

## 5. 调度算法

每轮输入为不可变的连接/流/任务快照，先计算完整 `SchedulePlan`，再逐轮提交。

### 5.1 候选

```text
pending = streams with pending bytes and sendable local state
pool    = pending with stream window > 0 and connection window > 0
if stream_count > candidate_threshold:
    candidates = heapq.nsmallest(candidate_limit, pool, key=stable_rank)
else:
    candidates = sorted(pool, key=stable_rank)
```

稳定排序键：

```text
(priority, -weight, sent_unacked, -pending_bytes, stream_id)
```

候选截断使用有界堆式 top-K，时间复杂度为 `O(N log K)`，不会对全部流排序。候选截断时记录遗漏流数、遗漏待发字节、`error_bound_bytes` 和遗漏权重比例。后续轮重新计算排名，因此近似只影响单轮候选预算，不会永久饿死未选流。

### 5.2 轮预算分配

1. 按优先级分组。
2. 组权重 `max(1, priority_scale // 2^priority)`，用整数加权分配得到每组预算。
3. 组内按流 `weight` 再做一次带硬容量的整数加权分配。
4. 每个流容量为 `min(pending_bytes, stream_window)`；总预算同时受 `round_budget` 与连接窗口约束。
5. 同权重、余数等冲突统一按 `stream_id` 升序，确保跨进程、跨批次、跨查询一致。

轨迹保存本轮实际字节、优先级、权重、分配依据、窗口前后值和任务消费明细。END_STREAM 在同一事务中写入迁移。运行结果再按流聚合 `diagnoses`，给出已发送字节、轮次、最终状态、窗口和确定性结论。

## 6. 持久化与恢复

关键表：

- `connections` / `streams`：当前完整状态。
- `data_jobs`：每条 DATA 的原始字节、剩余字节和发送状态。
- `samples`：样本内容哈希和原样复用的响应。
- `schedule_runs` / `schedule_rounds`：运行和轮次事实。
- `trace_entries` / `blocked_entries`：逐流轨迹与阻塞诊断。
- `state_transitions` / `protocol_violations`：状态与协议审计。
- `intents`：INGEST 和 SCHEDULE_ROUND 的先写意图。
- `recovery_reports`：重启恢复事实。

每个调度轮：

```text
BEGIN IMMEDIATE; INSERT intent PENDING; COMMIT;
BEGIN IMMEDIATE;
  apply window/job/state/trace updates;
  UPDATE intent COMMITTED;
COMMIT;
```

强杀发生在第二个事务任一点时，SQLite 回滚窗口和轨迹；PENDING 意图在重启后转为 ABORTED。运行状态随后变为 ABORTED，已提交轮次保留。恢复报告中的事实均由数据库读取，不构造“完整退出摘要”。

## 7. 一致性与幂等

- 样本提交锁在样本 ID 上；连接写锁在连接 ID 上。锁顺序固定为样本锁后连接锁，避免死锁。
- 相同样本内容哈希返回存储的响应并标记 `cache_replayed=true`，不会重复插入任务或推进窗口。
- 内容不同的同 ID 提交在写入前返回 `E_CONFLICTING_SAMPLE`。
- 只读查询在 `BEGIN` 快照内读取连接和所有流；即使调度正在逐轮提交，也只能看到某个完整轮次边界。
- 调度运行结果和轮次轨迹均来自已提交行；运行中的查询不会伪造最终状态。

## 8. 规模取舍

默认 `round_budget=16384`、`candidate_threshold=256`、`candidate_limit=64`。数十到数百流时通常走精确路径；数千流时每轮用有界堆选出 `K` 个候选，复杂度为 `O(N log K)`，预算分配和 SQL 更新在候选数量上线性；未选流由后续轮次覆盖。
