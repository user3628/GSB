# 算子融合与背压预算组件说明

## 1. 交付范围

本仓库是一个可离线运行的本地流计算验证组件：

- 固定种子生成 3 个分区、3 个滚动窗口、每窗口 4 条事件，共 36 条确定性事件。
- 生成 `events.jsonl`、`events-snapshot.json` 与带 SHA-256 摘要的 `policy-v1.json`。
- 校验事件与策略，规划线性算子链和融合分组。
- 按融合组执行 Scan、Filter、Project、Partition-local Aggregate。
- 持久化批次、意图、缓冲水位、背压事件、降载决策、窗口结论和恢复报告。
- 支持策略整份替换、增量 patch、版本绑定和同事务级联重建。
- 支持同批次幂等、内容冲突检测、分区单写者和并发只读查询。
- 支持真实 `SIGKILL` 后的意图扫描与恢复。

SQLite 驱动为纯 Go 的 `modernc.org/sqlite`，依赖已放入 `vendor/`。使用空 `GOMODCACHE`、空 `GOCACHE` 和 `GOPROXY=off` 也可构建。

## 2. 代码地图

- `model.go`：事件、策略、融合计划、批次、结论、恢复报告等契约。
- `sample.go`：固定种子样本、默认策略和数据目录初始化。
- `spec.go`：JSONL/快照/策略解析、int64 边界、分区序号单调性和摘要校验。
- `plan.go`：线性链、签名相容、融合规则和判定依据。
- `exec.go`：窗口归属、过滤、投影、计数/求和/最小值/最大值和溢出检查。
- `budget.go`：确定性准入、并发上限、时间预算和 SHED 排序。
- `store.go`、`store_*.go`：SQLite schema、意图、缓冲、结论、策略切换和查询。
- `service.go`、`service_advance.go`、`service_round.go`：幂等单飞、分区锁、推进和轮次。
- `service_policy.go`：整份替换、增量 patch、stale base 和级联重建。
- `recovery.go`：未完成意图恢复。
- `demo.go`：一键端到端演示编排。
- `cmd/feedbackstream/main.go`：CLI。

## 3. 关键语义

### 窗口与迟到

默认窗口为 1000ms 滚动窗口，对齐原点为固定样本时间 `1767225600000`，边界规则显式声明为：

`START_INCLUSIVE_END_EXCLUSIVE`

即窗口为 `[start_ms, start_ms + 1000)`。事件按时间戳归入对应窗口；迟到事件仍归属其时间戳窗口，但如果该分区+窗口+融合签名已经提交，则不重算、不覆盖旧结论，只记录 `late_events` 并将批次置为 `LATE_RECORDED`。

当前链路没有跨分区重分布，因此聚合是 partition-local；`window_results.partition_id` 是聚合键的一部分。需要全局口径时，可在 SQLite 中按 `window_start_ms, group_key` 再做 `SUM/COUNT/MIN/MAX` 汇总。

### 融合

默认策略声明：

- 可融合 kind：`scan`、`filter`、`project`、`aggregate`。
- 显式 pair：`scan -> filter`、`filter -> project`。
- 最大融合组大小：3。

因此默认计划为：

- `fg-1 = [scan, filter, project]`
- `fg-2 = [aggregate]`

每条边都会输出 `FUSED` 或不融合原因，例如 `PAIR_NOT_DECLARED`、`MAX_GROUP_SIZE`、`CROSS_PARTITION_REDISTRIBUTION`。

### 背压

每个融合组使用策略 `operator_buffers[fg-x]`；未配置时使用 `default_buffer`。在途批次数和字节数通过 `buffer_inflight` 实时计算：

- 达到 `high_watermark_ratio` 后标记 `BACKPRESSURED`。
- 已达水位时，新推送进入 `waiting_batches`，批次置为 `BLOCKED`。
- 背压事件记录水位、触发组和该目标组上游阻塞批次数。
- 所有融合组均背压且活动等待数达到 `max_wait_queue` 时，返回 `E_BACKPRESSURE_STALL`。
- 等待批次只更新为 `DONE/CASCADE_INVALIDATED`，不会被删除或丢弃。

### 预算与降载

候选批次按 `(partition_id ASC, window_start_ms ASC, batch_id ASC)` 稳定排序。单批估算成本为：

`cost = cost_model.base_cost_ms + cost_model.per_event_cost_ms * event_count`

`partition_concurrency` 限制全局同时活跃的分区数量；同时依据单写者约束，同一分区已有活跃批次或本轮已准入一个批次时，不再准入该分区的第二个批次。上述两种并发限制均输出 `PARTITION_CONCURRENCY_LIMIT`；如果成本超过剩余 `round_time_budget_ms`，输出 `ROUND_TIME_BUDGET_EXHAUSTED`。被降载批次持久化为 `SHED`，记录 rank、估算成本、决策前已消耗预算和剩余预算。随后可重新入队并在后续轮次执行，SHED 决策保留供复核。

### 策略版本与级联

- 全量快照必须携带正整数递增版本和 SHA-256 摘要。
- 全量替换要求新版本严格大于 active 版本。
- 增量 patch 必须携带 `base_version`；不等于当前 active 版本时返回 `E_STALE_BASE`，且不写任何状态。
- patch 生成 active+1 新版本和新的 `signature_hash`。
- 受影响算子所在组及其下游组、相关缓冲和等待状态在同一 SQLite 事务中失效。
- 已完成批次在同一事务内用新签名重建 `group_outputs`、`window_results` 和 `group_completions`。
- 旧窗口行标记为 `INVALIDATED` 但保留数值用于审计；active 查询只看新版本的 `COMMITTED` 行。

## 4. 并发与幂等

- 每个分区有独立 `sync.Mutex`，同分区推进串行；不同分区可并行。
- 策略读写使用 `sync.RWMutex`：批次先取策略读锁再取分区锁；策略切换取写锁后执行事务，锁顺序无环。
- 相同 `batch_id` 的并发请求通过 `sync.Map` 单飞；等待者在 channel 关闭后复用同一结果。
- 相同 ID 但 payload hash 不同返回 `E_CONFLICTING_REQUEST`。
- `group_completions` 与 `window_results` 在同一个 SQLite 事务中提交，读者不可能看到“完成标记已存在但结论未落定”的中间状态。
- SQLite 使用 WAL 和 busy timeout，支持多读者与单写事务。

## 5. 意图与强制终止恢复

所有关键状态迁移遵循“先持久化 PENDING 意图，再提交效果事务”：

- `START_BATCH`
- `COMMIT_WINDOW_CONCLUSIONS`
- `BACKPRESSURE_TRANSITION`
- `SWITCH_POLICY`

重启执行 `recover` 时：

- pending `START_BATCH`：对应 `RUNNING` 批次重置为 `QUEUED`。
- pending `COMMIT_WINDOW_CONCLUSIONS`：标记 `DISCARDED`；未提交的窗口行已被 SQLite 事务回滚。
- pending `SWITCH_POLICY`：标记 `ABORTED`，`STAGED` 策略行置为 `ABORTED`，旧 ACTIVE 策略继续可用。
- pending `BACKPRESSURE_TRANSITION`：标记 `ABORTED`，未提交水位迁移不生效。

恢复报告只列数据库中的 pending intent 和实际更新动作，不承诺被 SIGKILL 的窗口一定能自动补出完整结论；批次回到待执行后可重新推进，已提交结论保持终态。

## 6. CLI

```bash
go run ./cmd/feedbackstream init --data-dir data
go run ./cmd/feedbackstream plan --data-dir data
go run ./cmd/feedbackstream demo --data-dir data --reset
go run ./cmd/feedbackstream query windows --data-dir data
go run ./cmd/feedbackstream query shed --data-dir data
go run ./cmd/feedbackstream query bp --data-dir data
go run ./cmd/feedbackstream recover --data-dir data
go run ./cmd/feedbackstream replace-policy --data-dir data --policy-file policy-v2.json
go run ./cmd/feedbackstream patch-policy --data-dir data --patch-file patch.json
```

崩溃注入命令会在写出 PENDING 意图后对自身发送 `SIGKILL`，退出码通常为 137。`batch` 模拟批次开始后中断，`window` 和 `cascade` 会持有未提交 SQLite 事务并在写入部分结论/策略切换后中断，`policy` 模拟 staged policy 已落盘但原子切换前中断：

```bash
go run ./cmd/feedbackstream crash --mode batch --data-dir /tmp/crash-batch
go run ./cmd/feedbackstream recover --data-dir /tmp/crash-batch
go run ./cmd/feedbackstream crash --mode window --data-dir /tmp/crash-window
go run ./cmd/feedbackstream recover --data-dir /tmp/crash-window
go run ./cmd/feedbackstream crash --mode cascade --data-dir /tmp/crash-cascade
go run ./cmd/feedbackstream recover --data-dir /tmp/crash-cascade
go run ./cmd/feedbackstream crash --mode policy --data-dir /tmp/crash-policy
go run ./cmd/feedbackstream recover --data-dir /tmp/crash-policy
```

一键验收：

```bash
./scripts/acceptance.sh
```
