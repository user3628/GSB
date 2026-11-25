# 实际验证记录

验证日期：2026-11-25（Asia/Shanghai）。验证环境为 Linux 沙箱中的 Go 1.27.4；模块声明 `go 1.22`，SQLite 使用纯 Go 驱动，无 cgo。由于当前沙箱不是 Windows 主机，未在本机执行 Windows 二进制；代码路径、文件路径处理、脚本和依赖均面向 Windows 10+/Go 1.22。

## 1. 完整测试与静态检查

```text
$ go test ./...
?       local/eventbus/cmd/demo        [no test files]
?       local/eventbus/cmd/eventbus    [no test files]
ok      local/eventbus/internal/agg
ok      local/eventbus/internal/bus
ok      local/eventbus/internal/httpapi
?       local/eventbus/internal/sample [no test files]

$ go vet ./...
<无输出>
```

竞态检测：

```text
$ go test -race ./...
ok      local/eventbus/internal/agg
ok      local/eventbus/internal/bus
ok      local/eventbus/internal/httpapi

Windows 目标交叉编译：

```text
$ GOOS=windows GOARCH=amd64 go build ./...
<无输出，成功>
```
```

## 2. 覆盖的验收场景

| 场景 | 测试 |
|---|---|
| 生产序号无缺口、确定性路由、事件幂等、内容冲突 | `TestPublishIdempotencyGaplessRoutingAndConflict` |
| 并发拉取单租约、租约超时原序号重投、乱序确认不改状态 | `TestLeaseConcurrencyExpiryAndOrderedRedelivery` |
| 多消费者组独立租约 | `TestMultipleGroupsCanConcurrentlyHoldIndependentLeases` |
| 五种状态与完整合法迁移矩阵 | `TestAllowedTransitionMatrix` |
| 死信终结投递、位点推进且不计入聚合 | `TestDeadLetterTransitionsAndAdvancesOffsetWithoutAggregation` |
| 非法迁移无持久化副作用 | `TestInvalidTransitionLeavesPersistedStateUntouched` |
| 三种算法、消息级精确参考和声明误差上界 | `TestErrorBoundsCoverActualErrors`、`TestDeclaredBoundsHoldAcrossRandomWideMagnitudeSamples`、`TestThreeSumAlgorithmsBoundsAndBatchBoundaryIndependence` |
| 分批/整批聚合逐位一致 | `TestBatchBoundariesDoNotChangeState`、`TestThreeSumAlgorithmsBoundsAndBatchBoundaryIndependence` |
| 回拨聚合回退与直接消费逐位一致、重放期间暂停新事件 | `TestRewindReplayAndAggregateRollbackMatchDirectConsumption` |
| 回拨点切穿确认批次时账本仍覆盖保留前缀 | `TestRewindInsideAckBatchRetainsLedgerPrefix` |
| 越界回拨不修改状态、过期位点错误码 | `TestRewindValidationDoesNotMutate` |
| 同请求并发确认只执行一次、内容冲突 | `TestConcurrentDuplicateAckExecutesOnce` |
| 请求幂等结果跨进程重启复用 | `TestRequestIdempotencySurvivesRestart` |
| 多读者观察完整提交版本 | `TestConcurrentReadersObserveWholeCommittedVersions` |
| 事务内模拟终止后的意图回滚/提交识别 | `TestRecoveryRollsBackUncommittedAckAndCommittedAckSurvives` |
| 真实子进程强制退出后的恢复 | `TestForcedProcessTerminationRecovery` |
| 聚合溢出拒绝提交且无部分状态 | `TestAggregateOverflowAbortsAckWithoutPartialState` |
| HTTP 成功/失败统一 JSON、主流程、幂等头、死信 | `TestHTTPPublishPullAckRewindQueriesAndErrors` |

强制终止测试在子进程中于 `ack_after_intent` 或 `ack_before_commit` 阶段直接退出（退出码 42）。重启后两项均断言：

- `pending_scanned=1`、`rolled_back=1`。
- 位点仍为 0，租约已释放。
- `acks=0`、`agg_batches=0`、`agg_states=0`。
- 原序号 1 重投，`delivery_count=2`。

另测 `ack_after_commit` 场景：恢复报告识别为已提交（`committed_recovered=1`），位点与聚合计数不回滚。

## 3. 固定种子演示

命令：

```powershell
go run ./cmd/demo -seed 20261125 -events 64
```

关键结果（完整 JSON 可由命令重新生成）：

```json
{
  "seed": 20261125,
  "event_count": 64,
  "partitions": [
    {"partition": 0, "events": 27, "first_seq": 1, "last_seq": 27},
    {"partition": 1, "events": 37, "first_seq": 1, "last_seq": 37}
  ],
  "algorithms": [
    {"algorithm": "naive_sequential", "split_bulk_bitwise_equivalent": true, "absolute_error_bound": 0.012707158854873741},
    {"algorithm": "kahan_compensated", "split_bulk_bitwise_equivalent": true, "absolute_error_bound": 0.003632212701625318},
    {"algorithm": "magnitude_buckets", "split_bulk_bitwise_equivalent": true, "absolute_error_bound": 0.007264425403250633}
  ],
  "rewind": {
    "partition": 0,
    "target_seq": 13,
    "full_offset": 27,
    "reverted_batches": 14,
    "matches_direct_consumption_at_target": true,
    "matches_pre_rewind_full_state": true
  },
  "error_ledger_batches": 34,
  "validation_passed": true
}
```

判断：

- 示例样本同时覆盖两个热点分区键和长尾键，分区事件数分别为 27 和 37。
- 三种算法下，“逐条确认”和“一次确认 27 条”的状态 SHA-256 摘要分别完全相同。
- 回拨到分区 0 的序号 13 后，聚合摘要与独立基线组直接消费到 13 的摘要完全相同。
- 重放到序号 27 后，摘要与回拨前完整状态完全相同。
- 误差账本保留 34 个批次；账本可聚合绝对上界为 `0.04358655241950386`。
- 演示中三种算法的相对误差界分别约为 `3.61e-15`、`1.03e-15`、`2.06e-15`。

## 4. 复现命令

```powershell
go mod download
go test ./...
go test -race ./...
go run ./cmd/demo -seed 20261125 -events 64
```

一次演示生成的 SQLite 文件位于操作系统临时目录，演示进程退出时删除。
