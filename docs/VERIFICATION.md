# 实际验证记录

验证时间：2026-11-25 08:45 CST

环境：

- `go version go1.27.4 linux/amd64`（`go.mod` 声明 `go 1.22`，满足 Go 1.22+ 要求）
- SQLite：`modernc.org/sqlite v1.29.10`，纯 Go，依赖已 vendor
- 未使用 Docker、外部服务或网络数据

## 1. 自动化验证

以下命令均在仓库根目录实际执行通过：

```text
$ go test -count=1 ./...
ok  	feedbackstream	0.418s
?   	feedbackstream/cmd/feedbackstream	[no test files]

$ go vet ./...
# 无输出，退出码 0

$ go test -race -count=1 ./...
ok  	feedbackstream	2.335s
?   	feedbackstream/cmd/feedbackstream	[no test files]
```

详细测试日志保存在 `artifacts/go-test-v.log`；最终命令汇总保存在 `artifacts/final-verification.log`。

通过的测试覆盖：

- `TestSpecValidation`
- `TestPolicyChecksumAndPipelineInvalid`
- `TestFusionPlanAndRepartitionBoundary`
- `TestWindowBoundaryAndNumericOverflow`
- `TestBudgetDeterministicOrder`
- `TestGlobalPartitionConcurrencyLimit`
- `TestBackpressureStallIsCodedAndDoesNotDrop`
- `TestIdempotentSubmitConflictAndConcurrentReaders`
- `TestRecoveryDiscardsWindowIntent`
- `TestDemoEndToEnd`
- `TestSubmitOverflowPropagatesWindowAndGroup`
- `TestFullPolicyReplaceMonotonic`
- `TestEventSnapshotWrapper`
- `TestCLISIGKILLRecovery/batch_intent`
- `TestCLISIGKILLRecovery/window_transaction`
- `TestCLISIGKILLRecovery/cascade_transaction`
- `TestCLISIGKILLRecovery/policy_intent`

离线 vendor 验证：

```text
GOCACHE=<全新临时目录> GOMODCACHE=<空临时目录> GOPROXY=off GOSUMDB=off go build ./...
OFFLINE_FRESH_BUILD_OK
```

## 2. 一键端到端验收

执行：

```bash
./scripts/acceptance.sh
```

实际结果：

```text
demo OK: completed=9 v1_rows=24 v2_rows=6
crash batch: exit=137
recovery batch OK: pending=1
crash window: exit=137
recovery window OK: pending=1
crash cascade: exit=137
recovery cascade OK: pending=1
crash policy: exit=137
recovery policy OK: pending=1
ACCEPTANCE OK
```

脚本依次执行上述四种 SIGKILL 恢复场景，并生成/更新：

- 样本与数据库：`data/events.jsonl`、`data/events-snapshot.json`、`data/policy-v1.json`、`data/stream.db`
- Demo 报告：`artifacts/demo-report.json`
- 窗口结论查询：`artifacts/window-results.json`
- 降载决策：`artifacts/shed-decisions.json`
- 背压事件：`artifacts/backpressure-events.json`
- SIGKILL 前输出：`artifacts/crash-batch.out`、`artifacts/crash-policy.out`
- 恢复报告：`artifacts/recovery-batch.json`、`artifacts/recovery-window.json`、`artifacts/recovery-cascade.json`、`artifacts/recovery-policy.json`

## 3. 本地种子样本复核

固定样本包含 3 个分区、3 个窗口、每窗口 4 条事件，共 36 条事件，归一化为 9 个“分区+窗口”批次。

融合结果：

```text
fg-1 = [scan, filter, project]
fg-2 = [aggregate]
```

边 `scan -> filter`、`filter -> project` 的判定为 `DECLARED_FUSIBLE_PAIR`；`project -> aggregate` 输出 `PAIR_NOT_DECLARED` 与 `MAX_GROUP_SIZE`。若边或算子带跨分区重分布语义，会额外给出 `CROSS_PARTITION_REDISTRIBUTION` 并断开融合。

`data/stream.db` 实际状态：

```text
batches:              COMPLETED=9
policies:             v1=SUPERSEDED, v2=ACTIVE
window_results v1:    24 rows INVALIDATED
window_results v2:    6 rows COMMITTED
shed reasons:         PARTITION_CONCURRENCY_LIMIT=5
                      ROUND_TIME_BUDGET_EXHAUSTED=5
backpressure events: WATERMARK=2, BLOCKED=1
```

首轮预算为 11ms，单批估算成本为 `1 + event_count`，普通 4 事件批次成本为 5ms。确定性顺序为分区号、窗口起点、批次 ID；首轮准入 `batch-p0-w0` 和 `batch-p1-w0`，其余 7 批按并发或预算规则进入 `SHED`。后续重新入队并执行后，9 个批次全部 `COMPLETED`，SHED 决策仍保留在 SQLite。

## 4. 背压复核

Demo 使用 `fg-2.max_inflight_batches=2`：

1. 前两个批次进入 `fg-2`，第二个批次使批次水位达到上限并产生 `WATERMARK`。
2. 第三个批次推送被阻塞，产生一条 `BLOCKED` 事件：`inflight_batches=2/max_batches=2`、`blocked_upstream_batches=1`。
3. 排空一个在途批次后，阻塞批次被重新准入并完成。
4. 所有演示批次最终均为 `COMPLETED`，等待记录保留为 `DONE`，没有删除未处理批次。

专门测试 `TestBackpressureStallIsCodedAndDoesNotDrop` 构造两个融合组均背压且等待队列达到上限，断言返回 `E_BACKPRESSURE_STALL`，队列数量不继续增加。

## 5. 策略版本与级联复核

Demo 以 base version 1 提交增量 patch，收紧 filter 阈值并调整 `fg-2` buffer：

- 新策略版本：v2。
- 旧 v1 的 24 条窗口结论在同一策略切换事务中标记 `INVALIDATED`。
- 已完成批次按 v2 新签名原子重建出 6 条 `COMMITTED` 结论。
- 再次以 base version 1 提交 patch 返回 `E_STALE_BASE`，状态不变。

## 6. SIGKILL 与恢复复核

批次中断：

- `crash --mode batch` 写出 `intent-crash-batch=PENDING`，将 `batch-p0-w0` 置为 `RUNNING`，随后以 `SIGKILL` 终止，shell 记录退出码 137。
- `recover` 扫描到 1 条 pending intent，将 `batch-p0-w0` 从 `RUNNING` 回滚为 `QUEUED`，意图置为 `RECOVERED`。

事务中途中断：

- `window` 模式在未提交事务中写入 `DRAFT` 窗口行和完成标记后收到 SIGKILL；重启恢复扫描到 `intent-crash-window`，报告 `discarded_window_intents=1`，SQLite 中 draft/completion 行数均为 0。
- `cascade` 模式在未提交事务中把 v1 改为 `SUPERSEDED` 并插入 v2 `ACTIVE` 后收到 SIGKILL；事务回滚后只有 v1 `ACTIVE`，恢复将 `intent-crash-cascade-v2` 标记为 `ABORTED`。

策略中断：

- `crash --mode policy` 写出 `intent-crash-policy-v2=PENDING` 和 `STAGED` v2 策略行，随后 `SIGKILL`，退出码 137。
- `recover` 中止该切换，v2 行变为 `ABORTED`，active policy 仍为 v1。

恢复报告只包含已落盘的 pending intent 和更新动作；不承诺被强杀的窗口在未重新推进前自动得到完整结论。
