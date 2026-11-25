# 验证记录

环境：Linux 容器（交付目标为 Windows；代码仅依赖标准库 + 纯 Go sqlite 驱动，无 cgo/容器）。
工具链：`go version` go1.27.4（`go.mod` 声明 `go 1.21`，兼容任务最低版本）；`sqlite3` CLI 3.46.1。

## 自动化测试（30 个用例，全部 PASS）

- `go test -count=1 ./...`：engine / httpapi / model 三个包全部 `ok`。
- `go test -race -count=1 ./...`：无数据竞争。
- `go vet ./...` 干净；`gofmt -l` 无输出；engine 包语句覆盖率 72.2%。

覆盖到的验收点与对应用例：

| 业务要求 | 测试用例 |
| --- | --- |
| 1 校验/归一化、整批不入库 | `TestValidateServices`、`TestValidateObservations`、`TestInvalidBatchPersistsNothing` |
| 2 指纹确定性、闭区间窗口、窗口外新开 | `TestFingerprintDeterministic`、`TestWindowBoundaryInclusive`、`TestDedupWindowIsMin` |
| 3 多规则抑制/主原因/全命中、状态机、非法迁移不落库 | `TestSuppressionMultiRulePrimaryAndHits`、`TestStateTransitions`、`TestTransitionMatrix` |
| 4 定点评分、容差平局、舍入审计、整数跳数/时差 | `TestRootCauseFixedPointAndRanking`、`TestToleranceTieBreaksLexicographically`、`TestComputeScoreRoundingAudit`、`TestRoundHalfAwayFromZero`、`TestEquivalentWithTolerance` |
| 5 stale base、幂等复用/冲突、版本单调与摘要 | `TestStaleBaseRejected`、`TestIdempotentRequestReuseAndConflict`、`TestSnapshotMonotonicAndChecksum` |
| 6 级联失效原子重建、引用一致性 | `TestAtomicRebuildConsistency`、`TestTopologyHopsDeterministic` |
| 7 采样/完整逐条差异、确定性采样、根因同依据 | `TestSampledVsFullCrossCheck`、`TestSampledRootCausesReuseSameEvidence`、`TestStrideSamplerBounds` |
| 8 预算降级次序、队列满、并发只读、崩溃恢复 | `TestPatrolBudgetDegradationDeterministic`、`TestPatrolQueueFull`、`TestConcurrentReads`、`TestCrashRecoveryAfterIngestIntent`、`TestRecoveryNoPendingIntentsIsClean` |
| HTTP 回环/端口占用显式报错 | `TestHealthAndIngestOverLoopback`、`TestStaleBaseReturnsBusinessCode`、`TestBindFailureExplicit` |

## CLI 端到端（固定种子，确定性）

`alarmengine demo` 一次运行产出：80 条观测 → 34 条告警（17 抑制）→ 34 条抑制命中 →
65 条根因评分；采样路径（limit=20）与完整路径输出逐条 id 差异与共享根因键；
快照 v2→v3 原子替换并重建 34 告警/65 评分；巡检轮输出降级次序与剩余预算。

## 强制终止恢复（外部参数注入，未改源码）

| 崩溃点 | 崩溃时 SQLite 事实 | 重启恢复结果 |
| --- | --- | --- |
| `CRASH_AFTER_INTENT_INGEST` | 意图 `STARTED`、observations=0、v1 快照完好 | 回滚悬挂 v2，`last_complete=1`，请求台账协调后可重提成功 |
| `CRASH_AFTER_INTENT_SNAPSHOT` | `REPLACE_SNAPSHOT` 意图 `STARTED`、仅 v1 快照 | 删除悬挂快照，保持 v1，重新提交到 v3 成功 |
| `CRASH_MID_REBUILD` | 重建事务整体未提交（无 STARTED 残留） | 重启无悬挂意图，重跑成功，34 告警/65 评分落盘 |

崩溃进程退出码固定为 97；恢复报告只引用 `intents`/`meta` 中的已落盘事实。

## 回环 HTTP 实测

- 端口被占用时返回 `E_BIND_FAILED`（在 18080 上实测到环境占用并显式报错）。
- 空闲高位端口（18123）启动成功；`send` 首次提交返回 v2，重复同 request id 复用同一结果，
  同 id 不同载荷返回 `E_CONFLICTING_REQUEST`(409)，过期 base 返回 `E_STALE_BASE`(409)。

## 复现命令

```bash
go test -race -count=1 ./...
go run ./cmd/alarmengine demo --db /tmp/ae.db
go run ./cmd/alarmengine demo --db /tmp/crash.db --crash-point CRASH_AFTER_INTENT_INGEST # 退出码 97
go run ./cmd/alarmengine demo --db /tmp/crash.db                                          # 触发恢复
sqlite3 /tmp/crash.db "SELECT intent_id,kind,status FROM intents;"
```
