# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）

运行环境：

```text
Go:        go1.27.4 linux/amd64（满足 Go 1.22+）
SQLite:    modernc.org/sqlite v1.34.5（纯 Go，无 CGO/外部服务）
模块缓存:  /home/zsl/.feedback/runtime-home/.../go/pkg/mod
GOPROXY:   https://goproxy.cn,direct
HTTP:      127.0.0.1:18190（配置文件读取）
```

工作区中原先占用的 `18090` 被环境内其他进程使用，因此默认端口调整为 `18190`；`config.json` 和 `internal/config` 默认值已同步。演示也支持通过配置或 `PORT` 更换。

## 命令结果

| 命令 | 结果 |
|---|---|
| `go test ./... -count=1` | PASS |
| `go test -race ./... -count=1` | PASS，无数据竞争 |
| `go vet ./...` | PASS，无输出 |
| `GOOS=windows GOARCH=amd64 go build ./cmd/routerd` | PASS，PE32+ x86-64 |
| `GOOS=windows GOARCH=amd64 go build ./cmd/routerdemo` | PASS，PE32+ x86-64 |
| `NO_COLOR=1 ./scripts/demo.sh` | PASS，四段演示 + 五码矩阵 |
| `go run ./cmd/routerdemo` | PASS，纯 Go 跨平台端到端演示 |

原始日志：

- `artifacts/validation/go-test.log`
- `artifacts/validation/race.log`
- `artifacts/validation/go-vet.log`
- `artifacts/validation/windows-build.log`
- `artifacts/validation/coverage.log`
- `artifacts/validation/demo.log`（Bash）
- `artifacts/validation/go-demo.log`（纯 Go）

## 全量测试输出

```text
?   	example.com/gsb-router/cmd/routerd	[no test files]
?   	example.com/gsb-router/cmd/routerdemo	[no test files]
ok  	example.com/gsb-router/internal/config	0.004s
?   	example.com/gsb-router/internal/errs	[no test files]
ok  	example.com/gsb-router/internal/httpapi	0.347s
ok  	example.com/gsb-router/internal/model	0.005s
ok  	example.com/gsb-router/internal/routing	0.005s
ok  	example.com/gsb-router/internal/sample	0.011s
ok  	example.com/gsb-router/internal/store	0.897s
```

Race 输出同样全部 `ok`，HTTP/Model/Routing/Store 分别约 1.9/1.0/1.0/3.6 秒。

覆盖率（`go test -cover ./internal/...`）：

```text
config   87.5%
httpapi  54.1%
model    60.4%
routing  78.9%
sample   90.4%
store    72.3%
```

共 37 个自动化测试函数，全部通过。

### 配置与样本生成

- `TestLoadResolvesDatabaseRelativeToConfig`
- `TestLoadRejectsUnknownAndEmptyFields`
- `TestGeneratedSamplesAreDeterministic`
- `TestWriteFilesProducesParseableJSON`

验证点：端口/数据库从配置读取；相对数据库路径按配置目录解析；未知字段和空 listen 被拒绝；样本生成器重复调用完全一致；六个样本文件均为可解析 JSON。

## 自动化用例

### 模型与摘要

- `TestSnapshotV1ValidatesAndIsDeterministic`
- `TestChecksumMismatchRejected`
- `TestMissingPhysicalTableUsesSpecificCodeAndCategory`
- `TestOverlappingRangesRejected`
- `TestDuplicateLogicalTableRejected`

验证点：固定样本可校验；规范化摘要重复计算一致；摘要错误返回 `E_SNAPSHOT_INVALID`；映射缺失返回 `E_PHYSICAL_TABLE_MISSING` 且类别为 `E_SNAPSHOT_INVALID`；区间重叠返回 `E_RANGE_OVERLAP`；重复逻辑表被拒绝。

### 路由

- `TestEqualAndInRouting`
- `TestModRangeCrossesResidueBoundaryExactly`
- `TestRangeBoundarySemanticsAndEmptyRange`
- `TestHashRoutingDeterministicAndConservativeRange`
- `TestMergedLogicalTablesSharePhysicalTable`

实际断言包括：

- `orders / user_id=5 / mod 4` -> `db0.orders_01`；
- IN `[0,1,5]` -> `db0.orders_00`, `db0.orders_01`；
- `orders` 键范围 `[3,5]` 跨模边界 -> 槽位 `3,0,1`，精确得到 `db0.orders_00`, `db0.orders_01`, `db1.orders_03`；
- `ledger [999,1000)` 仅命中 `db2.ledger_00`；`(999,1000]` 仅命中 `db2.ledger_01`；
- 倒置空区间返回空集合；
- hash 等值查询重复 100 次逐项一致；hash 范围显式 conservative 返回全部 4 个分片；
- `audit_eu` 与 `audit_us` 均映射到 `db0.audit_shared_00`。

### 快照、增量与版本读

- `TestSnapshotCommitAndVersionBoundRead`
- `TestInvalidSnapshotLeavesOldStateUsable`
- `TestIncrementStaleReadAndReplacement`
- `TestIncrementIdempotencyAndConflict`
- `TestConcurrentSameBaseOnlyOneWins`
- `TestIncrementBatchRollsBackAtomically`
- `TestUnaffectedTableServesOldCacheVersionAtCurrentVersion`
- `TestConcurrentSameCommitIDReusesResult`
- `TestConcurrentReadsNeverObservePartialCommit`
- `TestLogicalTableAndAggregateCanBeAddedIncrementally`
- `TestShardKeyAndAlgorithmChangeCascade`
- `TestSnapshotPersistsAcrossReopen`

实际断言包括：

- v1 -> v2 后，`observed=1` 得到 `E_STALE_READ`，上下文含 `cache_version=2`、`observed_version=1`、`table_id=orders`；
- v2 后 `orders/user_id=1` 从 `db0.orders_01` 变为 `db0.orders_00`；
- 同 `commit_id` 同内容重放返回 `reused=true` 且不新增提交；不同内容返回 `E_CONFLICTING_REQUEST`；
- 同一基础版本两个并发增量恰好一个成功、一个 `E_STALE_BASE`；
- 同一 commit_id 并发两次只落一条提交回执，一个执行、一个复用；
- 合法 `orders` 变更 + 非法 `ledger` 重叠区间必须在整批校验中失败，快照版本、缓存和路由均保持 v1；
- v2 后未变 `events` 在 `observed=2` 时可用，并标注 `used_version=1`；
- 提交期间并发读取只能看到完整 v1 或得到版本替换裁决，不会读到新表状态配旧缓存；
- 增量可新增逻辑表与聚合视图，也可把 `events` 从 hash 改为 mod，并只级联重建其下游聚合；
- 关闭并重开 Store 后，快照与 hash 路由仍可用。

### 缓存、意图与恢复

- `TestCacheCascadeIsAtomic`
- `TestPropagationLagReturnsStaleRead`
- `TestRebuildInterruptIntentAndRecoveryAbort`
- `TestRecoveryRebuildsDriftedCaches`
- `TestCleanReopenAfterIncrementDoesNotFabricateRecovery`

实际断言包括：

- v2 只改 `orders` 后：`orders` 与下游 `order_overview` 缓存版本为 2；`order_items`、`events`、`audit_rollup` 保持 1；
- `orders` 物理表清单从 4 个变为 3 个，边界索引为 `db0.orders_00`, `db1.orders_02`, `db1.orders_03`；
- 人工制造 `expected=3 > cache=2` 后，当前版本读得到 `E_STALE_READ`，原因为 `cache_propagation_incomplete`；
- `rebuild_interrupt` 后主版本仍为 1、pending 意图存在；重开 Store 后意图 `aborted`，恢复报告 `pending=1`，v1 路由仍可读；
- 人工制造缓存边界漂移并留下 pending 意图，重开后报告 `rebuilt_current_snapshot`，缓存从最后完整快照版本恢复。
- 正常增量后干净重开不会伪造恢复报告，也不会把未变表的缓存版本改写成当前版本。

### HTTP 与异常码

- `TestUIHealthAndSnapshotFlow`
- `TestFiveFaultCodesAreDistinctAndStateSafe`
- `TestHTTPStaleReadContext`
- `TestMalformedSnapshotJSONUsesSnapshotInvalidCode`
- `TestDirectSnapshotJSONAccepted`
- `TestIncrementFaultWithOriginalChecksumKeepsFaultCode`

五码矩阵实际结果：

```text
E_PHYSICAL_TABLE_MISSING
E_RANGE_OVERLAP
E_STALE_BASE
E_OBSERVED_AHEAD
E_REBUILD_INTERRUPTED
```

五个码互不相同，错误体均含 `context`。整份快照 JSON 语法错误返回 `E_SNAPSHOT_INVALID`；不带包装的 `snapshot-v1.json` 可直接提交；带原摘要的增量注入仍返回目标故障码而不是被摘要错误遮蔽。执行五类注入后已提交版本仍为 1，v1 查询继续成功。

## 端到端演示输出

纯 Go 演示实际输出：

```text
== 0. build binary and generate fixed-seed JSON samples ==
  [ok] snapshot v1 committed
  [ok] orders user=1 -> db0.orders_01; repeated compute identical

== 2. submit v2 increment affecting only orders ==
  [ok] increment v2 committed; affected tables=[orders]
  [ok] orders and order_overview cascaded atomically; unrelated caches retained

== 3. replaced-version read is rejected ==
  [ok] observed v1 -> E_STALE_READ cache_version=2 observed_version=1
  [ok] observed v2 -> db0.orders_00 used_version=2

== 4. persist intent, force termination, restart and recover ==
  [ok] received E_REBUILD_INTERRUPTED after intent persistence
  [ok] server terminated abnormally: signal: killed
  [ok] recovery report: pending=1 aborted_incomplete current_version=2
  [ok] snapshot v2 remains active; interrupted v3 intent is aborted

== 5. five-code injection matrix ==
  [ok] distinct codes: E_PHYSICAL_TABLE_MISSING, E_RANGE_OVERLAP, E_STALE_BASE, E_OBSERVED_AHEAD, E_REBUILD_INTERRUPTED

== demo complete ==
  [ok] final current version=2
```

Bash 演示额外直接检查中断后数据库事实：

```text
server terminated with non-zero status 137; port should now be closed
on-disk facts: pending intents=1, last complete snapshot=2
```

## 复现步骤

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...

# 纯 Go，一条命令复现全部端到端场景
go run ./cmd/routerdemo

# 或 Bash 版本
NO_COLOR=1 ./scripts/demo.sh

# Windows 目标交叉编译确认
GOOS=windows GOARCH=amd64 go build -o .tmp/routerd.exe ./cmd/routerd
GOOS=windows GOARCH=amd64 go build -o .tmp/routerdemo.exe ./cmd/routerdemo
```

演示默认使用 `18190`。端口占用时可修改配置，Bash 使用 `PORT=18191 ./scripts/demo.sh`，Go 演示使用 `go run ./cmd/routerdemo -port 18191`。
