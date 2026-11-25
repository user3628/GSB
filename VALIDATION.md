# 实际验证结果

验证时间：2026-11-25（Asia/Shanghai）。

## 环境

```text
$ go version
go version go1.27.4 linux/amd64
```

依赖：`modernc.org/sqlite v1.29.10`（纯 Go，无 CGO SQLite 驱动），模块文件见 `go.mod` / `go.sum`。

## 已执行命令

```bash
GOPROXY=off CGO_ENABLED=0 go vet ./...
GOPROXY=off CGO_ENABLED=0 go test ./... -count=1
GOPROXY=off CGO_ENABLED=0 go test ./... -count=1 -v
GOPROXY=off go test -race ./internal/engine ./internal/httpapi -count=1
GOOS=windows GOARCH=amd64 GOPROXY=off CGO_ENABLED=0 go build ./...
GOPROXY=off CGO_ENABLED=0 go build -o /tmp/mvcc-demo-final ./cmd/demo
/tmp/mvcc-demo-final > artifacts/demo-run-local.log 2>&1
```

## 结果

- `go vet ./...`：通过，无输出。
- `CGO_ENABLED=0 go test ./... -count=1`：全部包 `ok`。
- 详细日志：`artifacts/final-go-test-verbose.log`。
- 竞态检测：`internal/engine` 与 `internal/httpapi` 均 `ok`，日志：`artifacts/final-race.log`。
- Windows amd64 纯 Go 交叉构建：`GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...` 通过，记录：`artifacts/final-windows-build.log`。
- 演示：退出码 `0`，输出末尾为 `DEMO_OK: all local MVCC scenarios passed`。
- 演示报告：`artifacts/demo-run-local.log`，共 17 项检查，全部 `ok=true`。
- 六类必需错误码均由本地 HTTP/演示路径覆盖：
  - `E_TXN_INVALID`
  - `E_VERSION_INVALID`
  - `E_VERSION_CHAIN_BROKEN`
  - `E_SNAPSHOT_INVALID`
  - `E_RECLAIM_UNSAFE`
  - `E_CONFLICTING_REQUEST`

## 验收语义对应

- 快照内可见性：`TestVisibilityReasonsSnapshotAndDelete` 验证命中 v2、自修改未提交、回滚、快照外提交和删除遮蔽；重复读取比较 canonical JSON。
- 样本整批校验：`TestInvalidSampleBatchIsNotPersisted` 验证事务序号缺口和非法摘要/空 row key 不入库。
- 版本链：`TestChainForkDetectAndRepair` 验证分叉点 `17`、尖端 `[18 19]`、错误码和修复后重新校验通过。
- 快照切换：`TestSnapshotSwitchValidationAndAffected` 验证坏摘要保持旧快照、序号单调、整体切换和影响的 3 个活跃事务。
- 强制终止恢复：`TestRecoveryAndReclaimSemantics` 与 demo 子进程 SIGKILL 验证 pending commit intent 扫描、事务 3 ABORTED、版本 3 回滚、行头重置和重启前后命中版本/摘要一致。
- 未完成写/快照意图：`TestRecoveryPendingWriteIntentDeletesVersion`、`TestRecoveryPendingSnapshotIntentKeepsOldSnapshot`。
- 并发与幂等：`TestConcurrentIdempotentCommit`、`TestConcurrentBeginWriteCommitUniqueRows` 和 `-race`。
- 长事务回收：`TestRecoveryAndReclaimSemantics` 验证最早阻塞快照 2、剩余引用 1、结束长事务后移除 v1，且回收前后读取投影不变。
- HTTP：`TestHTTPTransactionWriteCommitRead`、`TestHTTPRequiredErrorCodes` 覆盖主路径和六类业务错误码。
