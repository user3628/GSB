# 实际验证结果

环境：Linux/WSL2，Go 1.27.4，gcc 15.2（CGO），目标兼容 Go 1.22+，Windows 交叉编译通过。

## 自动化测试

- `go test ./...`：全部通过（31 个测试函数，11 个测试文件）。
- `go test -race ./...`：无数据竞争。
- `go vet ./...`：无告警；`gofmt -l`：无未格式化文件。
- `GOOS=windows GOARCH=amd64 go build -mod=vendor ./...` 与 Linux 构建均通过，
  依赖已 vendor，可完全离线构建。

覆盖的验收点：

| 验收点 | 测试 |
| --- | --- |
| 固定种子可复现（PNG 字节 + 清单指纹） | TestGeneratorDeterministic / TestLoadRoundTrip |
| 位姿非正交 / 零焦距 E_POSE_INVALID 带字段名 | TestValidateCamera* |
| 越界 E_SAMPLE_OUT_OF_RANGE、NaN E_NAN_INPUT | TestPixelOutOfRangeAndNaN / TestNaNPixelRejectedAtProjection |
| 零视角 E_EMPTY_VIEWS、密度非正 E_BAD_DENSITY | TestEmptyViewsAndBadDensity |
| 融合确定性、权重归一化、剪枝原因 | TestFusionDeterministicAndNormalized |
| 小场景不选稀疏、大场景不选全量、打分确定 | TestPlan* |
| 增量与全量逐字节一致 | TestFullAndIncrementalByteIdentical |
| 变更后未受影响视角复用且结果仍逐字节一致 | TestIncrementalReuseOnPixelChange |
| E_INCREMENTAL_MISMATCH 附首个差异体素 | TestIncrementalMismatchReportsFirstVoxel |
| 检查点连续序号、回退/篡改/推进分类 | TestCheckpointContiguousSequence / TestCrossShardIndependentCursors |
| 真实文件锁跨进程互斥 E_LOCKED | TestFileLockMutualExclusion / TestTwoServersOneWorkspaceLock |
| 原子写无半写残留 | TestAtomicWriteNoPartial |
| 强杀恢复后摘要/误差/稀疏度逐字节一致 | TestRecoveryAfterStop |
| 延迟预算超时 E_LATENCY_BUDGET 带 query_id/elapsed_ms | TestLatencyBudgetExceeded / TestHTTPLatencyBudgetError |
| 缓存命中与重算逐字节相同、键含三类指纹 | TestCacheHitByteIdentical / TestCacheKeyIncludesAllFingerprints |
| 超差 E_NUMERIC_MISMATCH 带 view_id/max_deviation | TestNumericMismatchReported |
| HTTP 端到端与错误码 JSON/stderr 一致 | TestHTTPEndToEnd / TestHTTPBadConfigBusinessCode |

## 独立进程强杀恢复事实

`scripts/recovery_demo.sh`（输出存于 `artifacts/recovery_demo_output.txt`）：

1. 以独立二进制启动服务，固定种子生成 3 分片 × 3 视角数据集。
2. 先做一次 `force_full` 连续全量重建作为基线。
3. `kill -9` 强制终止进程（模拟硬崩溃），再以新进程重启同一工作区。
4. 自适应重建（实测选择 BLOCK_CACHE，含候选打分与淘汰原因）并开启
   `verify_equality`，返回 `"equivalence":"byte_identical"`。
5. 重启后可正常列出全部检查点；stderr 仅含访问日志，无任何 E_ 业务错误。

Windows 下使用 `scripts/run_server.ps1`，文件锁走 `LockFileEx`，其余行为一致。

## 复现命令

```bash
go test ./...
go test -race ./...
./scripts/recovery_demo.sh
```
