# 实际验证记录

验证日期：2026-11-24（Asia/Shanghai）

环境：

```text
Go 1.27.4 linux/amd64
模块 go 指令：1.22
SQLite 驱动：modernc.org/sqlite v1.34.5
```

依赖缓存已补到本机默认 `GOMODCACHE`，以下证明可在禁止模块代理时工作：

```text
$ GOPROXY=off GOSUMDB=off go mod verify
all modules verified
```

## 单元与集成测试

```text
$ go test ./... -count=1
?   	h2diag/cmd/h2diag	[no test files]
?   	h2diag/cmd/samplegen	[no test files]
ok  	h2diag/internal/api	3.144s
ok  	h2diag/internal/engine	0.007s
?   	h2diag/internal/model	[no test files]
ok  	h2diag/internal/store	0.375s
```

```text
$ go test -race ./... -count=1
?   	h2diag/cmd/h2diag	[no test files]
?   	h2diag/cmd/samplegen	[no test files]
ok  	h2diag/internal/api	17.421s
ok  	h2diag/internal/engine	1.023s
?   	h2diag/internal/model	[no test files]
ok  	h2diag/internal/store	1.575s
```

```text
$ go vet ./...
# 无输出，退出码 0
```

关键测试断言：

- `TestBatchByStreamIsBitIdenticalToFullSubmission`：同一 200 帧样本一次提交与按 50 条声明流加 2 个流 0 分组、共 52 次 HTTP 批次提交，版本 0 到 65 的连接窗口、流窗口、状态、待发送字节、已发送字节、累计舍入误差逐字段相同；60 条轮次轨迹也完全相同。
- `TestConcurrentDuplicateSubmissionAdvancesOnce`：两个并发同内容请求分别得到 `201` 与 `200`，后续重复提交不增加 `commits` 行。
- `TestQueriesObserveOnlyCommittedSnapshotsWhileAdvancing`：推进期间并发查询只返回 `404` 或带有效摘要的完整快照，版本不倒退，最终稳定在版本 65。
- `TestCheckpointCorruptionReturnsBusinessCode`：篡改 SQLite 流窗口后，`/verify` 返回 `E_CHECKPOINT_CORRUPT`。
- `TestSamplingSampleReportsBucketedEstimate`：估计路径产生 12 个采样轮次；每轮包含采样率、桶方法、置信说明、精确顺序、估计顺序和差异原因。
- `TestRecoverAbortsIntentWithoutCommitAtLastBoundary`：意图已落盘、轮次未落盘时，重启回退到最后完整版本并写恢复报告。
- `TestRecoverRepairsCommittedIntent`：提交已落盘、意图未回写时，重启把意图修复为 `COMMITTED`。
- `TestRecoverWithoutIntentReportsCorruptCheckpoint`：无未完成意图但当前检查点摘要不匹配时，恢复报告返回 `E_CHECKPOINT_CORRUPT`。

## 完整调度结果

命令：

```bash
./scripts/demo.sh
```

`samples/baseline.json`：

```json
{
  "frames": 200,
  "event_commits": 5,
  "scheduling_rounds": 60,
  "selections": 288,
  "data_intent_frames": 96,
  "data_bytes_sent": 917252,
  "cumulative_rounding_error_ppm": 128,
  "sampling_rounds": 0,
  "exact_rounds": 60
}
```

最终版本 `65`，两个连接的窗口均为 `3,997,822`，所有流 `pending_bytes=0`。50 条流的状态分布：

```text
HALF_CLOSED_LOCAL = 36
CLOSED            = 12
RESET             = 2
```

两个 RESET 流（每连接流 ID 250）的 `total_sent` 均为 0。完整响应、快照和 60 条轮次轨迹保存在：

```text
artifacts/demo/baseline-response.json
artifacts/demo/baseline-snapshot.json
artifacts/demo/baseline-rounds.json
```

## 流控阻塞

`samples/blocked.json` 返回 HTTP `409`：

```json
{
  "code": "E_FLOW_CONTROL_BLOCKED",
  "message": "DATA demand remains blocked by flow-control windows",
  "context": {
    "connection_id": "loopback",
    "stream_id": 1,
    "connection_window_remaining": 0,
    "stream_window_remaining": 64511,
    "pending_bytes": 3072,
    "requested_bytes": 4096
  }
}
```

随后查询 `/v1/samples/blocked-v1` 得到 `404`，证明失败批次未推进任何持久化状态。

## 流重置

`samples/reset.json` 的 DATA 与 RST 位于同一时间批次。调度前 RST 清空待发送队列，最终：

```text
state             = RESET
pending_bytes     = 0
total_sent        = 0
data_bytes_sent   = 0
scheduling_rounds = 0
```

## 业务错误码

演示脚本实际观察到：

```text
E_WINDOW_OVERFLOW      HTTP 409
E_INVALID_TRANSITION   HTTP 400
E_FRAME_INVALID        HTTP 400
E_CONFLICTING_SAMPLE   HTTP 409
E_FLOW_CONTROL_BLOCKED HTTP 409
```

`E_CHECKPOINT_CORRUPT` 由集成测试覆盖并返回 HTTP 500。

## 采样路径

`samples/sampling.json` 使用 12 条同级流和 `scheduling_threshold=8`。一次实际结果为：

```json
{
  "version": 65,
  "scheduling_rounds": 60,
  "sampling_rounds": 12,
  "exact_rounds": 48,
  "selections": 284,
  "data_bytes_sent": 917252,
  "cumulative_rounding_error_ppm": 124
}
```

首个采样轮次的关键审计字段：

```json
{
  "active": true,
  "eligible_streams": 12,
  "sampling_rate_ppm": 500000,
  "sampling_rate": "6/12 streams",
  "bucket_method": "log2(weight) buckets; deterministic stride sample",
  "exact_order": [6, 12, 5, 11, 10, 4, 9, 3, 8, 2, 7, 1],
  "estimated_order": [12, 11, 5, 10, 4, 9, 3, 8, 7, 2, 1],
  "difference_reason": "order differs because bucketed path extrapolates per-log2(weight) demand from a stride sample before capped allocation; exact_order is the per-stream audit baseline"
}
```

虽然该轮顺序与精确路径不同，但最终 917,252 字节全部发送，所有流 `pending_bytes=0`。

## 强制终止与恢复

演示在首轮意图提交后由服务进程向自身发送 `SIGKILL`。实际观察：

```text
curl_rc       = 52 (Empty reply from server)
wait_rc       = 137
intent_status = ABORTED
outcome       = ABORTED_AT_LAST_COMPLETE_BOUNDARY
last_complete_version = 3
checkpoint_digest_valid = true
```

恢复后快照版本为 3：

- 连接窗口回到初始值 `262,144`；
- 已排队 DATA 仍为 `pending_bytes`，`total_sent=0`；
- 没有出现连接窗口已扣减但流状态已关闭的混合版本；
- 恢复报告的 `recomputable_snapshot` 包含完整连接和流状态，可直接复算；
- 报告明确写入 `exit_report_guaranteed=false`。

产物：

```text
artifacts/demo/recovery-report.json
artifacts/demo/recovered-snapshot.json
```

## 结论

- 固定样本的调度顺序、窗口余量、权重份额和累计舍入量可重复复算。
- 完整提交与按流分批提交在全部 66 个版本上逐字段一致。
- 阻塞、溢出、非法迁移、冲突和检查点损坏均返回统一 JSON 业务码。
- 单写者推进、多读者查询与强制终止恢复路径均已实际验证。
