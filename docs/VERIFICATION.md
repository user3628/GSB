# 验证记录

执行日期：2026-11-24。工作区执行器：WSL2 Linux x86-64；目标交付为 Windows 本地回环服务。当前主机没有 Windows 进程运行器，因此 Windows 侧完成了 `GOOS=windows GOARCH=amd64` 的生产二进制与测试二进制交叉编译，PowerShell 一键脚本需在 Windows 主机上直接运行。

## 1. 环境

```text
$ go version
go version go1.27.4 linux/amd64
$ gcc --version | head -1
gcc (Ubuntu 15.2.0-15ubuntu1) 15.2.0
$ sqlite3 --version
3.46.1 2024-08-13 ...
$ go env GOOS GOARCH CGO_ENABLED
linux
amd64
1
```

`go.mod` 声明 `go 1.22`；SQLite 驱动固定为 `modernc.org/sqlite v1.29.10`。实际运行不依赖 gcc，因为该驱动为纯 Go；gcc 仅作为任务环境检查项存在。

## 2. 离线依赖

首次下载后执行：

```text
$ go mod download
$ go mod verify
all modules verified
```

运行和测试不需要外部账号、外部 HTTP 服务或线上样本。模块下载只发生在准备阶段。

## 3. 自动化测试

```text
$ go test ./... -count=1
ok  gatewayfreshness.local/workbench/internal/config   0.003s
ok  gatewayfreshness.local/workbench/internal/freshness 0.004s
ok  gatewayfreshness.local/workbench/internal/httpapi  0.525s
ok  gatewayfreshness.local/workbench/internal/store    0.843s
?   gatewayfreshness.local/workbench/cmd/samplegen      [no test files]
?   gatewayfreshness.local/workbench/cmd/workbench      [no test files]
?   gatewayfreshness.local/workbench/internal/service   [no test files]
?   gatewayfreshness.local/workbench/samples            [no test files]
```

通过项包括：

- `TestTimestampBoundaryIsInclusive`：`-500` 和 `+500` 通过，`-501` / `+501` 分别返回旧/未来错误。
- `TestValidateBatchRejectsWholeBatch` 与 `TestValidateBatchWeightOverflow`：非法 nonce、负时间戳、非正权重、权重 int64 溢出、重复 request id 等。
- `TestNonceReplayAndReclaim`：首次占用、重放剩余窗口、不同客户端隔离、到期回收后复用。
- `TestReclaimRespectsInFlightIntentReference`：存在其他 pending 意图引用时不回收；引用结束后才回收。
- `TestIdempotencyAndConflict`：20 路并发同内容提交得到 1 次写裁决与 19 次复用，同 ID 异内容 409 且旧结果不变。
- `TestFastModeDoesNotSilentlyPassSuffix`：前缀 2/4、采样率 0.5、后缀 `not_prechecked`、只占用 2 个 nonce。
- `TestSnapshotBudgetPartialNeverMixesVersions`：零预算无数据但有版本标签；历史版本只出现对应记录。
- `TestInconsistencyDetailsAcrossPickedVersions`：`reader-old@v1` 与 `reader-current@v3` 得出不同 nonce 结论并输出差异字段。
- `TestForcedTerminationRecovery/after_intent`、`/after_writes`：真实测试子进程分别在意图落盘后和写事务中途 `os.Exit`，父进程重开库后恢复。
- 恢复测试还验证中止的计划版本形成空洞，重试提交获得下一个完整版本，空洞版本查询被拒绝。

## 4. Race 与静态检查

```text
$ go vet ./...
(no output; exit 0)

$ go test -race ./... -count=1
ok  gatewayfreshness.local/workbench/internal/config   1.012s
ok  gatewayfreshness.local/workbench/internal/freshness 1.010s
ok  gatewayfreshness.local/workbench/internal/httpapi  1.624s
ok  gatewayfreshness.local/workbench/internal/store    2.482s
```

## 5. Windows 目标构建

```text
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o .build/workbench-windows-amd64.exe ./cmd/workbench
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o .build/samplegen-windows-amd64.exe ./cmd/samplegen
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o .build/win-tests/store.test.exe ./internal/store
...
$ file .build/workbench-windows-amd64.exe
.build/workbench-windows-amd64.exe: PE32+ executable for MS Windows 10.00 (console), x86-64
```

Windows 二进制包括工作台服务与样本生成器；`freshness/config/store/httpapi` 的测试二进制也完成了 Windows 编译。Windows 主机上的最终命令为：

```powershell
.\scripts\verify.cmd
.\scripts\demo.cmd
.\scripts\crash-recovery-demo.cmd
```

## 6. 本地 HTTP 样本冒烟结果

使用临时库按顺序提交样本，实际结果：

| 样本 | 版本 | 结果 | 关键证据 |
| --- | ---: | --- | --- |
| `01-sample-accept-001.json` | 1 | accepted / OK | 2 条 accepted；tolerance 边界覆盖签名一致 |
| `02-sample-boundary-002.json` | 2 | rejected / E_TIMESTAMP_AHEAD | `+500` accepted，`+501` boundary=upper；`-500` accepted，`-501` boundary=lower |
| `03-sample-signature-003.json` | 3 | rejected / E_SIGNATURE_MISMATCH | 签名阶段失败 |
| `04-sample-replay-004.json` | 4 | rejected / E_NONCE_REPLAYED | first_occupied=1800000000000，remaining=4900 |
| `05-sample-fast-005.json` | 5 | partial / E_PRECHECK_INCOMPLETE | sample_rate=0.5；后 2 条 `not_prechecked` |
| `06-sample-reclaim-base-006.json` | 6 | accepted / OK | 窗口到期后重新占用 nonce |
| `07-sample-reclaim-trigger-007.json` | 7 | accepted / OK | 触发 06 nonce 到期回收 |
| `08-sample-reoccupy-008.json` | 8 | accepted / OK | 同一客户端/nonce 再次首次占用 |
| `10-sample-mode-full-010.json` | 10 | accepted / OK | 4 条 accepted；fast 投影差异数=2 |
| `11-sample-invalid-nonce.json` | 未消耗 | HTTP 400 E_REQUEST_INVALID | 之后快照版本仍为 0（独立测试） |
| `invalid-weight-overflow.json` | 未消耗 | HTTP 400 E_REQUEST_INVALID | int64 解码溢出 |
| `conflict-same-id-001.json` | 不新增 | HTTP 409 E_CONFLICTING_REQUEST | 持久化的 batch 01 content hash/version 不变 |

幂等重复提交 `01`：HTTP 200、`window_version=1`、`idempotent_replay=true`。

模式差异：完整批次 `10` 的 `mode-differences` 返回 `precheck_limit=2`、`fast_sample_rate=0.5`、`predicted_fast_outcome=partial`，第 3/4 条 fast 结果为 `not_prechecked`，差异数 2。

版本不一致：对批次 `06` 使用：

```json
{
  "views": [
    {"client_id": "reader-old", "picked_window_version": 6},
    {"client_id": "reader-new", "picked_window_version": 8}
  ]
}
```

结果为 `reader-old` 不变（OK），`reader-new` 因版本 8 可见的后续 nonce 占用而显示 `E_NONCE_REPLAYED`；差异字段为 `items[0].code`，包含客户端、拾取版本与实际/视图值。

快照：正常 40ms 预算返回 `complete=true`、三个区段和统一版本号；显式 `budget_ms=0` 返回 `partial=true`、`completed_sections=[]`、同一 `snapshot_window_version` 和 `reason=budget_exhausted_before_read_transaction`。

最终本地真实进程冒烟：`/healthz` 返回 `data.status=ok`；提交 `01` 返回 `accepted / OK / v1 / 2 items`，所有逐条阶段的版本均为 `(window=1, tolerance_config_version=cfg-cac7a20d4a4b5535ce72)`；服务监听日志确认只绑定 `127.0.0.1:18789`。

## 7. 强制终止恢复实测

使用构建后的本地服务，临时 SQLite，设置：

```text
WORKBENCH_CRASH_POINT=after_writes
```

服务启动后提交 `09-sample-recovery-009.json`，请求连接被中断，进程以退出码 71 终止。随后正常重启，启动恢复扫描输出：

```json
{
  "pending_intents": 1,
  "confirmed_committed": 0,
  "aborted": 1,
  "details": [{
    "intent_id": 1,
    "batch_id": "09-sample-recovery-009",
    "planned_window_version": 1,
    "action": "aborted_pending_intent",
    "decision_present": false,
    "commit_facts_found": false
  }],
  "facts_only": true
}
```

恢复前查询批次返回 `E_BATCH_NOT_FOUND`，说明未提交的裁决不可见。重试同一批次后：

```text
outcome=accepted, code=OK, window_version=2
snapshot: complete=true, occupancy.active_count=1
```

计划版本 1 已中止，因此提交版本从 2 继续；诊断只暴露已提交版本 2，不混合中止版本。测试中的 `after_intent` 还覆盖了“意图已落盘、裁决事务尚未开始”的强制退出。

## 8. 交付脚本验证边界

`scripts/demo.ps1` 与 `scripts/crash-recovery-demo.ps1` 使用 Windows PowerShell 启动本地 `.exe`、临时 SQLite、回环 HTTP 和一键断言。当前执行器为 Linux，无法实际执行 PowerShell；脚本内容与 Go 自动化测试覆盖同一行为。Windows 侧首次运行前需执行 `go mod download` 以准备离线模块缓存。
