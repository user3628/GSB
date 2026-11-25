# 实现说明与验证记录

本目录是一个可嵌入的 Go 领域组件，模块名为 `multicluster-convergence`。它覆盖快照解析与校验、漂移判定、依赖分批、HTTP 写回回读、增量级联、并发幂等、SQLite 意图持久化和强杀恢复，并提供 `cmd/converge` CLI 与本地确定性样本。

## 代码地图

- `model.go`：领域模型、JSON 归一化、唯一标识/依赖/预算校验、策略与快照摘要。
- `decimal.go`：`big.Int`/`big.Rat` 定点十进制比较，避免裸 `float64` 容差判断。
- `drift.go`：缺失、多余、取值偏移、来源冲突四类明细与稳定主类别。
- `plan.go`：拓扑分层、环检测、环外分层、批次容量与拆分元数据。
- `service.go`：单写者提交、MVCC 原子视图、增量合并、级联集合。
- `apply.go`：批次幂等、写回/回读、状态原子翻转、轮次预算。
- `scheduler.go`：最大并行批数、有界等待队列和 `E_WRITE_QUEUE_FULL`。
- `writeclient.go`：真实 HTTP PUT/GET、独立超时、请求级连接关闭和 `E_APPLY_MISMATCH` 明细。
- `store.go`：SQLite schema、意图/快照/派生结果/批次事实事务和重启恢复。
- `target.go`：确定性本地回环目标，支持延迟和回读不一致注入。
- `samples.go`、`samples/`：10 个初始对象（增量合并后 11 个）、策略、全量快照、增量、陈旧增量、环和非法样本。
- `cmd/converge/main.go`：样本生成、回环端点、提交、应用、恢复和查询命令。
- `scripts/verify.sh`、`scripts/verify.ps1`：Linux/WSL 与 Windows 一键验收脚本。

## 需求落点

- `E_SPEC_INVALID`：解析和 `NormalizeSnapshot` 聚合所有校验问题；提交失败时不替换 `atomic.Pointer[CommittedVersion]`，旧快照继续可读。
- 漂移判定：每个对象保留全部 `details`、`field_checks`、受影响字段、判定依据和按策略优先级得到的主类别；nil 字段集合返回业务错误。
- 依赖批次：依赖对象的层和全局批次序号严格更小；环错误 details 携带 `cycles`、`cyclic_object_ids`，环外对象仍返回部分计划。
- 版本并发：提交由 `writerMu` 串行化；增量校验当前版本和 `base_version`，同基并发只有一个成功，其余得到 `E_STALE_BASE`。
- 写回核对：PUT 后 GET 并重新归一化比较；不一致返回 `E_APPLY_MISMATCH` 以及对象、期望字段、实际字段。
- 超时与队列：每次 PUT/GET 有独立 `context.WithTimeout`；超时/取消会关闭该请求跟踪到的连接。调度器限制并行批数和等待队列。
- 一致读：读者通过 `atomic.Pointer` 获取完整提交版本；批次对象结果、状态和汇总在一次复制+指针替换中可见。
- 幂等：同一 `version/batch_id` 的并发调用共用 `batchCall`，跨进程复用 SQLite 终态；请求内容哈希不一致返回 `E_CONFLICTING_REQUEST`。
- 崩溃恢复：快照切换、计划构建和批次写回均先落 prepared intent；重启时中止快照/计划意图，把 applying/prepared 写回回滚为 pending。报告只来自 SQLite 行，并明确 HTTP 副作用可能已经到达、后续期望态 PUT/GET 可幂等收敛；`exit_report_promise=false`。

## 复现命令

Linux、macOS 或 WSL：

```bash
go test ./... -count=1
go test -race ./... -count=1
bash scripts/verify.sh
```

原生 Windows PowerShell：

```powershell
go test ./... -count=1
.\scripts\verify.ps1
```

脚本会启动随机端口的 `127.0.0.1` 回环目标，完成全量应用、增量级联应用、陈旧基版本、非法规格、依赖环、`kill -9`/`Stop-Process -Force`、SQLite 恢复和恢复后重放。

## 实际验证结果（2026-11-25）

Linux/WSL 环境：

- `go version go1.27.4 linux/amd64`
- `go test ./... -count=1`：`multicluster-convergence` 与 `cmd/converge` 均 ok。
- `go test -race ./... -count=1`：两个包均 ok。
- `go vet ./...`：通过。
- `bash scripts/verify.sh`：输出 `VERIFY_SH_OK artifacts/verify-sh/verification-summary.json`；全量 6 个批次、增量后 7 个批次；恢复报告含 1 条 `rolled_back_write_batches`，新状态为 `pending`。

Windows 侧真实验证：

- 临时安装免安装工具链：`go version go1.22.12 windows/amd64`（`C:\Temp\goportable-259\go\bin\go.exe`）。
- 因 Windows Go 不能对 `\\wsl.localhost\...` 上的 `go.mod` 加锁，将源码复制到本地 NTFS 目录 `C:\Temp\conv-work-259` 后执行。
- `go test ./... -count=1`：`multicluster-convergence` 与 `cmd/converge` 均 ok，其中包含真实构建 CLI、启动 `.exe`、强杀和恢复的集成测试。
- `powershell.exe -ExecutionPolicy Bypass -File .\scripts\verify.ps1`：输出 `VERIFY_PS1_OK C:\Temp\conv-work-259\artifacts\verify-windows\verification-summary.json`。
- 取回的摘要副本：`artifacts/verify-windows/verification-summary.json`，其中 `full_batches=6`、`delta_batches=7`、`exit_report_promise=false`。
- 原生 Windows `go test` 输出副本：`artifacts/verify-windows/go-test.txt`。

另执行过 Linux Go Windows 交叉编译：

```bash
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o artifacts/bin/convergence-windows.test.exe .
```

## 说明

`artifacts/` 中的 SQLite 数据库、日志、JSON 输出和二进制是本地验证产物，可随时由 `scripts/verify.sh` 或 `scripts/verify.ps1` 重新生成。SQLite 驱动使用已 vendored 的 `modernc.org/sqlite`，因此 Windows 构建不需要 CGO、gcc 或 Docker。
