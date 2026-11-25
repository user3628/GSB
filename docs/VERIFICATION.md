# 实际验证记录

验证时间：2026-11-25（Asia/Shanghai）。

执行环境为 WSL2/Linux，Go 主工具链 `go1.27.4 linux/amd64`；另外使用 `GOTOOLCHAIN=go1.22.12` 验证 Go 1.22 兼容性。目标平台 Windows 通过 `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build` 交叉编译为 PE32+ 可执行文件。仓库提供 `scripts/verify.ps1`，但当前记录中的 PowerShell 脚本未在真实 Windows 桌面会话执行；等价的 `scripts/verify.sh` 已在 WSL 实跑通过。

## 命令与结果

```text
$ GOTOOLCHAIN=go1.22.12 go test ./... -count=1
?       edgeconfig/cmd/edge-dist        [no test files]
ok      edgeconfig/internal/verify      0.278s
ok      edgeconfig/pkg/edgeconfig       0.284s

$ go test -race ./... -count=1
?       edgeconfig/cmd/edge-dist        [no test files]
ok      edgeconfig/internal/verify      1.577s
ok      edgeconfig/pkg/edgeconfig       1.669s

$ ./scripts/verify.sh
==> go test ./...
ok      edgeconfig/internal/verify      0.218s
ok      edgeconfig/pkg/edgeconfig       0.269s
==> Build bin/edge-dist
==> Deterministic end-to-end verification
checks=22 passed=22 failed=0 report=artifacts/verification-report.json
force-kill/affected_recompute=PASS pending=1
force-kill/probe_round=PASS pending=1

$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o edge-dist-windows.exe ./cmd/edge-dist
WINDOWS_BUILD_OK
```

机器可读完整报告：`artifacts/verification-report.json`。

## 端到端检查项

| # | 结果 | 检查 |
|---:|---|---|
| 1 | PASS | 非法拓扑返回业务错误且整批不落盘 |
| 2 | PASS | 合法拓扑持久化 |
| 3 | PASS | 版本 1 快照提交和摘要 |
| 4 | PASS | 旧快照版本返回 `E_STALE_SNAPSHOT` |
| 5 | PASS | 增量影响面、区域分桶和全部预期节点 |
| 6 | PASS | 相同增量请求 ID 幂等重放 |
| 7 | PASS | 错误基础版本返回 `E_STALE_BASE` |
| 8 | PASS | `edge-global` 新观察版本收敛 |
| 9 | PASS | `downstream-x` 新观察版本收敛 |
| 10 | PASS | `downstream-z` 新观察版本收敛 |
| 11 | PASS | 过期观察版本返回 `E_STALE_READ` 并附两个版本 |
| 12 | PASS | 真实 HTTP 探测成功 |
| 13 | PASS | 真实 HTTP 423 业务拒绝映射为 `E_PROBE_REJECTED` |
| 14 | PASS | 挂起端点超时映射为 `E_PROBE_TIMEOUT` |
| 15 | PASS | 超时后上游模拟端点活动连接归零，底层连接被关闭 |
| 16 | PASS | 恢复端点后全节点真实探测收敛 |
| 17 | PASS | 定点一致度为 `1.000000`、阈值判定 PASS、含 6 个舍入步骤 |
| 18 | PASS | 相同探测请求 ID 幂等重放 |
| 19 | PASS | 同 ID 不同内容返回 `E_CONFLICTING_REQUEST` |
| 20 | PASS | 影响面报告持久化并可经 HTTP 读取 |
| 21 | PASS | 强杀影响面重算子进程后恢复 |
| 22 | PASS | 强杀探测轮子进程后恢复 |

## 强制终止恢复事实

报告中的两次演练均：

1. 启动真实子进程；
2. 加载固定样本并提交版本 1；
3. 持久化一条 `PENDING` 意图；
4. 探测场景额外启动长挂起 loopback HTTP 请求；
5. 父进程调用 `Process.Kill()` 强杀；
6. 重新打开 SQLite；
7. 观察到 `pending_before_recovery = 1`；
8. `Recover` 恰好中止 1 条意图；
9. `last_complete_version = 1`、`rolled_back_to_version = 1`。

Windows 上 `Process.Kill()` 对应 `TerminateProcess`，不执行进程内 `defer`，因此演练覆盖了意图日志恢复路径。

## 自动化测试覆盖

- `pkg/edgeconfig/numeric_test.go`：满分定点分数、累计舍入误差、相对容差平局和定点字符串解析。
- `pkg/edgeconfig/affected_test.go`：逐键索引、区域分桶、三级传播跳数、全订阅节点指标和未声明订阅校验。
- `pkg/edgeconfig/service_test.go`：增量、`E_STALE_READ`、并发相同请求只提交一次、同 ID 内容冲突、意图恢复。
- `pkg/edgeconfig/service_test.go` 中的 `TestProbeTimeoutReleasesConcurrencySlots`：4 个挂起端点、并发 2、独立超时，验证名额释放和全部超时返回。

## 复现方式

Windows PowerShell：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

只跑 Go 测试：

```powershell
go test ./... -count=1
```

不做强杀演练的快速验证：

```powershell
.\bin\edge-dist.exe verify --force-kill=false
```
