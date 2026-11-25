# 实际验证记录

验证时间：2026-11-25 10:34 (Asia/Shanghai)

执行环境：

- `go version go1.27.4 linux/amd64`
- `go.mod` 声明 `go 1.22`，使用 `modernc.org/sqlite v1.34.5`，无需 CGO 即可构建 Windows 产物
- 未使用 Docker、外部数据库、真实账号或外部网络目标；探测目标均为 `127.0.0.1` 内置端点

完整原始日志：`artifacts/results/final-validation.log`

机器可读 E2E 报告：`artifacts/results/e2e-result.json`

## 1. 已执行命令

```bash
go vet ./...
go test -count=1 ./...
CGO_ENABLED=1 go test -race -count=1 ./internal/probe ./internal/store ./internal/api
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o bin/deployverify-windows-amd64.exe ./cmd/platform
bash scripts/demo.sh
```

结果：

- `go vet ./...`：通过，无输出。
- 普通测试：`internal/api`、`internal/derive`、`internal/diff`、`internal/probe`、`internal/store` 全部 `ok`。
- 竞态测试：`internal/probe`、`internal/store`、`internal/api` 全部 `ok`。
- Windows 交叉编译：`GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...` 与平台二进制构建均成功；平台产物约 17 MB：`bin/deployverify-windows-amd64.exe`。
- E2E 脚本：退出码 `0`，JSON 报告 `"ok": true`，15 个验收步骤全部通过。

## 2. E2E 业务码结果

| 验收点 | 实际 HTTP / 业务码 | 结果 |
| --- | --- | --- |
| 非法规格不写入状态 | HTTP 422 / `E_SPEC_INVALID` | 通过 |
| v1 快照提交 | HTTP 201 / `OK` | 通过 |
| 同 base 并发增量 | 1 个 `OK`，1 个 `E_STALE_BASE` | 通过 |
| identity 变更级联 | HTTP 200，影响 `identity/gateway/billing` | 通过 |
| 配置键引用索引 | `SESSION_TTL` 返回引用记录 | 通过 |
| 依赖环提交后降级拓扑 | HTTP 201 / `OK` | 通过 |
| 环诊断与受阻清单 | HTTP 422 / `E_DEPENDENCY_CYCLE`，`blockedCount=4` | 通过 |
| 跨环境漂移 | HTTP 409 / `E_ENV_DRIFT` | 通过 |
| 空轮次 | `empty=true`，结论含 `QUEUE_EMPTY` | 通过 |
| 单探测独立超时 | `timeoutCount=1`，约 86 ms 返回 `E_PROBE_TIMEOUT` | 通过 |
| 有界队列满 | HTTP 429 / `E_PROBE_QUEUE_FULL` | 通过 |
| 轮次时间预算 | 120 ms 预算，4 个任务均 `skipped` / `E_PROBE_BUDGET_EXCEEDED`，约 142 ms 结束 | 通过 |
| 探测与计划原子一致 | `prod/identity` 计划状态为 `success` | 通过 |
| `Kill()` 强杀后重启恢复 | pending intent 清零，`run-crash-e2e=interrupted`，恢复版本 4 | 通过 |

## 3. 崩溃恢复实证

E2E 先提交到完整版本 4，再启动固定 ID 为 `run-crash-e2e` 的 30 秒慢探测。轮次意图和 `probe_runs(status=running)` 可见后，测试调用操作系统级 `Process.Kill()`：

```text
waitError: signal: killed
clientError: Post "http://127.0.0.1:<port>/api/probe-rounds": EOF
```

使用同一 SQLite 文件重启后，启动日志实际输出：

```json
{
  "pendingIntents": ["intent-cfdf66a9-4482-4c8f-8a0c-cdf8e4496886"],
  "abortedIntents": ["intent-cfdf66a9-4482-4c8f-8a0c-cdf8e4496886"],
  "interruptedRuns": ["run-crash-e2e"],
  "restoredVersion": 4,
  "recoveryRecordId": 2
}
```

随后查询：

- `/api/intents?status=pending`：0 条；
- `/api/probe-runs/run-crash-e2e`：`status=interrupted`，`totalTasks=1`；
- `/healthz`：`currentVersion=4`。

这证明进程中断后状态停留在最后完整版本，未留下 pending 意图或永久 running 轮次。

## 4. Windows 使用方式

在 Windows 空白工作区安装 Go 1.22+ 后：

```powershell
.\scripts\demo.ps1
```

或手动运行：

```powershell
$env:CGO_ENABLED="0"
go run ./cmd/platform -addr 127.0.0.1:18080 -db workspace/platform.db -workdir workspace
```

另开终端：

```powershell
go run ./cmd/e2e -platform-bin .\bin\platform.exe -out artifacts\results\e2e-result.json
```

脚本和驱动均不依赖 C 编译器；`modernc.org/sqlite` 提供纯 Go SQLite 实现。
