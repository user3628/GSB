# 示例

- `run-demo.sh`：Linux、WSL、Git Bash；使用 `curl` 调用本地 HTTP API。
- `run-demo.ps1`：Windows PowerShell 5+；使用 `Invoke-RestMethod`。
- `run-go-run-smoke.sh`：直接以 `go run ./cmd/releasegate` 启动服务，核对阶段统计、门禁和恢复报告查询。

完整 `run-demo` 脚本使用 `go build` 后的原生进程，以便精确断言故障注入导致的退出码 `70`；日常启动方式仍为 README 中的 `go run ./cmd/releasegate`。

两个脚本执行同一序列：

1. 以固定种子生成 `testdata`。
2. 构建并启动 `cmd/releasegate`。
3. 创建发布并开始阶段 1。
4. 摄入阶段 1，查询统计与误差契约，门禁推进到阶段 2 / 300‰。
5. 摄入阶段 2，触发 `E_GATE_FAILED` 并暂停在 `OBSERVING`。
6. 通过故障注入在持久意图后强制退出进程（退出码 70）。
7. 重启服务，自动恢复并查询恢复报告。
8. 确认回滚，生成 `artifacts/demo/demo-output.json`。

Windows 示例需要允许本地脚本执行，或使用：

```powershell
powershell -ExecutionPolicy Bypass -File .\examples\run-demo.ps1
```
