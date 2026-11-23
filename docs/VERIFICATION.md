# 验证记录

验证日期：2026-11-24（Asia/Shanghai）

## 环境

| 项 | 值 |
|---|---|
| 验证主机 | Linux `amd64` |
| Go 工具链 | `go1.27.4 linux/amd64` |
| 模块语言版本 | `go 1.22` |
| CGO | `CGO_ENABLED=1`，但 SQLite 使用纯 Go 驱动 `modernc.org/sqlite v1.34.5` |
| 网络 | 仅回环 `127.0.0.1` |
| 容器 | 未使用 |

Windows 本机无法在当前执行环境中直接运行，因此额外执行了 `GOOS=windows GOARCH=amd64` 交叉编译。三个可执行目标均构建成功。演示脚本本身是跨平台 Go 程序，Windows 入口为 `scripts/demo.ps1`。

## 静态检查与自动测试

实际执行并通过：

```text
$ rg --files -g '*.go' | xargs gofmt -w
$ go vet ./...                 # 通过，无输出
$ go test -count=1 ./...       # 全部通过
ok  recoveryengine/internal/engine   3.183s
ok  recoveryengine/internal/fixed    0.003s
ok  recoveryengine/internal/graph    0.004s
ok  recoveryengine/internal/httpapi  0.222s
ok  recoveryengine/internal/probe    0.100s
ok  recoveryengine/internal/spec     0.004s

$ go test -race -count=1 ./... # 全部通过
ok  recoveryengine/internal/engine   7.088s
ok  recoveryengine/internal/fixed    1.015s
ok  recoveryengine/internal/graph    1.016s
ok  recoveryengine/internal/httpapi  1.889s
ok  recoveryengine/internal/probe    1.103s
ok  recoveryengine/internal/spec     1.019s

$ go build ./...               # 通过
```

测试覆盖包括：

- 重复组件保留首条并记录明细；
- 负数、`int64` 溢出、缺失依赖返回 `E_SPEC_INVALID`；
- 影响面、传播跳数和依赖优先批次；
- 依赖环拒绝及未污染部分的批次保留；
- 整快照替换、错误 JSON 形状、`E_CONFLICTING_REQUEST`；
- 同一基础版本并发增量一成功一 `E_STALE_BASE`；
- 同一幂等键八并发只执行一次，其余重放；
- 写意图期间读者仍看到旧完整版本；
- 真实超时、连接拒绝、业务拒绝三类端点结果；
- 强杀后 pending 意图中止，恢复后重试与未中断执行逐项一致；
- 同一快照三次固定点重算摘要逐位一致。

## Windows 交叉构建

```text
$ GOOS=windows GOARCH=amd64 go build -trimpath -o .tmp/cross-windows/recovery-server.exe ./cmd/server
$ GOOS=windows GOARCH=amd64 go build -trimpath -o .tmp/cross-windows/testendpoint.exe ./cmd/testendpoint
$ GOOS=windows GOARCH=amd64 go build -trimpath -o .tmp/cross-windows/demo.exe ./cmd/demo

-rwxr-xr-x 16M demo.exe
-rwxr-xr-x 17M recovery-server.exe
-rwxr-xr-x 9.5M testendpoint.exe
```

## 最新控制台/API 冒烟

最新源码重建后实际请求结果：

```text
GET /                     200  AEGIS · Recovery Coordinator
GET /styles.css           200  text/css; charset=utf-8          34,991 bytes
GET /app.js               200  text/javascript; charset=utf-8    41,556 bytes
OPTIONS /stats (19090)    204  Access-Control-Allow-Origin: *
```

`/app.js` 已包含提交成功后推进 `base_version` 并生成新幂等键的逻辑。

样本校验分派冒烟：

```json
{"ok":true,"kind":"increment","version":2,"changed":2}
{"ok":true,"kind":"component-list","valid":true}
{"ok":true,"kind":"drill","failed":["gateway"]}
```

## 端到端演示

命令：

```text
$ go run ./cmd/demo
```

先执行完整构建运行，再执行 `-skip-build` 复用缓存二进制运行；两次均通过全部步骤。最终证据目录：

```text
.tmp/demo/20261124-043912/evidence.json
```

关键实测结果：

| 检查 | 结果 |
|---|---|
| 全量快照 | v1，9 组件 / 8 边 |
| 快照校验摘要 | `sha256:0c02a49546900288227a115caafb3bd30e8c2ae1f5e44953a48a1303d0d9aa87` |
| v1 派生摘要 | `sha256:df0146633a1dfca5c2c800eed5086128f21efd0790654d5ecdc4e3799a38bef8` |
| 非法样本 | 4 个样本（负数、溢出、缺失依赖、权重和错误）均 `E_SPEC_INVALID`，已提交版本保持 v1 |
| `gateway` 影响面 | 4 个受影响组件，最大 3 跳 |
| 恢复批次 | `gateway → telemetry → reporter → notifier` |
| 依赖环 | `E_DEPENDENCY_CYCLE` + `partial_result`，版本保持 v1 |
| 资源增量 | v1→v2，changed_refs=2，invalidated=13，rebuilt=66 |
| 陈旧基线 | `E_STALE_BASE`，当前版本保持 v2 |
| 并发写 | writer-1 得到 409，writer-2 得到 200；最终版本 v3 |
| 演练耗时 | 742 ms |
| 演练业务码 | 超时、探测拒绝、连接拒绝的 probe/recover 六个不同业务码均出现；另有 2 个 `OK` |
| 固定点重算 | 三次摘要均为 `sha256:62e9eb5bce6c6464c7fe077ebd432f39852b790e29e8725baa69a4e3e1aac21a` |
| 超时连接 | 测试端点观察到上下文取消；活动预留 0，累计释放 4 |
| 强杀现场 | 最后完整版本 v3，pending 意图目标版本 v4 |
| 重启恢复 | 恢复报告扫描 1 个中止意图，版本仍为 v3 |
| 重试结果 | v4；checksum `sha256:41c83c1c35866e766428051e0c20cdfb7dbd43dd54f9a3de3d1403f46fce25be` |
| 重试派生摘要 | `sha256:f9ade28af98d40e9f67cafbcb41e8fd34eec1745e02e260de62a5b4090215af7` |
| 与未中断基线比较 | 版本、基础版本、快照摘要、派生摘要、图摘要、一致度及级联集合逐项一致 |
| 再启动 | v4 摘要保持不变 |
| 整份替换 | v4→v5，组件/边与全部派生结构整体切换 |

演示运行日志保留在同一目录，包含：

- `test-endpoint.log`
- `coordinator-main-1.log`
- `coordinator-baseline.log`
- `coordinator-crash-window.log`
- `coordinator-recovered.log`
- `coordinator-restarted.log`

其中 `coordinator-crash-window.log` 只记录到强杀前的健康检查和 pending 意图查询，没有完成提交记录；重启日志以 v3 启动，并产生恢复报告和 v4 重试提交，符合“只基于已落盘事实”的预期。

## 结论

固定样本下的影响面/批次推导、非法批原子拒绝、增量级联重建、并发陈旧基线、真实超时与资源释放、强杀恢复和精确重放均已实际验证通过。Windows 目标已完成交叉编译，运行入口为 `scripts/demo.ps1`。
