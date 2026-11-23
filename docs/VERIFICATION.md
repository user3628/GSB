# 实际验证记录

验证时间：2026-11-24（Asia/Shanghai）。运行环境：Linux amd64，`go version go1.27.4 linux/amd64`；模块声明兼容 `go 1.22`。

## 依赖与环境

```text
$ go version
go version go1.27.4 linux/amd64

$ cat go.mod
module configimpact
go 1.22
require modernc.org/sqlite v1.34.5
```

`modernc.org/sqlite` 为纯 Go 驱动，`CGO_ENABLED=0` 可构建。

## 单元与集成测试

```text
$ go test ./...
ok      configimpact            0.418s
?       configimpact/cmd/configimpact  [no test files]

$ go vet ./...
(no output)
```

覆盖：

- 文档校验失败保持旧版本可用。
- 分组默认值、显式值、直接/间接依赖候选与来源链。
- 同键高 order 胜出、未定义键不落零值。
- 补丁沿监听、分组、依赖传播，输出触发键和 0/1/2 跳。
- 增量/全量逐键一致。
- 历史读、复用物化版本、未来版本 `E_STALE_READ`。
- 并发重复补丁、`E_CONFLICTING_PATCH`、`E_STALE_BASE`。
- 提交中读者绑定旧版本，物化/影响/引用原子可见。
- SQLite 重启后状态和幂等结果仍可用。
- 含空格数据库路径可创建、写入并重启读取。
- 文档行存在但物化/影响不完整的伪版本会在恢复时被丢弃并写入报告。

## 竞态检测

```text
$ go test -race ./...
ok      configimpact            2.244s
?       configimpact/cmd/configimpact  [no test files]
```

## 强制终止恢复

测试 `TestRecoveryAfterForcedTermination` 启动同一测试二进制的子进程。子进程在 `patch_log=in_progress` 和 `intents=pending` 已提交、最终快照事务尚未开始时执行 `os.Exit(91)`。

父进程实测结论：

- 子进程退出码为 91。
- 重启扫描到 1 个未完成意图并将其标记为 `aborted`。
- `last_complete_version = 1`，完整文档版本列表仅含 `[1]`。
- `documents` 表仅 1 行，版本 2 不可读，读取返回 `E_STALE_READ`。
- `patch_log` 中 `crash-patch` 状态为 `aborted`。
- 相同补丁重试后成功提交版本 2，值 `db.pool_size = 16`。
- 再次干净启动时扫描到 0 个待处理意图，最后完整版本为 2。

## 固定样本 demo

命令：

```bash
go run ./cmd/configimpact demo -db /tmp/configimpact-demo.db -fresh
```

补丁 `patch-001` 从版本 1 提交到版本 2，触及：

- `log.level`（分组 `base`）
- `db.pool_size`（组件 `db`）
- `http.retries`（分组 `edge`）

实测影响结果：

| 组件 | 跳数 | 触发键数 | 路径 | 下游 |
|---|---:|---:|---|---|
| api | 0 | 3 | full | worker |
| audit | 0 | 3 | full | - |
| auth | 0 | 2 | full | api, audit, worker |
| db | 0 | 2 | full | api, audit, auth, worker |
| metrics | 0 | 1 | full | - |
| worker | 0 | 3 | full | - |

版本 1 与版本 2 的组件读取对比：

```text
component  mat@v1  db.pool_size@v1  log.level@v1  mat@v2  db.pool_size@v2  log.level@v2
db         1       8                info          2       16                error
auth       1       8                debug         2       16                error
api        1       8                debug         2       16                error
worker     1       8                debug         2       16                error
audit      1       8                debug         2       16                error
metrics    1       <undefined>      warn          2       <undefined>       warn
```

对版本 3 的未来观察返回：

```json
{
  "code": "E_STALE_READ",
  "details": {
    "committed_version": 2,
    "lagging_version": 2,
    "latest_materialized_version": 2,
    "observed_version": 3
  }
}
```

## Windows 构建

当前执行环境不是 Windows，因此不能在本机运行 Windows 二进制；已执行 Windows amd64 交叉构建作为编译级验证：

```text
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/configimpact-windows-amd64.exe ./cmd/configimpact
$ file /tmp/configimpact-windows-amd64.exe
PE32+ executable for MS Windows 10.00 (console), x86-64, 16 sections
```

CLI 仅使用跨平台标准库和纯 Go SQLite；Windows 工作区可按 README 的 PowerShell 命令直接构建和运行。
