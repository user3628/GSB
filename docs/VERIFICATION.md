# 验证记录

## 环境

- 日期：2026-11-26（Asia/Shanghai）
- OS：Linux / WSL Ubuntu 26.04
- Go：`go1.27.4 linux/amd64`（模块最低声明 `go 1.22`）
- SQLite CLI / library：`3.46.1`
- cgo：启用，链接系统 `libsqlite3`
- 第三方 Go 依赖：无

## 命令与结果

```text
$ go test ./... -count=1
ok   registrylab/registry
ok   registrylab/workbench
?    registrylab/cmd/registrylab
?    registrylab/internal/sqlite
?    registrylab/sample
?    registrylab/testfixture

$ go test -race ./... -count=1
ok   registrylab/registry   3.338s
ok   registrylab/workbench  1.217s

$ go vet ./...
(no output)

$ gofmt -l .
(no output)

$ go build -o registrylab ./cmd/registrylab
$ ./registrylab verify
18/18 checks passed
```

最终 `verify` 耗时约 `578ms`，`pass=true`。完整、机器可读的报告见 `artifacts/verification-report.json`。

## 18 项检查

| 检查 | 结果 |
|---|---|
| 固定种子摘要确定性 | PASS |
| 完整快照提交 | PASS |
| 旧版本快照拒绝且保留旧状态 | PASS |
| 增量提交 | PASS |
| 过期基础版本拒绝 | PASS |
| 同一基础版本并发增量仅一个成功 | PASS |
| 租约心跳 | PASS |
| HTTP/TCP/LEASE 真实探测 | PASS |
| 连续阈值状态推进 | PASS |
| 权重归一化与不可用实例排除 | PASS |
| 合法 DRAINING 迁移 | PASS |
| 非法迁移不改变派生 revision | PASS |
| 有界等待队列返回 E_PROBE_QUEUE_FULL | PASS |
| 预算耗尽确定性延后排序 | PASS |
| 同一请求并发探测轮次幂等 | PASS |
| 级联重建中途 SIGKILL 恢复 | PASS |
| 派生暂存完成后 SIGKILL 恢复 | PASS |
| 探测中途 SIGKILL 恢复 | PASS |

## 关键实测事实

- 慢 HTTP 探针配置 90ms 超时，最终验收中实际结果 `E_PROBE_TIMEOUT`、耗时约 90ms；测试另确认服务端活跃慢请求最终回到 0，即底层连接被真实关闭。
- HTTP 正常、坏状态码、坏标记分别得到 `E_PROBE_OK`、`E_PROBE_STATUS_MISMATCH`、`E_PROBE_MARKER_MISMATCH`。
- TCP 成功得到 `E_TCP_OK`，已关闭的回环端口得到 `E_TCP_REFUSED`。
- 两次成功探测后，配置阈值为 1 或 2 的成功实例均进入 `HEALTHY`；失败实例不进入权重分配。
- 三个服务的可用权重份额分别严格合计 `1,000,000` ppm。
- 对 `DRAINING` 直接迁移到 `HEALTHY` 返回 `E_INVALID_TRANSITION`，实例状态和派生 revision 均未改变。
- 全局 1、每服务 1、队列 1、8 个待测任务时返回 `E_PROBE_QUEUE_FULL`，上下文给出 `taskCount=8`、`maxActive=1`。
- 1ms 预算下延后任务按优先级降序输出，每条都包含消耗预算与零剩余预算依据。
- 两个并发相同 `requestId` 的探测轮次返回同一个 `roundId`，没有产生第二个轮次。
- 级联中途 SIGKILL 后重开：当前版本仍为 1，恢复报告记录 1 个中止意图。
- 派生暂存后 SIGKILL 后重开：当前版本/派生版本仍为 1，报告删除 1 个不完整派生 revision 与 1 个暂存快照。
- 探测中途 SIGKILL 后重开：9 条未提交探测结果全部重置为 `PENDING`，健康与派生仍停留在最后一个完整版本 2。
- `serve` 端到端烟测：页面可加载；发布 9 实例快照后执行真实轮次，9/9 完成，HTTP 正常/坏状态/坏标记、TCP 成功/拒绝、慢请求超时和 LEASE 缺失均得到预期业务码。
- SQLite CLI 复核最终数据库：15 张业务/日志表；主库有 3 个 `COMPLETE` 轮次与 1 个 `BUDGET_DEFERRED` 轮次；三个崩溃库的恢复报告分别记录中止意图 1 个、清理暂存派生/快照各 1 个、重置未提交探针 9 条。

## 可重复运行

```bash
go build -o registrylab ./cmd/registrylab
./registrylab verify                 # 输出 JSON
./registrylab verify --json=false    # 输出 JSON 后附通过计数
go test ./... -count=1
go test -race ./... -count=1
```

`verify` 默认使用临时目录并在结束时清理。若需保留数据库与崩溃场景数据库以便用 `sqlite3` 检查：

```bash
mkdir -p /tmp/registry-sentinel
./registrylab verify --db /tmp/registry-sentinel/verify.db --json=false
sqlite3 /tmp/registry-sentinel/verify.db \
  'SELECT round_id,status,error_code FROM probe_rounds ORDER BY created_at;'
```
