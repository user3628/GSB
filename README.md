# Local SQLite Connection Pool (Go)

一个从零实现的本地 SQLite 连接池与数据访问示例。所有组件只使用本地文件、`127.0.0.1` 回环 HTTP 和纯 Go `modernc.org/sqlite`，不依赖外部数据库、账号或网络服务。

## 交付内容

- 单写者 actor 管理的连接池：空闲有序队列、使用中集合、有界 FIFO 等待队列。
- 连接生命周期：`available`、`in_use`、`invalid`、`closed`，以及创建时间、最近使用时间、使用次数和替换原因。
- 获取/释放、FIFO 真实唤醒、获取超时、等待队列溢出和最大空闲数淘汰。
- SQLite 两阶段意图账本：`pending` 意图先与状态迁移一起提交，随后标记 `committed`；强制终止后扫描 pending 意图并生成恢复报告。
- 本地 HTTP API、统一 JSON 错误体、请求幂等与版本单调配置。
- 五类可复现异常注入：连接建立失败、查询超时、底层连接强断、等待队列溢出、获取超时。
- 并发/恢复单元测试，以及真实启动进程、`SIGKILL`、重启并核对恢复报告的端到端脚本。

## 快速开始

环境要求：Go 1.22 或以上；本地可写 `GOPATH`/`GOMODCACHE`。`go.mod` 固定纯 Go 驱动 `modernc.org/sqlite v1.34.5`（其自身要求 Go 1.21，避免使用要求 Go 1.26 的最新版）。

```bash
go mod download
go test ./... -count=1 -race
./scripts/run_e2e.sh
```

只启动 HTTP 服务：

```bash
mkdir -p .e2e-data
go run ./cmd/poolserver \
  -sample samples/pool_sample.json \
  -data-dir .e2e-data \
  -addr 127.0.0.1:19090 \
  -allow-debug
```

`-allow-debug` 只供确定性崩溃验证使用；服务强制绑定 `127.0.0.1`。

直接作为 Go 组件集成（无需经过 HTTP）：

```go
db, err := sqlitepool.Open(ctx, sqlitepool.Options{
    DatabasePath: "app.sqlite",
    MetadataPath: "app-pool-meta.sqlite",
    Config: sqlitepool.Config{
        MaxOpen: 4, MaxIdle: 2,
        MaxLifetimeMS: 60000, MaxIdleTimeMS: 30000,
        AcquireTimeoutMS: 1500, MaxWaiters: 16, Version: 1,
    },
})
lease, err := db.Acquire(ctx, sqlitepool.AcquireOptions{RequestID: "req-1"})
result, err := db.Query(ctx, lease.ConnectionID, "SELECT 1", sqlitepool.QueryOptions{})
_, err = db.Release(ctx, lease.ConnectionID, "req-1-release")
_ = db.Shutdown(ctx)
```

根目录包 `sqlitepool.go` 暴露稳定的配置、租约、快照、恢复报告和错误码别名；`internal/` 不构成外部集成接口。

## 实际验证结果

验证日期：2026-11-24。

- `go test ./... -count=1 -race`：通过。覆盖并发租约唯一性、FIFO 唤醒、超时移除、队列溢出无残留、空闲过期、强断替换、两阶段恢复、HTTP 幂等与配置版本。
- `./scripts/run_e2e.sh`：通过，退出码 `0`。
- 并发步骤：16 次获取/查询/释放，2 个连接，最大并发租约数 2，同一连接租约重叠计数 0。
- FIFO 步骤：两个等待者按序唤醒，观察到至少 2 个 `acquire_wake` 事件，最终等待数 0。
- 五类注入返回互不相同的业务码：`E_DIAL_FAILED`、`E_QUERY_TIMEOUT`、`E_CONN_RESET`、`E_POOL_EXHAUSTED`、`E_ACQUIRE_TIMEOUT`；每次观测均持久化并有 `injection_id`。
- 强断注入会真实新建替换连接；旧连接为 `invalid`，新旧记录分别带 `replaced_by`、`replaced_connection_id` 与相同的替换原因，持久化结果和原子池快照一致。
- 强杀恢复：先写入 2 条未提交意图，再对服务进程执行 `SIGKILL`；重启报告显示 pending acquire `in_use -> available`、pending release `available -> in_use`。
- 恢复报告明确标记：元数据状态可还原、运行期句柄未保留、空闲队列精确顺序不可还原、报告只基于持久化事实。
- 修复后的完整 E2E 连续运行 3 次均通过，脚本退出码与结果 JSON 的 `success` 一致。

可复核产物：

- `artifacts/e2e-verification.json`：逐步观测、每次池快照、注入记录与恢复报告。
- `artifacts/e2e-run.log`：脚本原始输出。
- `artifacts/recovery-latest.json`：重启进程生成的恢复报告。
- `artifacts/server-restart.log`：重启服务日志。

## 目录结构

```text
cmd/poolserver/       HTTP 服务入口
cmd/e2edemo/          进程级确定性演示与断言
sqlitepool.go         可嵌入的公开 Go API 门面
internal/domain/      共享类型、配置、业务错误码
internal/dbconn/      modernc SQLite 连接封装与真实 Close
internal/pool/        单写者 actor、FIFO、生命周期、原子快照
internal/store/       意图账本、连接/配置/注入/恢复报告持久化
internal/httpapi/     HTTP API、幂等协调器、异常注入
samples/              固定本地样本
scripts/run_e2e.sh    一键构建与端到端验证
artifacts/            本次实际验证结果
```

更完整的实现说明见 `docs/DESIGN.md`。

## 核心语义

池配置字段（毫秒）：

```json
{
  "max_open": 2,
  "max_idle": 2,
  "max_lifetime_ms": 60000,
  "max_idle_time_ms": 30000,
  "acquire_timeout_ms": 1500,
  "max_waiters": 2,
  "version": 7
}
```

- 获取：优先取空闲队列队首；空闲不足且 `active < max_open` 时新建；达到上限进入 FIFO 等待队列。
- 释放：若已有等待者，释放连接直接交给队首并产生 `release_handoff` + `acquire_wake` 事件；否则进入空闲队尾，超过 `max_idle` 时关闭最旧空闲连接。
- 获取超时：actor 将等待者从队列移除并返回 `E_ACQUIRE_TIMEOUT`；后续释放不会误唤醒它。
- 队列满：返回 `E_POOL_EXHAUSTED`，溢出的请求不会写入等待队列。
- 生命周期：归还时校验存活时间与空闲时间；空闲扫描器也会周期淘汰超限空闲连接。失效连接不会再进入空闲队列。
- 并发：所有状态迁移由单个 actor 串行执行；统计和连接明细通过不可变快照原子读取，多个读取者只会看到某个完整提交版本。

## 意图持久化与恢复

每个连接状态迁移分两阶段写入 SQLite：

1. 同一事务内先插入 `pending` 意图，再更新连接行；提交。
2. 独立事务把意图标记为 `committed`。

因此进程在 1 与 2 之间被强杀时，重启扫描会看到确定性的未完成事实：

- `acquire` pending：连接状态回滚为 `available`。
- `release` pending：连接状态回滚为 `in_use`。
- `invalidate` pending：回滚到 payload 中记录的先前状态。
- `config_change` pending：不应用新配置，保留旧版本。

恢复报告只包含已持久化连接行、配置行、意图与恢复动作。它不声称能恢复原进程的底层句柄、租约归属或精确空闲 FIFO 顺序；这些字段在报告中显式为不可恢复。报告由重启进程生成，不是被强杀进程的退出报告。

## HTTP API

所有写操作支持 `X-Request-Id` / `Idempotency-Key` 或 JSON 内 `request_id`。相同 ID + 相同内容只执行一次并复用精确响应；相同 ID + 不同内容返回 `E_CONFLICTING_REQUEST`。

| 方法与路径 | 说明 |
|---|---|
| `GET /healthz` | 健康检查 |
| `GET /v1/config` | 当前池配置 |
| `PUT /v1/config` | 配置变更；`version` 必须严格递增，否则 `E_STALE_CONFIG` |
| `POST /v1/acquire` | 获取连接，可带 `timeout_ms` |
| `POST /v1/release` | 释放连接 |
| `POST /v1/query` | 在使用中的连接上执行 SQL |
| `GET /v1/pool/stats` | 池统计、事件和有界等待状态 |
| `GET /v1/pool/connections` | 连接明细与空闲顺序 |
| `POST /v1/injections` | 触发五类异常之一并持久化观测 |
| `GET /v1/injections/{id}` | 查询注入记录 |
| `GET /v1/injections` | 最近注入记录列表 |
| `GET /v1/recovery` | 最近一次重启恢复报告 |
| `POST /v1/debug/crash-fixture` | 仅 `-allow-debug`；写入待恢复意图后供外部 `SIGKILL` |

成功响应：

```json
{"ok": true, "request_id": "req-1", "data": {"lease": {"connection_id": "conn-...", "created_at": "...", "use_count": 1}}}
```

失败响应：

```json
{"ok": false, "request_id": "req-1", "error": {"code": "E_ACQUIRE_TIMEOUT", "message": "...", "context": {"waiter_id": "...", "wait_ms": 80, "business_code": "E_ACQUIRE_TIMEOUT"}}}
```

业务码与 HTTP 状态：

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_ACQUIRE_TIMEOUT` | 408 | 等待获取超时 |
| `E_POOL_EXHAUSTED` | 429 | 有界等待队列已满 |
| `E_DIAL_FAILED` | 503 | 连接建立失败 |
| `E_QUERY_TIMEOUT` | 504 | 查询执行超时 |
| `E_CONN_RESET` | 502 | 底层连接被断开或连接不再可用 |
| `E_CONFLICTING_REQUEST` | 409 | 同一请求 ID 内容不一致 |
| `E_STALE_CONFIG` | 409 | 配置版本未严格递增 |

另有请求参数、内部、查询失败、关闭等辅助业务码，均使用同一 JSON 错误体。

## 五类异常注入

`POST /v1/injections` 的 `type` 与 `params` 来自固定样本：

| type | 行为 | 业务码 |
|---|---|---|
| `dial_failed` | 作用域内连接建立失败，不改变池内连接状态 | `E_DIAL_FAILED` |
| `query_timeout` | 故障包装器让查询耗时超过本次 deadline，归还连接 | `E_QUERY_TIMEOUT` |
| `conn_reset` | 通过回环注入端点调用真实 `sql.Conn.Close`，随后查询被拒绝并记录替换原因 | `E_CONN_RESET` |
| `wait_queue_overflow` | 占满连接与等待队列后发送额外请求，随后清理等待者与占用连接 | `E_POOL_EXHAUSTED` |
| `acquire_timeout` | 占满连接后按指定短超时获取，确认等待条目被移除 | `E_ACQUIRE_TIMEOUT` |

`conn_reset` 是对真实 SQLite 连接执行 Close，而非只改内存标志。每个注入的 `params`、`observation`、业务码和完成时间都写入 `injections` 表，并可通过 `GET /v1/injections/{id}` 逐项核对。注入在服务内串行运行并在返回前清理自身占用，避免改变其他类别行为。

## 本地样本

`samples/pool_sample.json` 固定：

- 监听地址 `127.0.0.1:19090`；
- 数据文件与元数据文件；
- 初始池配置版本 7；
- 4 个并发工作者 × 4 次请求；
- 五类注入参数。

脚本每次使用新建临时数据目录，因此结果可重复且不依赖上次运行。端口被占用时会立即失败并提示。

## 测试

```bash
go test ./... -count=1 -race
./scripts/run_e2e.sh
```

单元/集成测试包括：

- `internal/pool/pool_test.go`：FIFO 唤醒、超时移除、溢出无残留、dial 失败不泄漏名额、强断替换、空闲过期、并发唯一持有。
- `internal/store/recovery_test.go`：pending acquire/release/config 恢复语义与 committed 意图不误回滚。
- `internal/httpapi/server_test.go`：并发幂等复用、内容冲突、配置版本单调、错误体上下文。
- `sqlitepool_test.go`：公开门面获取、查询、释放与恢复报告。

## 设计边界

- 查询超时注入使用服务内故障包装器模拟执行超时，并在观测中标记 `simulated: true`；真实 SQLite 查询、连接关闭和其他生命周期行为仍走真实驱动。
- `dial_failed` 注入是作用域化的确定性失败；连接池真实 dial 失败路径另由单元测试 `ForceDialFailure` 覆盖。
- 状态快照是进程内原子版本；跨重启只有 SQLite 中已持久化事实可恢复，句柄和等待队列顺序不可恢复。
- 仅供本地受控验证，不包含认证/TLS，也不会绑定非回环地址。
