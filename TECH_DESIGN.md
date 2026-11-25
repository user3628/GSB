# MVCC 快照与版本链组件说明

本实现使用 Go 1.22+、纯 Go SQLite 驱动 `modernc.org/sqlite`，`CGO_ENABLED=0` 可构建。核心代码不依赖外部服务，HTTP 与示例只监听 `127.0.0.1`。

## 目录

- `pkg/mvcc`：事务、版本、快照、业务错误码、确定性样本校验与无副作用可见性判定。
- `internal/engine`：SQLite schema、意图记录、单写者引擎、恢复、链校验/修复、快照切换与回收。
- `internal/httpapi`：JSON HTTP API 与统一错误信封。
- `internal/serverapp`：打开 SQLite、启动恢复、装配 HTTP 服务和演示故障点。
- `cmd/mvcc-server`：本地 API 服务。
- `cmd/demo`：一键演示；`server` 子命令可作为可强杀的独立服务进程。
- `samples/deterministic.json`：固定本地样本。

## 模型

- 事务序号从 1 开始单调递增且无缺口；事务状态为 `ACTIVE`、`COMMITTED`、`ABORTED`。
- 每个事务在开启时固定 `snapshot_seq` 与 `upper_bound=txn_seq`，因此后续全局快照切换不会混用进该事务的重复读。
- 版本使用全局单调 `version_seq`，记录 `row_key`、创建事务、提交状态、64 位十六进制摘要、可选内容、`prev_version_seq`、删除标记和取代提交序号。
- `row_heads(row_key PRIMARY KEY, version_seq)` 保证正常路径下每行只有一个正式最新指针。

## 可见性

`mvcc.Evaluate` 从行头沿 `prev_version_seq` 自新到旧遍历，并返回命中版本、判定依据和每个候选版本的原因：

| 候选事实 | 原因 | 是否跳过 |
| --- | --- | --- |
| 当前读事务创建且仍未提交 | `SELF_UNCOMMITTED` | 是 |
| 其他活跃事务的未提交版本 | `OTHER_UNCOMMITTED` | 是 |
| 版本或创建事务已回滚 | `ROLLED_BACK` | 是 |
| 已提交但创建事务序号大于快照上界 | `SNAPSHOT_OUTSIDE_COMMIT` | 是 |
| 快照内已提交的删除版本 | `DELETED_IN_SNAPSHOT` | 停止，行不可见 |
| 快照内已提交的普通版本 | `VISIBLE_COMMITTED_IN_SNAPSHOT` | 命中 |

同一 `ReadResult` 不含时间戳；同一引擎状态、同一快照和同一行重复读取会得到逐字节一致的 `CanonicalJSON()`。

## 链校验与修复

`Engine.ValidateChain` 检查：

- `prev_version_seq < version_seq`，禁止序号回退；
- 前一版本必须属于同一行；
- 引用不得成环；
- 非回滚版本不能形成多个无引用尖端；
- 正式 `row_heads` 必须指向有效尖端。

分叉时返回 `E_VERSION_CHAIN_BROKEN`，上下文包含 `row_key`、`fork_version_seq` 和 `head_version_seqs`。`RepairChain` 只向旧方向寻找“当前点及全部祖先均存在且 COMMITTED”的最后完整前缀，将行头回退到该点，并把其后的分叉/坏尾标记为 `detached=1`；不会插入或猜测缺失版本。

## 快照文件

快照 JSON 字段为：

```json
{
  "snapshot_seq": 2,
  "generated_at_ms": 1764000099000,
  "txn_upper_bound": 17,
  "checksum": "<sha256>"
}
```

摘要是对固定规范文本
`mvcc-snapshot-v1\\n<snapshot_seq>\\n<generated_at_ms>\\n<txn_upper_bound>`
计算的 SHA-256。加载时先校验摘要，再在引擎写锁内校验序号严格递增和上界不回退；任何失败都返回 `E_SNAPSHOT_INVALID`，内存旧快照与 SQLite 当前指针均不变。成功时整体替换 `current_snapshot`，并列出仍固定旧读视图的活跃事务序号。

## SQLite 意图与恢复

主要表：`txns`、`versions`、`row_heads`、`snapshots`、`intents`、`idempotent_requests`、`reclaimed_versions`、`meta`。

写入版本、提交/回滚、切换快照、修复链和回收前，先独立提交一条 `intents.status=PENDING` 记录。随后的业务事实、意图置为 `DONE`、幂等响应在同一个 SQLite 事务中提交。SQLite 的事务原子性保证不会出现“事务 COMMITTED 但版本仍 UNCOMMITTED”或“行头已更新但 prev 引用未落库”的中间可见状态。

重启时 `RecoverPending` 只依据已落盘事实处理：

- 未完成的 `write_version`：若版本存在则删除并把行头退回 `prev_version_seq`，报告 `deleted_version_seqs` / `rolled_back_version_seqs`。
- 未完成的 `commit_txn`：若事实未显示事务和版本已提交，则把事务及版本标记为 `ABORTED`，行头回退到最后已提交版本；若事实已完整提交，则把意图对账为 `DONE`。
- 无提交意图的其他 `ACTIVE` 事务保持 `ACTIVE`。
- 未完成的 `abort_txn` 前滚为 ABORTED。
- 未完成的 `switch_snapshot` 废弃意图，保持旧快照。
- 未完成的 `reclaim` 根据审计表和版本事实前滚或废弃。
- 遗留的 PENDING 幂等键标记为失败/中断，并进入 `interrupted_request_keys`。

## 并发与幂等

- `sync.RWMutex` 保护内存事实；开启事务、写入、提交、回滚、链修复、快照切换和回收均为单写者临界区。
- 读操作在 RLock 下复制一份小型视图后立即放锁，可见性计算在锁外进行，多个读者可并发。
- SQLite `MaxOpenConns(1)` + WAL/busy timeout 避免写竞争。
- `Idempotency-Key`、`X-Request-ID` 或 JSON 字段 `request_id` 均可提供请求标识。同 ID 同内容的并发调用共享同一个完成通道和结果；同 ID 不同 `scope` 或规范化载荷返回 `E_CONFLICTING_REQUEST`。完成结果同时持久化在 `idempotent_requests.response_json`。

## 回收裁决

当旧版本存在 `superseding_commit_seq` 时，回收器查找所有满足 `txn.status=ACTIVE && txn.upper_bound < superseding_commit_seq` 的快照。存在阻塞者时：

- 显式回收该版本返回 `E_RECLAIM_UNSAFE`；
- 上下文包含 `row_key`、`version_seq`、`superseding_commit_seq`、`earliest_blocking_snapshot_txn_seq`、`blocking_snapshot_txn_seqs` 和 `remaining_references`。

无阻塞时先写 `reclaim` 意图，再在同一 SQLite 事务中写 `reclaimed_versions` 审计并删除版本。已回收版本的悬挂 `prev` 引用会被识别为安全回收边界，而不是链损坏。

## HTTP API

所有业务响应为 JSON；失败响应形如：

```json
{
  "ok": false,
  "code": "E_TXN_INVALID",
  "message": "...",
  "context": {"txn_seq": 999},
  "error": {"code": "E_TXN_INVALID", "message": "...", "context": {}}
}
```

端点：

- `POST /txns/begin`、`POST /txns/commit`、`POST /txns/rollback`
- `POST /versions`（别名 `POST /rows/versions`）
- `POST /rows/read`、`GET /rows?txn_seq=1&row_key=k`、`GET /rows/read`
- `POST /snapshots/switch`、`GET /snapshots/current`
- `POST /chains/validate`、`GET /chains/validate?row_key=k`、`POST /chains/repair`
- `POST /reclaim`
- `GET /state`、`GET /recovery`、`POST /recovery/recover`
- `POST /samples/import`、`POST /samples/validate`
- `GET /healthz`

## 常用命令

```bash
go version
go test ./... -count=1
go test -race ./internal/engine ./internal/httpapi -count=1   # 可选，需要本机 C 工具链
go run ./cmd/demo
go run ./cmd/mvcc-server -addr 127.0.0.1:18080 -db ./data/mvcc.db -snapshot-dir ./data/snapshots
```

离线模块缓存可用时：

```bash
GOPROXY=off CGO_ENABLED=0 go test ./... -count=1
GOPROXY=off CGO_ENABLED=0 go run ./cmd/demo
```

Windows PowerShell 使用独立可写目录：

```powershell
$dir = Join-Path $env:TEMP ("mvcc-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $dir | Out-Null
go run ./cmd/mvcc-server -addr 127.0.0.1:18080 `
  -db (Join-Path $dir "mvcc.db") `
  -snapshot-dir (Join-Path $dir "snapshots")
```

`go run ./cmd/demo` 会自行创建 `%TEMP%\mvcc-demo-*`，运行固定样本、HTTP 并发提交、幂等等待、链分叉/修复、快照校验/切换、真实子进程 SIGKILL 后重启恢复，以及长快照阻塞回收。成功时输出完整 JSON 报告和 `DEMO_OK`。
