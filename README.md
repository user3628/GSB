# CI 制品缓存与复用组件

这是一个可直接在 Windows 本地运行的 CI 制品缓存实现。它面向由“阶段标识 + 输入指纹 + 工具链版本”组成的缓存键，使用 SHA-256 内容摘要寻址制品，并通过 SQLite 纯 Go 驱动持久化缓存事实、状态、淘汰历史与意图日志。

实现不依赖 Docker、CGO 或外部服务。本地确定性样本位于 `samples/entries.json`，HTTP 端到端验证只绑定 `127.0.0.1` 临时端口并使用临时目录。

## 快速开始

要求 Go 1.22 或更高版本。模块本身声明 `go 1.22`，SQLite 驱动为 `modernc.org/sqlite`。

```powershell
go mod download
go test -count=1 ./...
go run ./cmd/cachectl validate-samples --samples samples/entries.json
go run ./cmd/cachectl verify --samples samples/entries.json
```

启动本地 HTTP 端点。`CACHE_ADDR` 是完整监听地址，端口设为 `0` 时由操作系统分配空闲端口，启动日志会输出实际地址：

```powershell
$env:CACHE_ADDR = "127.0.0.1:0"
$env:CACHE_DIR = Join-Path $PWD ".cache-data"
$env:CACHE_CAPACITY_BYTES = "1073741824"
go run ./cmd/cachectl serve
```

也可以直接运行：

```powershell
.\scripts\start-server.ps1 -Port 0
.\scripts\verify.ps1
```

启动成功后首行输出类似：

```json
{"capacity_bytes":1073741824,"dir":"C:\\work\\.cache-data","endpoint":"http://127.0.0.1:51234","event":"listening"}
```

## 目录结构

- `cmd/cachectl`：HTTP 服务、样本校验、端到端验证与强杀辅助命令。
- `internal/cache`：缓存模型、状态机、SQLite 持久化、写入/命中/淘汰、快照、恢复。
- `internal/httpapi`：本地 HTTP 契约与统一错误响应。
- `internal/verify`：固定样本的 HTTP 端到端核对工具。
- `samples/entries.json`：固定本地确定性样本。
- `scripts/*.ps1`：Windows PowerShell 启动与验证脚本。
- `VALIDATION.md`：本环境实际执行结果。

## 核心设计

### 数据与制品

缓存键先规范化：三个阶段字段做首尾空白清理；输入指纹本身允许任意非空字符串，若它是 `sha256:` 摘要则统一为小写。随后用带长度前缀的 UTF-8 字节串计算 SHA-256。制品摘要必须是 `sha256:` 加 64 位小写十六进制。

制品文件保存在 `CACHE_DIR/artifacts/<64位摘要>.bin`。SQLite 的 `artifacts` 表记录摘要、大小、相对路径与引用计数；只有引用计数归零时才删除物理文件。这样相同摘要可被多个阶段条目复用。

样本字段如下，整批先校验后写入；任一条非法都会返回 `E_ENTRY_INVALID`，不会持久化已经通过的合法前缀：

```json
{
  "entries": [
    {
      "stage_id": "build",
      "input_fingerprint": "sha256:...",
      "toolchain_version": "go1.22.5",
      "artifact_content": "artifact-alpha\n",
      "artifact_digest": "sha256:...",
      "artifact_size": 15,
      "retain_until": "2030-01-01T00:00:00Z"
    }
  ]
}
```

校验包括摘要格式、非负字节数、声明大小与实际内容一致、声明摘要与实际内容一致、保留期为 RFC3339。

### 状态机

```text
PENDING -> WRITING -> AVAILABLE -> STALE -> EVICTED
```

- 写入开始前后先落 `WRITE` 意图，再把条目以 `PENDING` 持久化。
- `PENDING -> WRITING` 和 `WRITING -> AVAILABLE` 都有独立意图记录。
- 只有制品文件已落盘、两次 SHA-256 复核通过后，才允许进入 `AVAILABLE`。
- `AVAILABLE -> STALE` 用于保留期到期或容量淘汰前置。
- `STALE -> EVICTED` 必须走真实淘汰事务，减少引用计数、写淘汰历史、必要时删除文件。
- 所有其他迁移返回 `E_INVALID_TRANSITION`，并且校验发生在写意图和数据库更新之前。

公开迁移接口也会在 `WRITING -> AVAILABLE` 时登记制品占用，在 `STALE -> EVICTED` 时复用完整淘汰回收路径。

### 并发与可见性

- 所有写入、迁移、命中统计更新和淘汰由进程内单写者互斥量串行化。
- 内存目录是不可变快照，通过原子指针发布。命中查询可以在同一完整提交版本上并发读取并复核摘要。
- 命中先并发复核，再在单写者临界区提交 `hits + 1` 和 `last_hit_at`，最后发布新的完整目录快照；响应不会返回“已命中但统计尚未提交”的中间状态。
- 淘汰事务提交后，在可见性写锁内切换目录并删除无引用文件，读者不会看到“已 EVICTED 但文件仍占用”的可见窗口。
- 相同缓存键的并发写入只有一个生效，其余返回同一条目并标记 `idempotent: true`；摘要不同则返回 `E_CONFLICTING_WRITE`。
- 已淘汰键再次写入会创建新的条目代次，旧 `EVICTED` 行继续保留用于未命中原因和历史命中统计。

### SQLite 与意图日志

数据库默认位于 `CACHE_DIR/cache.sqlite3`，使用 WAL、`synchronous=FULL` 与 busy timeout。核心表：

- `entries`：条目、状态、版本、命中统计与保留期。
- `artifacts`：内容摘要、字节数、相对路径与引用计数。
- `evictions`：淘汰时的条目、命中统计、原因与时间。
- `intents`：`WRITE`、`TRANSITION`、`EVICT`、`SNAPSHOT` 的 RUNNING/DONE/ABORTED 事实。
- `metadata`：单调快照序号。
- `recovery_reports`：持久化的恢复报告。

写入、迁移和淘汰均为“先提交 RUNNING 意图，再提交实际事务”，实际事务同时把意图改为 `DONE`。因此重启时仍为 `RUNNING` 的意图一定对应未提交动作。

### 崩溃恢复

服务启动时自动扫描 `RUNNING` 意图：

- 未完成 `WRITE`：把 `PENDING/WRITING` 条目回滚为 `PENDING`，清理未被 `artifacts` 表引用的孤儿文件与 staging 文件。
- 未完成 `EVICT`：把 `STALE` 条目回滚为 `AVAILABLE`，保留原制品引用。
- `TRANSITION`/`SNAPSHOT`：动作与意图完成处于同一事务，未提交时只把意图标记为 `ABORTED`。
- 恢复报告只统计数据库和文件系统中已经存在的事实，并写入 `recovery_reports`。

### 快照

`GET /v1/manifest` 导出带以下字段的快照：

- `snapshot_version`：当前为 `1`。
- `sequence`：严格单调，必须大于活动序号。
- `generated_at`：RFC3339Nano。
- `entries`：条目、状态、命中数和活动条目内容（base64）。
- `checksum`：对除 `checksum` 外的规范 JSON 计算 SHA-256。

加载会先完整校验 JSON、版本、序号、校验和、键规范化、状态、摘要、大小和内容。校验失败时不会替换数据库或内存目录，旧快照继续可用。成功路径是：校验并准备新制品文件 -> 写 `SNAPSHOT` 意图 -> SQLite 单事务替换表并推进序号 -> 原子发布新内存目录 -> 删除旧快照不再引用的文件。

### 淘汰顺序

`POST /v1/evict` 先扫描保留期已到期的 `AVAILABLE` 条目并迁为 `STALE`。所有 `STALE` 条目为强制淘汰候选；若回收后仍超容量，再追加 `AVAILABLE` 候选。确定性顺序为：

```text
last_hit_at ASC, artifact_size DESC, cache_key ASC, entry_id ASC
```

总占用按有引用制品的唯一字节数计算。共享摘要只在最后引用被淘汰时释放。每条淘汰都写 `EVICT` 意图，并在事务中更新条目、引用计数与 `evictions` 历史。

容量上限在显式 `POST /v1/evict` 触发时强制收敛；写入阶段不会隐式淘汰，便于 CI 明确记录“写入成功”与“随后触发回收”两个事件。

## HTTP API

所有响应为 JSON，失败统一为：

```json
{
  "code": "E_DIGEST_MISMATCH",
  "message": "artifact content failed digest recheck",
  "context": {
    "method": "GET",
    "path": "/v1/hit",
    "cache_key": "sha256:..."
  }
}
```

| 方法与路径 | 用途 |
| --- | --- |
| `GET /healthz` | 本地健康检查 |
| `POST /v1/entries` | 写入一个样本条目 |
| `GET /v1/hit` | 按键、可选期望摘要命中并返回制品内容 base64 |
| `POST /v1/entries/transition` | 显式状态迁移；非法路径返回业务码 |
| `GET /v1/manifest` | 导出快照；`?path=` 同时原子写文件 |
| `POST /v1/snapshots/switch` | 通过 `path` 或内联 `snapshot` 整体切换 |
| `POST /v1/evict` | 触发保留期回收与容量淘汰 |
| `GET /v1/recovery/report` | 查询最近一次持久化恢复报告 |

命中查询参数为 `stage_id`、`input_fingerprint`、`toolchain_version`，可选 `expected_digest`。

未命中原因位于 `context.reason`：

- `KEY_NOT_FOUND`：缓存键不存在。
- `EVICTED`：活动条目已淘汰。
- `NOT_AVAILABLE`：条目仍为 PENDING/WRITING。
- `EXPIRED`：保留期已过或条目已 STALE。
- 摘要不一致返回独立的 `E_DIGEST_MISMATCH`。

至少覆盖的业务码：

| 业务码 | HTTP 状态 | 含义 |
| --- | --- | --- |
| `E_ENTRY_INVALID` | 400 | 条目/样本/请求字段非法 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移 |
| `E_DIGEST_MISMATCH` | 422 | 期望摘要或落盘内容复核失败 |
| `E_ENTRY_EXPIRED` | 410 | 保留期到期或已 STALE |
| `E_CONFLICTING_WRITE` | 409 | 同键已存在不同内容 |
| `E_CACHE_MISS` | 404 | 可核对的未命中 |
| `E_SNAPSHOT_INVALID` | 400 | 快照版本/校验和/结构非法 |
| `E_SNAPSHOT_SEQUENCE` | 409 | 快照序号不单调 |
| `E_NOT_FOUND` | 404 | 未知 HTTP 路由 |
| `E_METHOD_NOT_ALLOWED` | 405 | 路由存在但 HTTP 方法不匹配 |

示例（PowerShell）：

```powershell
$entry = @{
  stage_id = "build"
  input_fingerprint = "sha256:f10ec3ffef16a8dace9b58801e0da28819c98af4c8933410d7e7cacc952f9e9b"
  toolchain_version = "go1.22.5"
  artifact_content = "artifact-alpha`n"
  artifact_digest = "sha256:70d5a7f7f762df5aaa93a52f0d5e4058b66682476efef74e9fa05d9347a2f0c1"
  artifact_size = 15
  retain_until = "2030-01-01T00:00:00Z"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri "$endpoint/v1/entries" -ContentType "application/json" -Body $entry
Invoke-RestMethod -Method Post -Uri "$endpoint/v1/evict"
Invoke-RestMethod -Method Get -Uri "$endpoint/v1/recovery/report"
```

## 端到端验证

```powershell
go run ./cmd/cachectl verify --samples samples/entries.json
```

验证器在回环地址启动随机端口 HTTP 服务，使用固定样本创建独立临时目录，并逐项比较预期值。覆盖：

- 写后命中、制品内容一致、命中统计为 1。
- 非法迁移 `AVAILABLE -> PENDING` 被拒绝。
- 同键不同内容返回 `E_CONFLICTING_WRITE`。
- 篡改落盘内容后命中返回 `E_DIGEST_MISMATCH`。
- 过期样本命中返回 `E_ENTRY_EXPIRED`。
- 容量超限时按确定性顺序淘汰并真实释放 46 字节。
- 淘汰后命中给出 `reason=EVICTED`。
- 相同序号的快照切换被拒绝，递增序号后成功切换。
- 子进程停在 WRITING 后被强制杀死，重启回滚为 PENDING。
- 子进程写入淘汰意图后被强制杀死，重启回滚为 AVAILABLE。

实际运行记录见 `VALIDATION.md`。当前环境结果为 `12/12 PASS`。

## 运行边界

- HTTP 端点默认只监听 `127.0.0.1`，无认证，不应绑定到不可信网络。
- “单写者”在单个服务进程内保证；多个服务进程不应共享同一个 `CACHE_DIR`。
- 容量约束在显式淘汰触发点收敛，不存在后台淘汰协程。
- 样本与 HTTP 请求中的 `artifact_content` 是 UTF-8 字符串；成功命中返回 base64，以支持一般字节内容的复用。
