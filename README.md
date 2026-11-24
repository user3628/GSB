# 本地内容寻址块池（blockpool）

这是一个从空仓库构建的本地内容块池。它把 SHA-256 内容块作为第一等实体，由一个或多个
逻辑节点通过带版本、带到期时刻的租约引用块；物理内容只保存一份，引用计数与当前有效
引用集合在同一 SQLite 事务中变更。组件同时提供 Go 包、回环地址 HTTP API、固定种子
样本和可复现的租约失效/回收竞态演示。

## 实现边界

- 目标环境：Windows 10/Server 2019+、Go 1.22+、纯 Go SQLite `modernc.org/sqlite`，无需 CGO。
- 本组件是单个本地活动进程的组件；跨进程一致性由 SQLite 事务与重启恢复保证。HTTP 服务只允许绑定 `127.0.0.1`、`::1` 或 `localhost`。
- 样本中的逻辑路径是普通字符串，不是本机真实文件路径，示例使用 Windows 风格的 `C:/...`。
- 组件不使用 Docker、容器或外部对象存储。

## 代码结构

- `pool.go`、`schema.go`：打开数据库、SQLite 模式、WAL、迁移、键级锁。
- `block.go`、`lease.go`、`batch.go`、`reclaim.go`：入库、挂载/续租/卸载、批量替换、GC。
- `idem.go`：请求认领、持久化意图、结果复用和 `E_CONFLICTING_REQUEST`。
- `recovery.go`：重启扫描、过期物化、引用计数重建和差异报告。
- `sample.go`、`cmd/gensamples`：严格样本解析器与固定种子生成器。
- `httpapi/`：统一 JSON 错误和本地 HTTP 接口。
- `cmd/demo/`：装载固定样本并提供租约过期、回收竞态演示端点。
- `samples/fixed.json`：固定种子 `20261124` 的确定性样本。

## 快速验证（PowerShell）

```powershell
go mod download
go build ./...
go test ./...
go test -race ./...
go vet ./...

# 可选：重新生成样本，结果应与已提交文件逐字节一致
go run ./cmd/gensamples -seed 20261124 -out samples/fixed.json
```

Windows 纯 Go 交叉构建检查也已执行：

```powershell
$env:GOOS="windows"; $env:GOARCH="amd64"; $env:CGO_ENABLED="0"; go build ./...
```

## 核心不变量

1. `blocks.digest` 是唯一主键；相同摘要再次入库只复用已有物理内容。重新入库已回收的
   摘要时，必须再次提供匹配字节，内容校验失败返回 `E_CONTENT_CORRUPT`。
2. `refs` 每个 `(namespace, path, digest)` 最多一行当前引用；`leases` 保存逐次租约历史，
   `version` 对同一节点+块单调递增。只有 `expires_at_ms > now` 的引用有效。
3. 挂载、卸载、批量替换、回收的计数变更与引用/租约/回收记录处于同一事务。事务提交后
   会检查 `blocks.ref_count == COUNT(refs)`。
4. 挂载和回收共享同一摘要键锁。回收事务再次检查有效引用数、有效租约数、保留期和块
   大小；因此不会出现“回收已提交但新租约仍成功”或“回收后仍有非零引用”。
5. 读取使用 SQLite 只读事务，看到某一完整提交版本。读取块前先把已持久化的过期引用
   物化为失效，因此之后返回的有效租约数、引用数与 `ref_count` 一致。
6. 请求先认领 `requests` 行，再单独提交 `intents` 行，最后执行业务事务；业务提交与
   请求结果、intent 完成标记同事务提交。重复请求等待并使用已持久化结果。

## SQLite 持久化

数据库保存以下事实：

- `blocks`：摘要、大小、入库毫秒、保留期毫秒、状态、持久化引用计数、内容 BLOB。
- `nodes` / `namespaces`：命名空间和命名空间+路径标识的逻辑节点。
- `refs`：当前有效引用；`leases`：所有租约版本及 `active/expired/unmounted` 状态。
- `reclaim_records`：真实移除内容的审计记录。
- `intents`、`requests`：意图日志、幂等结果；`batch_locks`：节点批量替换在途锁。
- `recovery_reports`、`recovery_diffs`：每次成功打开时基于落盘事实生成的恢复报告。

块状态为 `live`、`pending_reclaim`、`reclaimed`。回收事务内部经过 `pending_reclaim`，并在
同一提交中设置 `reclaimed` 和 `content=NULL`，外部读者不会看到中间状态。

## HTTP API

所有失败响应都是统一结构：

```json
{
  "code": "E_LEASE_HELD",
  "message": "another mount issued the lease while this request was in flight",
  "context": {"version": 2, "expires_at_ms": 1800000060000}
}
```

业务码与 HTTP 状态：

| 业务码 | HTTP | 含义 |
| --- | ---: | --- |
| `E_SPEC_INVALID` | 400 | 参数、摘要、命名空间、非负整数或溢出校验失败 |
| `E_CONTENT_CORRUPT` | 409 | 字节内容与摘要/声明大小不一致，或批量所需内容不可用 |
| `E_LEASE_STALE` | 409 | 续租版本不是当前版本，`context.current_version` 给出当前版本 |
| `E_LEASE_HELD` | 409 | 并发挂载已有新租约，`context` 给出版本和到期时刻 |
| `E_NODE_BUSY` | 409 | 同一节点已有批量替换在途 |
| `E_RECLAIM_UNSAFE` | 409 | 有引用、有效租约、保留期未过或状态不安全 |
| `E_CONFLICTING_REQUEST` | 409 | 同一 `request_id` 被不同操作或不同载荷复用 |
| `E_NODE_UNKNOWN` / `E_BLOCK_NOT_FOUND` | 404 | 节点未注册或块没有持久化事实 |
| `E_INTERNAL` | 500 | 不变量或持久化错误 |

### 接口列表

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/namespaces` | 声明命名空间，body `{"name":"ns"}` |
| `POST /v1/nodes` | 预先注册节点，body `{"namespace":"ns","path":"dir/file@1"}`；挂载也会在已声明命名空间下自动登记节点 |
| `POST /v1/blocks` | 入库，body 含 `digest`、`data_base64`、可选 `size_bytes`、`retention_ms`、`request_id` |
| `POST /v1/mount` | 挂载，body 含 `namespace`、`path`、`digest`、可选 `lease_ms`、`request_id` |
| `POST /v1/renew` | 续租，body 另含当前 `version` |
| `POST /v1/unmount` | 立即卸载并递减计数 |
| `POST /v1/nodes/replace` | 原子替换节点期望引用全集，`blocks` 元素可带内容字节；同一节点重复在途返回 `E_NODE_BUSY` |
| `POST /v1/reclaim` | 指定 `digest` 安全回收，或给出 `capacity_bytes` 按容量预算调度 |
| `GET /v1/blocks/{digest}` | 查询块元数据、引用数、有效租约 |
| `GET /v1/blocks/{digest}/leases?history=1` | 查询有效租约或完整租约历史 |
| `GET /v1/blocks/{digest}/content` | 读取并校验仍为 `live` 的块字节，响应头含 `X-Content-SHA256` |
| `GET /v1/recovery` | 最新恢复报告与差异明细 |
| `GET /v1/health` | 本地健康检查 |

`request_id` 也可放在 `Idempotency-Key` 或 `X-Request-ID` 请求头中。相同 ID、相同操作、
相同规范化载荷只执行一次；不同载荷返回 `E_CONFLICTING_REQUEST`。批量替换不带内容的
摘要必须已经由入库存在且内容校验通过。

### PowerShell 示例

```powershell
$base = "http://127.0.0.1:18080"
$ns = @{ name = "demo" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/v1/namespaces" -ContentType "application/json" -Body $ns

$node = @{ namespace = "demo"; path = "dir/file@1" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/v1/nodes" -ContentType "application/json" -Body $node

$bytes = [Text.Encoding]::UTF8.GetBytes("hello block")
$sha = [Security.Cryptography.SHA256]::Create()
$digest = [Convert]::ToHexString($sha.ComputeHash($bytes)).ToLowerInvariant()
$sha.Dispose()

$ingest = @{
  request_id = "ingest-1"
  digest = $digest
  data_base64 = [Convert]::ToBase64String($bytes)
  retention_ms = 0
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/v1/blocks" -ContentType "application/json" -Body $ingest

$mount = @{
  request_id = "mount-1"
  namespace = "demo"
  path = "dir/file@1"
  digest = $digest
  lease_ms = 5000
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/v1/mount" -ContentType "application/json" -Body $mount

Invoke-RestMethod -Uri "$base/v1/blocks/$digest"
```

## 固定样本与演示

`cmd/gensamples` 使用 SplitMix64 固定种子生成 3 个命名空间、9 个逻辑节点、8 个块和 34
个乱序动作。动作包含跨节点共享摘要、重复挂载、续租、卸载、批量替换以及一个
`retention_ms=0` 块的安全回收。样本装载与逐步回放都在测试中执行。

启动演示服务（会只监听回环地址；端口被占用时会失败退出）：

```powershell
go run ./cmd/demo -addr 127.0.0.1:18080 -db .demo/blockpool.sqlite -sample samples/fixed.json
```

演示端点：

```powershell
# 短租约 -> 等待持久化到期 -> 查询计数归零 -> 回收
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18080/v1/demo/lease-expiry"

# 同时发起挂载与回收，响应给出两方结果、最终状态和互斥结论
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18080/v1/demo/reclaim-race"

Invoke-RestMethod -Uri "http://127.0.0.1:18080/v1/demo/info"
```

这两个端点会为每次调用注册新的逻辑节点，并复用固定样本中的 8192 字节块。演示完成后
清理引用；内容被回收后，下一次调用从固定样本字节重新入库。

## 回收语义

单个摘要回收的安全条件必须全部成立：

1. 持久化 `ref_count == 0`；
2. `refs` 中不存在 `expires_at_ms > now` 的行；
3. `stored_at_ms + retention_ms <= now`（加法做 `int64` 溢出保护）；
4. 块仍为 `live`。

不满足时返回 `E_RECLAIM_UNSAFE`，`context` 给出 `ref_count`、`effective_lease_count`、
`remaining_retention_ms` 和 `blockers`。容量回收按 `size_bytes DESC, digest ASC` 稳定排序，
只选安全候选并进行真实内容删除，返回 `reclaimed` 清单与 `released_bytes`。预算仍不足
但没有一个安全候选时返回 `E_RECLAIM_UNSAFE`。

## 恢复语义与限制

打开数据库时执行以下恢复：

1. 扫描 `intents.state='pending'`，将它们标记为 `aborted`，并把对应未完成请求标记为
   `aborted`；不把未提交意图当作业务事实。
2. 依据持久化到期时刻删除过期 `refs`、标记过期租约。
3. 对每个块依据仍有效的 `refs` 重建引用计数；与旧 `ref_count` 不同时将差异写入恢复报告。
4. 修复“有 active 引用但租约状态错误”和孤立 active 租约。

恢复只依据已提交的 SQLite 行。进程被强制杀死时不可能保证写出退出报告；报告只会在
下一次成功 `Open` 后产生，`note` 字段明确说明这一点。测试通过独立子进程写持久化事实后
`os.Exit(17)`，父进程重开数据库，验证差异报告和引用计数重建。

## 实际验证结果（2026-11-24）

执行环境：提供的 Linux/WSL2 工作区，Go 1.27.4；另做 Windows amd64 交叉编译。结果：

- `go build ./...`：通过。
- `go test -count=1 ./...`：通过；根包 13 个行为测试，HTTP 包 2 个契约测试。
- `GOTOOLCHAIN=go1.22.12 go test -count=1 ./...`：通过，确认声明的最低 Go 版本可构建和运行。
- `go test -race ./...`：通过，无数据竞争。
- `go vet ./...`：通过。
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...`：通过。
- 固定样本生成两次并 `cmp`：逐字节一致。
- 演示 `POST /v1/demo/lease-expiry`：返回 `expiry_was_honored=true`，到期后引用数 0，
  随后释放 8192 字节。
- 演示 `POST /v1/demo/reclaim-race`：连续 20 次并发调用中 6 次挂载胜出（回收返回
  `E_RECLAIM_UNSAFE`，存在引用/有效租约），14 次回收胜出（挂载返回
  `E_BLOCK_NOT_FOUND` 且块状态为 `reclaimed`）；20 次均 `mutually_exclusive=true`。

测试覆盖跨节点去重计数、零字节块、租约到期与旧版本续租、并发挂载冲突、批量原子性和
节点忙锁、保留期/容量 GC、回收与挂载竞态、请求幂等等待、损坏计数重建、子进程强杀
恢复以及固定样本全量回放。
