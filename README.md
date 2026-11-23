# 本地对象存储分片上传事务日志与元数据重建引擎

这是一个面向本地对象存储场景的 Go 服务：上传会话把对象切分为有序分片，SQLite 持久化会话、分片内容与摘要、对象元数据、事务意图日志和请求幂等结果。服务提供本地 HTTP API，支持乱序/幂等分片提交、同键代际裁决、取消与 TTL 回收、并发摘要复核、原子合并和重启恢复。

## 环境与构建

- Go 1.22+
- SQLite 驱动：`modernc.org/sqlite v1.34.5`（纯 Go，无 CGO，可直接在 Windows 构建）
- 不依赖 Docker、外部账号或网络服务

Windows PowerShell：

```powershell
go mod download
go test ./...
go build -o objectstore.exe .\cmd\objectstore
.\objectstore.exe -db .\objectstore.db -addr 127.0.0.1:8080
```

也可以直接在仓库根目录运行 `go run .`，参数相同。

启动参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-db` | `objectstore.db` | SQLite 文件，WAL + `synchronous=FULL` |
| `-addr` | `127.0.0.1:8080` | 本地回环监听地址 |
| `-verify-concurrency` | `4` | 全局同时执行的分片摘要复核数 |
| `-verify-queue` | `32` | 全局有界等待队列长度；满了返回 `E_VERIFY_QUEUE_FULL` |
| `-verify-budget-ms` | `2000` | 每轮复核预算；`<= 0` 时不启动复核并输出全部跳过清单 |
| `-reap-interval` | `1s` | TTL 过期会话回收周期；`0` 关闭后台回收 |

服务启动时会先执行一次恢复，再开始监听 HTTP。

## 核心语义

- **对象摘要**：对所有有效分片按 `part_number` 升序拼接原始字节后计算 SHA-256。乱序到达不改变该结果。
- **幂等分片**：同一会话、同一序号首次成功后，摘要相同的重复提交累计 `duplicate_count`，不新增分片；摘要不同返回 `E_PART_MISMATCH` 并保留首次内容。
- **单活跃会话**：同一对象键任意时刻最多一个 `active` 会话，由 SQLite 部分唯一索引和事务共同保证。其他会话按递增 `generation` 标识。
- **取消/过期**：分片在同一事务内迁移为 `void`，并释放对象键的活跃会话槽位；这些分片不参与合并和对象重建。
- **原子合并**：验证连续性、大小、请求期望值和摘要后，对象元数据与会话 `completed` 状态在同一事务写入。失败不会留下部分对象元数据。
- **请求幂等**：`Idempotency-Key` 头或请求体 `request_id` 作为请求标识。相同标识和相同规范化内容只执行一次，后续复用已持久化的完整 HTTP 结果；内容不同返回 `E_CONFLICTING_REQUEST`。

## HTTP API

所有 POST/PUT 请求使用 JSON。成功响应直接返回 JSON 对象；失败响应统一为：

```json
{
  "code": "E_PART_MISSING",
  "message": "part sequence contains gaps",
  "context": {"missing_part_numbers": [2]},
  "request_id": "part-1"
}
```

| 方法与路径 | 用途 |
| --- | --- |
| `POST /v1/uploads` | 发起上传会话 |
| `GET /v1/uploads/{session_id}` | 查询会话状态 |
| `GET /v1/uploads/{session_id}/parts` | 查询分片清单（不含正文） |
| `PUT /v1/uploads/{session_id}/parts/{part_number}` | 提交一个分片 |
| `POST /v1/uploads/{session_id}/merge` | 复核并原子合并 |
| `POST /v1/uploads/{session_id}/cancel` | 取消会话并作废分片 |
| `GET /v1/objects?key={object_key}` | 查询当前对象元数据 |
| `GET /v1/objects/{object_key...}` | 查询当前对象元数据（对象键可含 `/`） |
| `POST /v1/admin/recover` | 执行恢复并返回新报告 |
| `GET /v1/admin/recovery-report[?report_id=N]` | 查询最新或指定恢复报告 |
| `POST /v1/admin/reclaim-expired` | 主动回收过期会话 |
| `GET /healthz` | SQLite 健康检查 |

`Idempotency-Key`、`X-Request-ID`、`X-Idempotency-Key` 任一请求头与请求体 `request_id` 均可提供请求标识；多个来源必须一致。

### 发起会话

```json
{
  "request_id": "create-001",
  "object_key": "samples/catalog/items.json",
  "lower_bound": 1,
  "ttl_ms": 600000
}
```

`lower_bound` 省略时默认为 `1`；`ttl_ms` 省略或为 `0` 时默认为 10 分钟。同键存在未过期活跃会话时返回 `409 E_SESSION_CONFLICT`，`context.existing_session_id` 给出冲突会话。

### 提交分片

```json
{
  "request_id": "part-001",
  "size": 23,
  "sha256": "cf225e47e010e8695cf8e39125199ff2149b805f9f46f095166fca6e8161ad07",
  "arrived_at_ms": 100,
  "content_base64": "eyJpZCI6MSwibmFtZSI6ImFscGhhIn0="
}
```

服务会同时校验声明大小、内容实际 SHA-256 和幂等冲突。首次提交返回 `201`，相同内容重复提交返回 `200` 且 `duplicate=true`。`arrived_at_ms` 省略时使用服务器毫秒时间。

### 合并

```json
{
  "request_id": "merge-001",
  "expected_total_size": 68,
  "expected_part_count": 3
}
```

响应包含对象元数据、会话状态和复核报告：

```json
{
  "request_id": "merge-001",
  "object": {
    "object_key": "samples/catalog/items.json",
    "session_id": "upl_...",
    "generation": 1,
    "total_size": 68,
    "part_count": 3,
    "sha256": "3bbcaa6e982f1c4e82bd9d858d2312e279245783172de0003b173ea430a3e898",
    "updated_at_ms": 1800000000000
  },
  "session": {"status": "completed"},
  "verification": {
    "concurrency": 4,
    "queue_capacity": 32,
    "budget_ms": 2000,
    "verified_part_numbers": [1, 2, 3],
    "skipped_part_numbers": [],
    "degraded": false
  },
  "already_merged": false,
  "intent_seq": 5
}
```

复核按分片序号提交到全局调度器。超过并发上限的任务进入有界等待队列；队列满立即返回 `E_VERIFY_QUEUE_FULL`。预算耗尽后，尚未启动或仍在等待的确定性后缀列入 `skipped_part_numbers`，`degraded=true`。任何已执行复核失败都会返回 `E_PART_MISMATCH`，且不写对象元数据。

### 取消

```json
{"request_id": "cancel-001", "reason": "operator request"}
```

取消和过期均在同一事务内更新会话状态、作废分片并释放对象键活跃槽位。重复取消已完成回收的会话是幂等成功。

## 业务错误码

| 业务码 | HTTP | 含义 |
| --- | --- | --- |
| `E_SESSION_CONFLICT` | 409 | 同键已有未过期活跃会话 |
| `E_PART_MISMATCH` | 422 | 内容/记录摘要不一致，或重复分片摘要冲突 |
| `E_PART_MISSING` | 422 | 序号缺口或请求的分片数不匹配 |
| `E_SIZE_MISMATCH` | 422 | 声明大小、存储大小或期望总大小不一致 |
| `E_VERIFY_QUEUE_FULL` | 429 | 全局复核等待队列已满 |
| `E_CONFLICTING_REQUEST` | 409 | 请求标识已用于不同内容 |
| `E_SESSION_STATE` | 409 | 会话状态不允许当前操作 |
| `E_SESSION_NOT_FOUND` | 404 | 会话不存在 |
| `E_OBJECT_NOT_FOUND` | 404 | 对象或恢复报告不存在 |
| `E_INVALID_ARGUMENT` | 400 | 请求参数非法 |
| `E_INTERNAL` | 500 | 存储或内部错误 |

## 事务日志与恢复

每次创建、分片提交、取消、过期回收和合并的流程为：

1. 在独立事务中追加 `intents(status=pending)`，包含日志序号、会话、对象键、分片序号、摘要、大小和请求标识。
2. 在 `BEGIN IMMEDIATE` 数据事务中修改数据表，并在同一事务中将对应意图改为 `committed`。

因此进程在步骤 1 后中断只会留下 `pending` 意图，数据仍是最后一个完整提交版本。恢复入口会：

- 将 `pending` 意图标记为 `abandoned`，报告其 `applied=false`；
- 回收已过 TTL 的活跃会话并作废其分片；
- 读取已提交的会话、分片与对象表，生成完整快照报告；
- 校验已完成会话元数据、非活跃会话残留有效分片和过期活跃会话数量。

恢复报告只引用 SQLite 中已落盘的行和本次恢复已事务提交的回收动作，不推测未提交的数据。

## 固定样本与验证

`samples/scenario.json` 包含确定的三个对象键、分片原始内容、SHA-256、到达时刻和乱序提交序列，覆盖：

- 对象 A：分片按 `3,1,2` 到达，并重复提交分片 2；
- 对象 B：提交后取消，分片作废；
- 对象 C：第 1 代取消，第 2 代重新上传并合并。

`internal/engine/sample_test.go` 直接读取该文件回放全部步骤，并校验对象摘要、代际和恢复报告。

本地实际验证命令与结果：

```text
$ go test ./... -count=1
ok  local/objectstore/internal/engine
ok  local/objectstore/internal/httpapi

$ go test -race ./internal/engine ./internal/httpapi -count=1
ok  local/objectstore/internal/engine
ok  local/objectstore/internal/httpapi

$ GOOS=windows GOARCH=amd64 go build -o objectstore.exe ./cmd/objectstore
（构建成功，生成 Windows amd64 可执行文件）
```

真实 HTTP 冒烟测试记录见 `docs/VERIFICATION.md`。

## 目录结构

```text
main.go                根目录入口（go run .）
cmd/objectstore/       服务入口
internal/app/          共享启动、配置与优雅关闭逻辑
internal/model/        领域模型、状态与统一错误码
internal/store/        SQLite 模式、事务、意图日志、恢复
internal/engine/       会话/分片/合并规则与复核调度
internal/httpapi/      HTTP 路由、幂等请求协调、错误体
samples/               固定确定性上传样本
docs/                  设计与实际验证记录
```

