# 上游连接池诊断工作台

这是一个面向正向代理场景的本地离线诊断工作台。它读取固定端点规格和客户端请求轨迹，按轨迹真实拨号本机回环上游测试服务、复用 TCP 连接、对上传做确定性限速，并维护每条连接的七阶段状态与每个端点的池占用。工作台通过本地 HTTP API 提交轨迹、查询任意轨迹时刻的快照和单连接阶段轨迹。

核心目标是结构一致性：同一轨迹整批提交与按时间分段提交得到相同的池占用、空闲队列、排队明细和连接阶段序列；进程在写入或提交中途被 `kill -9` 后，重启可从 SQLite 已落盘事实恢复到一致状态并继续计算。

## 快速开始（Windows / WSL）

前置条件：

- Go 1.22 或更高版本；`go version` 可见。
- `GOBIN` 与模块缓存可写；`go env GOBIN GOMODCACHE` 可检查。
- SQLite CLI（可选但建议）：`sqlite3 --version` 可见；用于抽查 `checkpoints`、`intents` 等表。
- 本地端口 `18700`（HTTP）、`18701`、`18702`（上游测试服务）可用。
- 不使用 Docker，不访问非回环目标。端点地址和 HTTP 监听地址会在启动时强制校验为回环地址。

Windows PowerShell 示例：

```powershell
winget install GoLang.Go
winget install SQLite.SQLite
go version
go env GOBIN GOMODCACHE
go build -o bin\poolctl.exe .\cmd\poolctl

# 终端 1、2：独立上游测试进程
.\bin\poolctl.exe upstream -listen 127.0.0.1:18701
.\bin\poolctl.exe upstream -listen 127.0.0.1:18702

# 终端 3：工作台
.\bin\poolctl.exe serve -config samples\lab.json -db data\workbench.db
```

Linux/WSL：

```bash
go build -o bin/poolctl ./cmd/poolctl
./bin/poolctl upstream -listen 127.0.0.1:18701 &
./bin/poolctl upstream -listen 127.0.0.1:18702 &
./bin/poolctl serve -config samples/lab.json -db data/workbench.db
```

工作台启动时自动加载 `samples/lab.json` 中声明的端点，并将端点摘要和所有运行状态写入 SQLite。Go 侧使用纯 Go 的 `modernc.org/sqlite v1.34.5`，不需要 CGO 或本地数据库服务。项目 `go.mod` 固定为 `go 1.22.0`，并已验证 Windows amd64 交叉编译。

## 样本

- `samples/lab.json`：端点规格、默认超时、检查点间隔。`edge-a` 展示复用与空闲回收；`edge-reject` 展示拒绝；`edge-queue` 展示排队超时；`edge-down` 展示健康状态拒绝；`edge-refused` 用于真实拨号失败。
- `samples/track-full.json`：整批轨迹，包含三次同连接使用、复用上限后的 DRAINING/CLOSED、REJECT 和 DOWN。
- `samples/track-segment-1.json` / `track-segment-2.json`：按时间戳拆分同一轨迹，使用相同 `track_id`、不同 `batch_id`。
- `samples/track-exhaustion.json`：慢发送端占满单连接池，两个等待请求在 120ms 后返回 `E_POOL_ACQUIRE_TIMEOUT`。
- `samples/track-dial-failure.json`：引用不可达回环端口，真实拨号返回 `E_DIAL_FAILED`。
- `samples/track-crash-segment-*.json`：强制终止恢复演示样本。

单连接最大复用次数的含义是“首次连接后的额外复用次数”。例如 `max_reuses=2` 时，同一连接最多使用 3 次；第三次响应完成后必须依次进入 `DRAINING`、`CLOSED`，之后其池槽位才释放。

## 演示与验证

Linux/WSL：

```bash
bash scripts/demo.sh
```

Windows PowerShell：

```powershell
.\scripts\demo.ps1
```

脚本会构建二进制、启动两个独立回环上游进程和工作台，然后自动验证：

- 整批轨迹与两段轨迹在最终时刻和 5 个中途时刻的快照逐项一致。
- 重复提交同一批次不推进；同批次不同内容返回 `E_CONFLICTING_BATCH`。
- REJECT 策略返回 `E_POOL_EXHAUSTED`，排队超时返回 `E_POOL_ACQUIRE_TIMEOUT` 且附带 `waited_ms`。
- 拨号拒绝返回 `E_DIAL_FAILED`。
- 在 `APPLY_PLAN` 意图落盘后强制终止工作台；重启产生 ABORTED 意图、恢复批次并续算，最终快照与未中断基线一致。

测试：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

实际执行结果记录在 `VERIFICATION.md`，最近一次完整演示输出在 `artifacts/demo-run.txt`。

## HTTP API

### 提交批次

```http
POST /v1/tracks
Content-Type: application/json

{
  "track_id": "demo-segmented",
  "batch_id": "segment-1",
  "requests": [
    {
      "request_id": "r1",
      "timestamp_ms": 0,
      "client_id": "client-a",
      "endpoint_id": "edge-a",
      "request_bytes": 1000,
      "upload_rate_bps": 10000,
      "response_bytes": 100
    }
  ]
}
```

同一轨迹的分段必须使用相同 `track_id`、不同 `batch_id`。同一 `batch_id` 重复提交且规范化内容相同只返回原结果并标记 `advanced=false`；内容不同返回 `E_CONFLICTING_BATCH`。

响应包含每个请求的 `status`、错误体、连接标识、排队等待毫秒、完成轨迹时刻，以及该提交完成后的最终快照：

```json
{
  "advanced": true,
  "requests": [
    {
      "request_id": "wait-1",
      "status": "ERROR",
      "error": {
        "code": "E_POOL_ACQUIRE_TIMEOUT",
        "details": {"waited_ms": 120}
      }
    }
  ],
  "snapshot": {"seq": 10, "at_ms": 140, "endpoints": [], "connections": [], "queue": []}
}
```

### 查询快照

```http
GET /v1/tracks/{track_id}/snapshot
GET /v1/tracks/{track_id}/snapshot?at_ms=500
```

不带 `at_ms` 返回轨迹完成时刻的默认最终快照；带 `at_ms` 返回该轨迹时刻之前最后一个完整提交版本。每个快照包含端点活跃数、空闲槽位、空闲连接有序队列、连接阶段、复用次数、当前请求和排队明细。

### 查询单连接轨迹

```http
GET /v1/tracks/{track_id}/connections/{connection_id}/trace
```

连接标识形如 `edge-a-c000001`。响应为逐条阶段迁移，包含全局序号、轨迹时刻、起止阶段、关联请求和原因。

### 其他接口

- `GET /healthz`：健康检查。
- `GET /v1/endpoints`：已加载端点规格。
- `GET /v1/recovery`：最近一次恢复报告，只列出已落盘意图、待恢复批次和已恢复批次。
- `POST /v1/statemachine/validate`：校验 `from`/`to` 阶段组合；非法组合返回 `E_INVALID_TRANSITION`。

## 错误码

| 业务码 | 含义 | HTTP/位置 |
| --- | --- | --- |
| `E_SPEC_INVALID` | 规格、轨迹、溢出或未声明端点；整批不入池 | 400 |
| `E_CONFLICTING_BATCH` | 批次标识已用于不同内容 | 409 |
| `E_INVALID_TRANSITION` | 非法连接阶段迁移，不修改持久化状态 | 409 |
| `E_POOL_ACQUIRE_TIMEOUT` | 有界队列等待超时，附 `waited_ms` | 批次请求结果 |
| `E_POOL_EXHAUSTED` | 池满且策略拒绝或队列已满 | 批次请求结果 |
| `E_DIAL_FAILED` / `E_DIAL_TIMEOUT` | 真实拨号失败/超时，套接字关闭 | 批次请求结果 |
| `E_SEND_TIMEOUT` | 单次写入截止时间超时，套接字真实关闭 | 批次请求结果 |
| `E_RECV_TIMEOUT` | 响应读取超时，套接字真实关闭 | 批次请求结果 |
| `E_ENDPOINT_UNHEALTHY` | 端点健康状态为 DOWN | 批次请求结果 |
| `E_CHECKPOINT_CORRUPT` | 检查点校验和或 JSON 损坏 | 500 |

批次级错误体统一为 `{"code":"...","message":"...","details":{...}}`。被批次接纳后，单个请求的失败位于 `requests[].error`，不会破坏其他请求的持久化结果。

## 阶段状态机

合法迁移如下；`max_reuses` 用尽、空闲回收和传输失败都只能经 `DRAINING` 到 `CLOSED`：

```text
(create)      -> CONNECTING
CONNECTING    -> HANDSHAKING | CLOSED
HANDSHAKING   -> SEND_BODY | DRAINING | CLOSED
SEND_BODY     -> RECV_RESPONSE | DRAINING | CLOSED
RECV_RESPONSE -> IDLE | DRAINING | CLOSED
IDLE          -> HANDSHAKING | DRAINING
DRAINING      -> CLOSED
CLOSED        -> (terminal)
```

复用路径固定为 `IDLE -> HANDSHAKING -> SEND_BODY`。同一连接同一时刻最多有一个请求持有者。

## 目录

```text
cmd/poolctl/          命令行入口：serve、upstream、inspect
internal/config/      规格和溢出校验
internal/statemachine/七阶段状态机
internal/sim/         确定性轨迹事件模拟与快照生成
internal/client/      真实拨号、限速写入、逐写超时
internal/upstream/    本地回环上游测试服务
internal/store/       SQLite schema、意图、检查点、快照与恢复
internal/service/     批次编排、幂等、I/O 回灌、恢复续算
internal/httpapi/     本地 HTTP 接口和统一错误体
samples/              固定端点、轨迹、分段与故障样本
scripts/              可重复演示脚本（Bash / PowerShell）
artifacts/            最近一次演示输出
```

详细设计取舍见 `TECHNICAL.md`。

