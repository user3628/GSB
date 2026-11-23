# 本地配置中心热更新中间件

这是一个不依赖容器和外部服务的本地多服务实验工程。中间件用 SQLite 保存带版本号的配置快照、推送状态、确认历史、结果摘要和意图日志，并通过真实 loopback HTTP 连接向本地客户端测试端点推送增量。工程同时提供确定性客户端模拟器、HTTP API、异常注入实验和可重复验证产物。

## 快速复现

环境要求：Go 1.22+、本机可写临时目录、无需账号或外部服务。依赖只有 `modernc.org/sqlite v1.34.5`。

Linux / WSL：

```bash
go mod download
go test ./...
./scripts/run_experiments.sh
```

Windows PowerShell：

```powershell
go mod download
go test ./...
powershell -ExecutionPolicy Bypass -File scripts\run_experiments.ps1
```

若所在网络无法访问默认 Go 代理，可先设置可用的 `GOPROXY`（例如镜像）再执行 `go mod download`。实验运行期间不会下载样本或访问非 loopback 目标。

实验脚本会执行 `go test ./...`，然后由 `cmd/experiment` 清理并重建 `artifacts/experiment/` 下的运行数据库，启动本地客户端模拟器和多个真实 middleware 子进程，输出：

- `artifacts/experiment/summary.json`：逐项断言和最终 `all_passed`
- `01-timeout-*.json` 至 `06-*.json`：异常发生前后的触发响应、持久化状态、恢复报告
- `artifacts/experiment/run/middleware.db`：本次实验实际使用的 SQLite 数据库（被 `.gitignore` 忽略）

## 实现结构

| 路径 | 作用 |
| --- | --- |
| `cmd/middleware` | 配置中心 HTTP 服务，启动时执行恢复扫描 |
| `cmd/client-sim` | 本地确定性客户端测试端点，支持成功、延时、重复确认、错误版本等行为 |
| `cmd/experiment` | 五类异常及推送/结果两次强制终止的自动复现与断言 |
| `cmd/configctl` | 样本生成、快照摘要计算、确定性管线计算 |
| `internal/core` | 业务错误、状态机、快照/结果/确认模型与规范摘要 |
| `internal/pipeline` | 固定样本张量上的加权求和、归一化、阈值判定 |
| `internal/store` | SQLite schema、单写者、意图先落盘、只读事务 |
| `internal/engine` | 快照/客户端、推送编排、确认、结果校验、状态查询、恢复 |
| `internal/httpapi` | JSON HTTP API 和统一错误响应 |
| `internal/sim` | 测试端点的真实请求取消、重复 ACK 和统计 |
| `samples` | 固定快照、客户端清单、样本张量及管线说明 |

## 关键语义

### 快照

快照字段为 `version`、`generated_at`、`config`、`digest`。摘要算法是：

```text
sha256("snapshot.v1\n" + version + "\n" + generated_at + "\n" + canonical_config)
```

`canonical_config` 按配置键排序并用稳定浮点格式编码。提交时先校验摘要；摘要不一致返回 `E_SNAPSHOT_INVALID`。版本号必须严格大于当前最大已提交版本，否则返回 `E_STALE_SNAPSHOT`，且不改变已提交状态。

### 推送状态机

```text
"" --创建--> PENDING
PENDING -> PUSHED | ABORTED
PUSHED  -> ACKED  | ABORTED
ACKED   -> APPLIED | ABORTED
APPLIED -> terminal
ABORTED -> terminal
```

其他迁移返回 `E_INVALID_TRANSITION`，并在事务内回滚推送状态。每次迁移都写入 `push_transitions`。数据库对 `(client_id, version)` 建唯一约束；进程内还有 per-push flight，同版本并发重复触发会等待首个请求完成并复用同一结果。若客户端订阅内容已变化导致同一版本规范内容摘要不同，则返回 `E_CONFLICTING_PUSH`。

`PENDING` 在推送开始前落盘；发 HTTP 前转为 `PUSHED`。HTTP 响应中的确认通过单独的 ACK 事务转为 `ACKED`。只有确认存在、摘要匹配且结果校验通过后，才转为 `APPLIED`。

### 真实推送、超时和并发上限

- 每次推送使用独立 `context.WithTimeout`、独立 `http.Transport` 和独立 TCP 连接跟踪器。
- 推送载荷包含客户端订阅键解析后的完整配置 `config`，并携带相对该客户端最后 `APPLIED` 版本的 `base_version` 与仅变化键的 `changes`；没有已应用基线时 `base_version=0`、所有订阅键都进入增量。
- 超时取消请求 context，并显式关闭该次请求打开的底层连接；客户端模拟器同时观察到 request context 被取消。
- 超时返回 `E_PUSH_TIMEOUT`，连接拒绝返回 `E_PUSH_REFUSED`，端点非 2xx 返回 `E_PUSH_UPSTREAM`。
- 全局并发名额由固定容量信号量控制；每轮另有有界等待队列和总时间预算。排队或执行超过轮预算返回 `E_PUSH_BUDGET_EXCEEDED`。
- 网络结束后先释放并发名额，再持久化确认/失败结果，避免异常路径占住全局名额。

### 确认和迟到确认

确认必须同时匹配 `push_id`、客户端、配置版本和推送内容摘要；错位返回 `E_ACK_VERSION_MISMATCH`。确认与状态迁移、ACK 当前记录和 ACK 历史记录在同一事务提交。

判断迟到确认时查询该客户端是否存在更高版本且状态为 `PENDING/PUSHED/ACKED/APPLIED` 的推送。若存在：

- 只追加 `ack_events`，`disposition=STALE`
- 返回 `E_STALE_ACK`
- 不修改任何当前推送或当前确认

同一客户端并发收到两个版本时，较旧版本在确认阶段看到更新的非中止版本，因此被置为 `ABORTED/E_STALE_ACK`，只有较新版本能进入 `APPLIED`。

### 确定性计算管线

固定样本见 `samples/samples.json`：seed 为 `424242`，64 个二维向量，文件自身带摘要。算法固定在 `samples/pipeline.json`：

```text
weighted_sum = sum(x_i * weight_x + y_i * weight_y) + bias * len(vectors)
normalized   = weighted_sum * normalization_ratio
decision     = normalized >= decision_threshold
result_digest = sha256(canonical picked_version, seed, sample_digest,
                       weighted_sum, normalized, decision)
```

已提交样本 v1 的计算摘要为：

```text
weighted_sum = 0.6911695
normalized   = 0.5529356
decision     = true
digest       = sha256:4b43f8c85a16e24c874ebbbf6369f7eb795e9dbb8419e8d833f5628862163893
```

服务端在接收结果时用相同样本和对应快照配置重算。结果状态包括：

- `APPLIED`：版本是当前最新、ACK 版本一致、无更新推送、所有摘要/字段一致，并在同一事务中完成 `ACKED -> APPLIED`
- `MISMATCH`：字段、样本摘要或结果摘要与重算结果不一致，不推进状态
- `RECOMPUTE_REQUIRED`：结果来自旧版本、ACK 版本错位或已有更新版本，不能并入最新版本

`GET /api/v1/inconsistencies` 返回客户端标识、拾取版本、ACK 版本、结果摘要、差异字段、推送状态和 `can_merge_latest`，并分别列出可并入与必须重算的结果 ID。判定依据明确包含版本和确认记录。

## HTTP API

所有响应均为 JSON。业务错误统一格式：

```json
{
  "error": {
    "code": "E_PUSH_TIMEOUT",
    "message": "client push timed out and its connection was closed",
    "context": {"push_id": "push-...", "timeout_ms": 250, "connections_closed": 1}
  }
}
```

主要端点：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/v1/snapshots` | 提交校验过的快照 |
| `GET` | `/api/v1/snapshots/latest` | 最新已提交快照 |
| `GET` | `/api/v1/snapshots/{version}` | 指定版本快照 |
| `POST` | `/api/v1/clients` | 注册客户端，仅允许 loopback endpoint |
| `PUT` | `/api/v1/clients/{client_id}` | 更新依赖键、端点和拾取策略 |
| `GET` | `/api/v1/clients` | 客户端清单 |
| `POST` | `/api/v1/pushes` | 创建批次并向客户端真实推送 |
| `POST` | `/api/v1/acks` | 外部确认回传或重复/迟到确认测试 |
| `POST` | `/api/v1/results` | 上报确定性结果摘要并校验/应用 |
| `GET` | `/api/v1/inconsistencies` | 跨版本差异和并入/重算判定 |
| `GET` | `/api/v1/recovery` | 最近一次启动恢复报告 |
| `GET` | `/api/v1/state` | 单个只读事务内导出快照、推送、迁移历史、ACK、结果、意图等完整状态 |
| `GET` | `/api/v1/health` | 健康检查 |

触发推送示例：

```bash
curl -X POST http://127.0.0.1:18080/api/v1/pushes \
  -H 'Content-Type: application/json' \
  -d '{"version":1,"client_ids":["client-success"],"timeout_ms":250,"round_budget_ms":2000}'
```

推送响应既包含持久化的批次/推送状态，也包含目标客户端返回的 `ack.result`。为保持“推送 -> 确认 -> 结果上报”的独立 API 语义，客户端结果仍需显式提交到 `/api/v1/results`。实验驱动会自动完成这一步。

客户端注册示例：

```json
{
  "client_id": "client-success",
  "keys": ["weight_x", "weight_y", "bias", "normalization_ratio", "decision_threshold"],
  "endpoint": "http://127.0.0.1:18081/push?behavior=success",
  "pickup_policy": "LATEST"
}
```

也支持 `PINNED` + `pinned_version`。显式请求与固定策略不一致时返回 `E_PICKUP_POLICY_MISMATCH`。

## SQLite 与恢复

数据库启用 WAL、外键和 busy timeout。核心表：`snapshots`、`clients`、`batches`、`pushes`、`push_transitions`、`acks`、`ack_events`、`results`、`intents`、`recovery_reports`。

写路径规则：

1. 在单写者互斥区内用自动提交事务写入 `PREPARED` 意图。
2. 在下一事务中执行业务变更，并在同一事务中把意图标记为 `COMMITTED`。
3. 业务失败则回滚变更；意图标记为 `FAILED`。某些“错误但必须留痕”的场景（如 `E_STALE_ACK`）会随历史事件一起提交。

服务启动时执行恢复扫描，只使用已经落盘的行：

- 先以独立 `RECOVERY_SCAN` 意图进入写路径；扫描时排除本次恢复意图自身，避免把正在执行的恢复误报为遗留操作
- 扫描其他 `PREPARED` 意图、`OPEN/DISPATCHING` 批次，以及关联的 `PENDING/PUSHED/ACKED` 推送
- 把未完成推送置为 `ABORTED`，错误码 `E_RECOVERED_ABORT`
- 开放批次内所有推送都已 `APPLIED` 时恢复为 `COMPLETED`；其他未完成批次置为 `ABORTED`
- 把 `PREPARED` 意图置为 `RECOVERED`
- 计算每个客户端最后的 `APPLIED` 版本
- 把恢复动作、`recovery_reports` 和本次 `RECOVERY_SCAN` 意图的 `COMMITTED` 状态放在同一事务提交，包含快照版本、未完成意图、开放批次、动作和恢复后客户端版本

如果进程在恢复事务提交前再次被终止，业务变更是回滚的，`RECOVERY_SCAN` 意图仍为 `PREPARED`；下一次启动会把它作为已落盘事实扫描并标记为 `RECOVERED`。

实验同时验证两种终止点：

- 真实延迟推送处于 `PUSHED` 时直接杀 middleware 进程
- `MW_FAULT_CRASH=RESULT_APPLY_AFTER_INTENT` 时，结果意图已持久化，但进程在业务事务提交前以 137 退出

两种情况重启后，都不会留下 `ACKED` 与 `APPLIED` 混合或残留 `PREPARED` 意图。

## 一致性设计

- 配置提交、批次/推送创建、状态迁移、ACK、结果应用、恢复写入都经过同一写者互斥区。
- 查询使用 SQLite 只读事务；一次 `/state` 或 `/inconsistencies` 响应中的所有表来自同一个 WAL 快照。
- `/state` 直接返回 `push_transitions` 与最近的 `intents`，异常后的状态可以按状态迁移和意图状态逐条核对，而不只依赖推导值。
- ACK 记录、`PUSHED -> ACKED` 迁移和 ACK 历史在单个事务提交，查询不可能看到“ACK 已存在但推送仍 PENDING”的中间态。
- `ACKED -> APPLIED` 与结果行插入在单个事务提交。
- 版本门控和最新版本判断也在写事务内完成，避免应用旧结果时跨过新版本。

## 实际验证结果

最近一次本机执行：

```text
go test -race -count=1 ./...                     PASS
./scripts/run_experiments.sh                     all_passed=true
GOOS=windows GOARCH=amd64 go build ./...         PASS
```

本次实测运行在 Linux/WSL；Windows amd64 产物已完成交叉编译验证，PowerShell 入口和纯 Go SQLite 驱动不依赖 CGO 或 Docker。

关键观测：

| 场景 | 实际结果 |
| --- | --- |
| 推送超时 | 2 个 250ms 独立超时在 253ms 内结束；两端 `ABORTED/E_PUSH_TIMEOUT`；每条连接 opened/closed 均为 1/1；客户端模拟器记录 2 次 context cancellation；随后推送 3ms 获得名额并 `ACKED` |
| 连接拒绝 | 关闭的 loopback 端口得到 `ABORTED/E_PUSH_REFUSED`，批次 `COMPLETED_WITH_ERRORS` |
| 确认版本错位 | 客户端故意回传 v2 ACK 给 v1 推送，推送 `ABORTED/E_ACK_VERSION_MISMATCH`；仅新增 `REJECTED` ACK 历史，不生成当前 ACK |
| 重复推送/响应 | 同一 `(client,version)` 两次触发复用同一 push；客户端实际只收到 1 次；重复 ACK 响应被去重，持久化 1 条 ACK 且 `ingress_count=2`；订阅内容变化后返回 `E_CONFLICTING_PUSH` |
| 迟到确认/版本并发 | v2 已应用后回传 v1 ACK 返回 `E_STALE_ACK`，仅新增 `STALE` 历史；客户端当前状态未改变；并发 v1/v2 中 v1 被 `ABORTED/E_STALE_ACK`，v2 最终 `APPLIED` |
| 结果差异 | 篡改 normalized 的结果被判 `MISMATCH`，不改变推送状态；不一致报告含 `ack_id`、拾取版本、结果摘要以及 `normalized`/`result_digest` 差异字段 |
| 强制终止 | 推送中杀进程后，恢复报告扫描 1 个 PREPARED 意图并记录 push/batch abort；结果意图落盘后以 137 退出，重启扫描 `RESULT_APPLY` 并把推送恢复为 `ABORTED/E_RECOVERED_ABORT` |

本次实验数据库最终有 0 个 `PREPARED` 意图，4 个 `RECOVERY_SCAN` 意图全部为 `COMMITTED`；另外 `go test -race` 中的子进程用例会在恢复事务提交前退出 137，重新打开数据库后仍只看到此前事实，下一次恢复再把遗留的 `RECOVERY_SCAN` 收敛为 `RECOVERED`。

逐项断言、时间戳和异常前后状态见 `artifacts/experiment/summary.json`、`01-*.json` 至 `06-*.json`。

也可以直接核对本次实验数据库：

```bash
sqlite3 artifacts/experiment/run/middleware.db \
  "SELECT client_id,version,status,error_code FROM pushes ORDER BY version,client_id;"
sqlite3 artifacts/experiment/run/middleware.db \
  "SELECT intent_id,kind,status,aggregate_id FROM intents ORDER BY created_at;"
sqlite3 artifacts/experiment/run/middleware.db \
  "SELECT report_json FROM recovery_reports ORDER BY finished_at DESC LIMIT 1;"
```

## 手工启动

两个终端分别启动客户端模拟器和 middleware：

```bash
go run ./cmd/client-sim -listen 127.0.0.1:18081 -samples samples/samples.json
go run ./cmd/middleware -listen 127.0.0.1:18080 -db data/middleware.db -samples samples/samples.json
```

`client-sim` 的 `/push` 支持 `behavior=success|delay|duplicate|reject|malformed|wrong_version`；`delay` 可配 `delay_ms`，`success` 可配 `delay_version` 只延迟指定版本。`GET /stats` 返回每个 push 的接收、完成、取消和重复 ACK 次数。

重新计算样本或摘要：

```bash
go run ./cmd/configctl gen-samples -out samples/samples.json
go run ./cmd/configctl digest samples/snapshot.v1.json
go run ./cmd/configctl compute -snapshot samples/snapshot.v1.json -samples samples/samples.json
```

`samples/clients.json` 中的 `{PORT}` 和 `{REFUSED_PORT}` 是实验驱动替换的占位符；手工使用时需替换为实际端口。middleware 只接受 loopback 客户端地址，避免误访问未授权目标。
