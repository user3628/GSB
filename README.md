# CDN 边缘缓存一致性仿真与诊断工作台

这是一个只监听本地回环地址的 CDN 边缘缓存一致性实验台。单个 Go 进程同时扮演源站、四个可配置边缘节点、缓存条目存储、调度器和诊断 API，用于离线验证带版本缓存策略、TTL/SWR、Vary、回源合并、purge 传播、异常注入和强制终止恢复。

项目不依赖容器、真实 CDN 账号或线上站点。运行时只使用 `net/http`、`database/sql` 和纯 Go SQLite 驱动 `modernc.org/sqlite`；仓库已包含 `vendor/`，因此可以断网构建和测试。

## 快速开始

### Windows PowerShell

要求 Go 1.22 或更高版本。SQLite CLI 只用于人工检查数据库，不是运行依赖。

```powershell
Set-Location <仓库目录>
go version
.\scripts\verify.ps1
```

若本机执行策略限制脚本：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

启动工作台：

```powershell
.\scripts\run.ps1
# 或显式指定参数
.\scripts\run.ps1 -Config samples\config.json -Listen 127.0.0.1:18080 -Database .\data\cdnlab.db
```

也可以先构建再启动：

```powershell
go build -o cdnlab.exe .\cmd\cdnlab
.\cdnlab.exe serve --config .\samples\config.json --listen 127.0.0.1:18080
```

### Linux / WSL

```bash
go version
./scripts/verify.sh
./scripts/run.sh
```

所有相对路径都相对配置文件所在目录解析。默认配置的数据库位于 `data/cdnlab.db`，恢复报告位于 `data/recovery-report.json`。

### 离线构建

```bash
GOPROXY=off go test ./...
GOPROXY=off go build ./...
GOOS=windows GOARCH=amd64 GOPROXY=off go build ./...
```

Go 1.22 在存在一致 `vendor/modules.txt` 时自动使用 vendor。必要时可显式加 `-mod=vendor`。

## 样本与配置

`samples/config.json` 定义源站根目录、SQLite 路径、策略初始版本、回源/重验证超时和四个节点：

| 节点 | 源站标识 | 调度优先级 |
| --- | --- | ---: |
| `edge-a` | `origin-main` | 40 |
| `edge-b` | `origin-main` | 30 |
| `edge-c` | `origin-main` | 20 |
| `edge-d` | `origin-main` | 10 |

样本策略：

| 快照 | 版本 | TTL | SWR | purge 窗口 | 全局/单源站并发 | 队列 | 轮预算 | 校验摘要 |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| `policy-v1.json` | 1 | 2000 ms | 1000 ms | 3000 ms | 2 / 1 | 32 | 20 ms | `ac81bc70c3db94da9dff88ae2e3d8db7d60a9b369d4ce9b3584f311dff8228d7` |
| `policy-v2.json` | 2 | 5000 ms | 2500 ms | 4000 ms | 3 / 1 | 64 | 30 ms | `3d9467139ee4071061e2fa43859df95928ce10367011aff36dd8eb9f70374b8a` |

检查或重写策略校验摘要：

```powershell
go run .\cmd\cdnlab checksum --config .\samples\config.json --file .\samples\policies\policy-v1.json
go run .\cmd\cdnlab checksum --config .\samples\config.json --file .\samples\policies\policy-v1.json -write
```

样本源站内容位于 `samples/origin/`，标签映射位于 `samples/origin/tags.json`，异常参数清单位于 `samples/faults/catalog.json`。改写源站内容应使用原子替换命令：

```powershell
go run .\cmd\cdnlab publish --config .\samples\config.json `
  --path /static/app.js --body 'console.log("app-v2");'
```

## 代码结构

| 路径 | 职责 |
| --- | --- |
| `cmd/cdnlab` | `serve`、`checksum`、`publish` 命令行入口 |
| `internal/cache` | 路径/查询/Vary 规范化，缓存键模型，TTL 判定，状态机 |
| `internal/policy` | 策略规范化、校验、SHA-256 校验摘要和默认值 |
| `internal/store` | SQLite 模式、单写者队列、意图、purge 记录、恢复报告 |
| `internal/scheduler` | 全局/单源站并发、有界等待队列、确定性轮预算调度 |
| `internal/engine` | 边缘请求、回源合并、版本复制、purge、故障注入、轨迹 |
| `internal/httpapi` | 源站、边缘、管理 API 与 JSON 错误封装 |
| `samples/` | 确定性节点、策略、内容和故障样本 |
| `scripts/` | Windows/Linux 启动与端到端验证脚本 |

SQLite 的主要表包括 `cache_entries`、`entry_tags`、`route_map`、`key_epochs`、`entry_transitions`、`origin_objects`、`origin_attempts`、`request_records`、`purge_records`、`purge_deliveries`、`intents`、`audit_events` 和 `recovery_reports`。数据库启用 WAL；HTTP 读取使用独立连接池，所有写入经单一写者 goroutine 和单连接串行提交。

## 缓存键与条目

缓存键的规范输入为：

1. 规范化路径：折叠重复 `/`、合并 `.` 和 `..`、统一百分号转义大小写，并解码非保留字符的百分号转义；保留字符（例如 `%2F`）保持转义，避免编码斜杠变成路径分隔符。
2. 规范化查询串：解码组件，按 `key`、`value` 排序（策略允许时）并以大写 `%XX` 重新编码；空查询与无查询等价。
3. 策略声明的 `vary` 头：名称小写、去重、排序；多值按逗号拆分、去除首尾空白后重新连接。
4. 以上内容序列化为版本化规范文本，再做 SHA-256，得到 `cache_key`。

`/static//./app.js?b=2&a=1` 与 `/static/app.js?a=1&b=2` 生成同一键。`Accept-Language: en` 与 `Accept-Language: zh-CN` 形成不同 Vary 键。无法解析的百分号、未转义控制字符、空格或非法查询转义返回 `E_KEY_INVALID`。

URL 中的 `__fault`、`__fault_node`、`__delay_ms`、`__status`、`__corrupt`、`__corrupt_bytes`、`__request_id` 是控制参数，规范化缓存键前会被移除，因此注入异常不会污染正常缓存键。HTTP 路由使用自定义分发器而非 `http.ServeMux`，避免标准库提前重写重复斜杠和点段；用 curl 验证原始形态时可加 `--path-as-is`。

每个 `(node_id, cache_key)` 条目保存规范化路径/查询、Vary 值、ETag、响应体、内容类型、标签、状态、快照版本、写入时刻、fresh/stale 截止时刻和最近请求/意图标识。源站 ETag 是响应体的 `sha256-...` 内容摘要；任一边缘收到响应后会重新计算摘要，若不匹配则拒绝写入。

## 状态机与 TTL

允许的缓存状态迁移如下；同状态提交是幂等无操作：

| From | 合法 To |
| --- | --- |
| `MISS` | `REVALIDATING`, `PURGED` |
| `REVALIDATING` | `FRESH`, `STALE`, `MISS`, `PURGED` |
| `FRESH` | `STALE`, `REVALIDATING`, `PURGED` |
| `STALE` | `FRESH`, `REVALIDATING`, `PURGED` |
| `PURGED` | `MISS` |

其他迁移返回 `E_INVALID_TRANSITION`。状态检查、迁移记录和条目更新在同一事务中完成，失败不会留下半更新状态。

TTL 使用毫秒整数：

```text
fresh_until_ms = written_at_ms + ttl_ms
stale_until_ms = fresh_until_ms + swr_ms
```

- `now < fresh_until_ms`：`FRESH`，直接命中。
- `fresh_until_ms <= now <= stale_until_ms` 且 `swr_ms > 0`：`STALE-SWR`，返回旧内容并触发一次异步重验证。
- `now > stale_until_ms`：旧内容不可返回，进入同步重验证。
- SWR 边界是闭区间；在恰好等于 `stale_until_ms` 时仍允许返回 stale，下一毫秒即过期。`swr_ms = 0` 时不存在 stale 可用窗口。

## 源站版本、回源合并与快照

回源合并键为 `origin:<policy-version>:<cache-key>`。同一快照下多个节点同时未命中时，只有一个任务真正回源，其余节点等待并复用同一个 `flightOutcome`。等待超过 `revalidate_timeout_ms` 返回 `E_REVALIDATE_TIMEOUT`；单个等待者超时只放弃自己的等待，不取消共享回源任务。

回源成功后，一个写事务会为快照覆盖的所有节点写入相同响应体和 ETag，并统一进入 `FRESH`。因此调度顺序不会造成“部分节点已更新、部分仍持有旧版本”的最终状态。版本未变化的并发等待者同样复用该次结果，不重复回源；已经 `FRESH` 的条目直接命中。purge 与回源竞争时使用缓存键 epoch：purge 先提交会使旧回源结果得到 `E_VERSION_RETIRED`，不会复活已失效内容。

策略快照包含单调递增版本、RFC3339 生成时间、节点集合、TTL/SWR 窗口、purge 窗口、Vary、键规则和调度参数。提交时：

- 版本必须大于当前已提交版本，否则返回 `E_STALE_SNAPSHOT`。
- 节点、窗口、并发上限和队列参数先整体校验；失败返回 `E_SNAPSHOT_INVALID` 或 `E_SNAPSHOT_CHECKSUM`，旧策略保持可用。
- 校验摘要覆盖除 `checksum` 和 `committed_at_ms` 外的规范化快照；提交时重新计算并原子替换当前策略。
- 一次请求在开始时绑定策略版本，并在响应头返回 `X-Snapshot-Version`；期间替换策略不会切换该请求的语义。可通过 `X-Policy-Version` 显式绑定保留版本，不存在时返回 `E_VERSION_RETIRED`。

版本复制在单个 SQLite 写事务内同步完成，不采用跨窗口的后台节点轮询。一次重验证从进入调度开始受 `revalidate_window_ms` 截止时间约束；若排队、回源或提交前已超过窗口，则取消任务、返回 `E_REVALIDATE_TIMEOUT`，不会提交迟到的版本。样本中的本地回源超时小于该窗口。`purge_window_ms` 是 purge 广播的硬截止时间。

## 调度与预算

回源任务和 purge 广播共享同一个确定性调度器，同时受以下限制：

- 全局回源并发上限 `global_origin_concurrency`。
- 每源站并发上限 `per_origin_concurrency`。
- 有界等待队列 `queue_capacity`；队列满返回 `E_ORIGIN_QUEUE_FULL`。
- 每轮虚拟预算 `round_budget_ms`，每个任务按 `default_task_cost_ms` 计费。

可调度任务按以下稳定顺序选择：

```text
priority desc, heat desc, key asc, node asc, kind asc, sequence asc
```

`priority` 来自节点配置，`heat` 来自该缓存键被请求的次数。预算不足时，超出预算的任务产生 `E_BUDGET_EXHAUSTED`，错误详情和 `/admin/scheduler` 都包含稳定的跳过顺序与 `skipped` 清单。轮次在队列和活动任务都清空时结束，报告记录已启动、已跳过、完成和失败任务。预算实现为确定性的虚拟任务成本，而不是对 goroutine 的硬 CPU 抢占，便于重复回放。

## Purge 传播

`POST /admin/purge` 支持按规范化路径或标签失效。流程为两阶段：

1. 在一个写事务中记录 undo 意图、purge 会话和每个节点的 delivery 记录。
2. 将各节点 delivery 任务提交到调度器，在 `purge_window_ms` 截止前完成。只要任一节点被注入中断、排队失败或超时，就返回 `E_PURGE_INCOMPLETE` 和 `incomplete_nodes`，所有条目保持原状态。
3. 全部 delivery 完成后，唯一改变可见状态的提交事务一次性把所有适用节点条目迁为 `PURGED`，级联失效 `entry_tags` 和 `route_map`，递增相关缓存键 epoch，并提交 purge 记录。

因此不存在“部分节点已经 purge、其余节点仍返回旧内容”的中间可见状态。相同 `request_id` 的 purge 是幂等的；已中止的 purge 重放会继续返回同一 `E_PURGE_INCOMPLETE` 事实。

示例：

```bash
curl -X POST http://127.0.0.1:18080/admin/purge \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: purge-demo' \
  -d '{"kind":"path","path":"/static/app.js"}'

curl -X POST http://127.0.0.1:18080/admin/purge \
  -H 'Content-Type: application/json' \
  -d '{"kind":"tag","tag":"static"}'
```

## 异常注入与轨迹

边缘请求支持以下确定性控制参数，也可用对应的 `X-Lab-*` 请求头：

| 参数 | 行为 | 业务码 |
| --- | --- | --- |
| `__fault=origin_timeout&__delay_ms=2000` | 本地源站真实延迟，客户端上下文超时后关闭连接 | `E_ORIGIN_TIMEOUT` |
| `__fault=origin_delay&__delay_ms=...` | 在真实本地 HTTP 端点制造延迟，可由回源超时截断 | `E_ORIGIN_TIMEOUT` 或成功 |
| `__fault=origin_refused` | 连接一个已保留后关闭的本地回环端口，表现为真实 TCP 拒绝 | `E_ORIGIN_REFUSED` |
| `__fault=origin_status&__status=503` | 本地源站返回指定 HTTP 状态 | `E_ORIGIN_STATUS` |
| `__fault=body_corrupt&__corrupt=truncate&__corrupt_bytes=4` | 源站 ETag 不变但响应体被截短 | `E_BODY_CORRUPT` |
| `__fault=body_corrupt&__corrupt=grow&__corrupt_bytes=8` | 源站 ETag 不变但响应体被扩展 | `E_BODY_CORRUPT` |
| purge 请求体 `interrupt_nodes` | 指定节点的广播被中断，整体不提交 | `E_PURGE_INCOMPLETE` |

`__fault_node` 可限定故障只作用于某节点。超时请求使用请求上下文控制真实连接，超时后取消并释放 HTTP 资源。

每个请求都会持久化轨迹，包含绑定快照、规范化路径/查询、Vary、缓存键、判定、缓存状态、内容版本、是否合并、回源次数、状态步骤和业务码。`X-Trace-Hash` 对判定相关字段做规范化 SHA-256，不包含墙钟时间，因此相同状态下的相同注入可逐条比较；相同请求标识的重复提交直接复用持久化响应并带 `X-Idempotent-Replay: true`。

## HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 健康检查 |
| `GET /origin/{path}` | 本地源站；返回 ETag、内容版本、标签和长度 |
| `GET /edge/{node}/{path}` | 经过指定边缘节点的缓存请求 |
| `POST /admin/policies` | 原子提交策略快照 |
| `GET /admin/policies` | 列出策略版本 |
| `GET /admin/policies/current` | 当前策略 |
| `GET /admin/policies/{version}` | 指定策略版本 |
| `POST /admin/purge` | 路径或标签 purge |
| `GET /admin/purges` | purge 列表 |
| `GET /admin/purges/{id}` | purge 详情与逐节点 delivery |
| `GET /admin/entries` | 条目查询；支持 `node`、`path`、`key`、`state`、`tag`、`limit` |
| `GET /admin/traces` | 最近轨迹；支持 `limit` |
| `GET /admin/traces/{request_id}` | 单请求完整轨迹 |
| `GET /admin/metrics` | 持久化与运行时计数、队列深度、活动任务和合并数 |
| `GET /admin/scheduler` | 最近一轮调度报告 |
| `GET /admin/recovery` | 最近一次启动恢复报告 |
| `POST /admin/debug/crash` | 仅在 `allow_debug_crash=true` 时启用，用于强制终止恢复验证 |

典型请求：

```bash
BASE=http://127.0.0.1:18080

# 首次未命中；成功后同键写入所有策略节点
curl -sS -D - "$BASE/edge/edge-a/static/app.js?a=1&b=2"

# 等价 URL 形态应命中同一缓存键
curl -sS -D - --path-as-is "$BASE/edge/edge-b/static//./app.js?b=2&a=1"

# Vary 形成独立条目
curl -sS -D - -H 'Accept-Language: en' "$BASE/edge/edge-c/static/app.js?a=1&b=2"

# 指定请求标识并读取轨迹
curl -sS -D - -H 'X-Request-ID: demo-1' "$BASE/edge/edge-a/static/app.js"
curl -sS "$BASE/admin/traces/demo-1"

# 提交样本 v2 策略
curl -sS -X POST "$BASE/admin/policies" \
  -H 'Content-Type: application/json' \
  -H 'X-Request-ID: policy-v2' \
  --data-binary @samples/policies/policy-v2.json
```

边缘响应关键头：

- `X-Cache-Status`: `MISS`、`HIT`、`STALE-SWR` 或 `COALESCED`。
- `X-Cache-State`: `FRESH`、`STALE`、`REVALIDATING`、`PURGED` 等状态。
- `X-Content-Version` / `ETag`: 内容摘要版本。
- `X-Snapshot-Version`: 本次请求绑定的策略版本。
- `X-Cache-Key`: 规范化后的 SHA-256 缓存键。
- `X-Coalesced`: 是否复用了其他节点的共享回源结果。
- `Warning: 110`: SWR 返回旧内容。

所有错误均为 JSON：

```json
{
  "code": "E_PURGE_INCOMPLETE",
  "message": "purge propagation did not complete; no node was purged",
  "details": {"incomplete_nodes": ["edge-b"], "affected_entries": 0},
  "request_id": "purge-demo"
}
```

## 单写者、幂等与强制终止恢复

所有条目写入、状态迁移、索引更新和 purge 提交都排队进入唯一写者 goroutine；读请求直接使用 WAL 读连接池并发执行。写事务前的 `intents` 记录保存完整 undo 行；请求标识在主键上只允许一次 claim，完成后重放已持久化响应。

验证强制终止恢复：

```bash
curl -sS -X POST -H 'X-Request-ID: crash-demo' \
  'http://127.0.0.1:18080/admin/debug/crash?node=edge-a&path=/static/app.js' || true
# 进程以退出码 90 终止且不返回正常响应；重启服务后：
curl -sS http://127.0.0.1:18080/admin/recovery
curl -sS 'http://127.0.0.1:18080/admin/entries?path=/static/app.js'
```

重启时，`Store.Recover` 在单写事务中扫描 `pending` 意图，按 undo 恢复条目和索引，将未完成 purge 标为 `aborted/E_PURGE_INCOMPLETE`，把崩溃时仍为 `inflight` 的请求标为 `E_REQUEST_ABORTED`，并生成 `facts_only: true` 的恢复报告。报告中的业务码只汇总 `request_records`、`purge_records`、`intents`、`audit_events` 和 `origin_attempts` 中已经持久化的事实，不推测未落盘状态。

可选 SQLite 检查：

```bash
sqlite3 data/cdnlab.db '.tables'
sqlite3 data/cdnlab.db \
  'select node_id,state,etag,written_at_ms from cache_entries order by node_id;'
sqlite3 data/cdnlab.db \
  'select purge_id,status,error_code,affected_entries from purge_records;'
```

## 业务码

| 业务码 | HTTP | 含义 |
| --- | ---: | --- |
| `E_KEY_INVALID` | 400 | 路径、查询或控制参数无法规范化 |
| `E_INVALID_TRANSITION` | 409 | 非法缓存状态迁移，原状态不变 |
| `E_STALE_SNAPSHOT` | 409 | 策略版本不递增 |
| `E_SNAPSHOT_INVALID` | 400 | 快照字段或节点引用非法 |
| `E_SNAPSHOT_CHECKSUM` | 400 | 快照校验摘要不匹配 |
| `E_VERSION_RETIRED` | 409 | 绑定版本不存在，或回源结果已被 purge 淘汰 |
| `E_PURGE_INCOMPLETE` | 409 | 传播窗口内未完成，附未完成节点 |
| `E_PURGE_ABORTED` | 409 | purge 已中止 |
| `E_ORIGIN_TIMEOUT` | 504 | 真实本地回源连接超时 |
| `E_ORIGIN_REFUSED` | 502 | 真实本地回环连接被拒绝 |
| `E_BODY_CORRUPT` | 502 | 响应体摘要与内容版本不一致 |
| `E_ORIGIN_STATUS` | 502 | 源站返回非 200 |
| `E_REVALIDATE_TIMEOUT` | 504 | 等待合并回源超过预算，或版本传播超过策略窗口 |
| `E_ORIGIN_QUEUE_FULL` | 429 | 有界调度队列已满 |
| `E_BUDGET_EXHAUSTED` | 503 | 轮预算不足或调度器停止 |
| `E_NODE_UNKNOWN` | 404 | 节点不在配置或绑定快照中 |
| `E_NOT_FOUND` | 404 | 对象、轨迹或 purge 记录不存在 |
| `E_BAD_REQUEST` | 400 | 请求结构或参数非法 |
| `E_REQUEST_ABORTED` | 409 | 请求在崩溃恢复时被中止 |
| `E_INTERNAL` | 500 | 未分类内部错误 |

## 实际验证结果

验证日期为 2026-11-24。Linux 侧环境为 Go 1.27.4 / linux-amd64；Windows 侧使用 Windows PowerShell 5.1.26100.9818 和 Windows amd64 二进制。

- `gofmt -w ./cmd ./internal`：无待格式化文件，`git diff --check` 通过。
- `go vet ./...`：通过。
- `GOPROXY=off go test ./... -count=1`：26 个顶层测试和 6 个子测试全部通过；覆盖多节点命中/Vary/规范化、跨节点回源合并、SWR 与版本传播、版本传播窗口、原子 purge、快照绑定、四类源站故障与幂等重放、强制崩溃恢复、合并等待超时、队列满和预算跳过。
- `GOPROXY=off go test -race ./... -count=1`：通过，未检测到数据竞争。
- `GOPROXY=off go build ./...`：通过。
- `GOOS=windows GOARCH=amd64 GOPROXY=off go build ./...`：通过。
- 交叉编译的 5 个 Windows amd64 测试二进制在 Windows PowerShell 5.1 中执行，全部 `PASS`。
- `scripts/verify.sh`：输出 `VERIFY_OK`。
- Windows PowerShell `scripts/verify.ps1 -Binary <windows-amd64-cdnlab.exe>`：输出 `VERIFY_OK`。

端到端样本观察值：

- 四节点并发未命中同一键：源站成功获取 1 次，其余节点复用；响应内容版本一致。
- v1 内容 ETag：`sha256-51addb55da58103994732001ae44228a8145675c685304214ece5c4af740cca9`。
- 等价 URL 形态得到同一 `X-Cache-Key`；不同 Vary 得到不同条目。
- 注入 `edge-b` purge 中断返回 HTTP 409 / `E_PURGE_INCOMPLETE`，`incomplete_nodes` 为 `["edge-b"]`，所有节点条目保持 `FRESH`。
- 注入超时返回 HTTP 504 / `E_ORIGIN_TIMEOUT`；连接拒绝返回 `E_ORIGIN_REFUSED`；截短/扩长响应体返回 `E_BODY_CORRUPT`。
- 崩溃恢复报告为 `facts_only: true`、`scanned_intents: 1`、`rolled_back_rows: 4`，四个节点均恢复为原来的 `FRESH` 版本；报告中包含持久化事实 `E_PURGE_INCOMPLETE`、`E_ORIGIN_TIMEOUT` 和 `E_STALE_SNAPSHOT`。

## 运行约束

- 服务默认只绑定 `127.0.0.1`，源站和所有边缘节点在同一进程的本地 HTTP 服务中。
- purge 的逐节点传播使用共享确定性调度器模拟；最终可见失效由单事务原子提交。
- 轮预算是离线可重复的虚拟任务成本；真实超时、连接拒绝、HTTP 状态和响应体摘要校验均由本地端点实际执行。
- 全部验证只访问本机回环地址，不包含容器、外部网络依赖或真实 CDN 账号。
