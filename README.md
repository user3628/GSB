# CDN 边缘缓存一致性仿真验证与诊断工作台

这是一个只使用本机回环 HTTP、本地 SQLite 和固定样本的 CDN 边缘缓存一致性验证工作台。一个 FastAPI/uvicorn 进程同时提供源站角色、多个边缘节点角色、管理 API 和诊断 API；不会访问真实 CDN、外部站点或未授权目标。

## 已实现能力

- 规范化缓存键：路径去重斜杠、合并点段、百分号编码统一、查询参数排序、Vary 请求头取值分离。
- 缓存状态机：`MISS -> REVALIDATING -> FRESH -> STALE -> PURGED`，非法迁移返回 `E_INVALID_TRANSITION`。
- TTL 与 SWR：命中时递减剩余 TTL，返回 `Age`、`Cache-Control`、`X-Cache-Status`；SWR 窗口内返回陈旧内容并启动解耦的后台回源。
- 回源合并：同一节点同一缓存键的并发未命中只发起一次真实回源，其他请求等待并复用结果。
- 内容版本识别：源站返回 ETag 和内容 SHA-256；版本变化时删除旧条目及其派生索引，再插入完整新条目。
- 策略快照：版本号、生成时间、节点集合、SHA-256 摘要；新快照整体替换，旧版本返回 `E_STALE_SNAPSHOT`。
- 原子 purge：路径或标签 purge 在一个 SQLite 事务内更新所有适用节点，并级联删除路由索引与标签索引。
- 异常注入：真实延迟与客户端超时断开、源站 5xx、响应体长度变化、真实连接拒绝、purge 广播中断。
- 精确一次请求：`request_id` 持久化去重；重复请求复用已存结果，不再次执行缓存逻辑。
- 崩溃恢复：写入和 purge 先持久化意图；强制终止后重启扫描未完成意图，按持久化事实输出恢复报告。
- 可核验诊断：请求轨迹、条目状态演进、异常记录、源站回源尝试、恢复报告和节点状态摘要。

## 目录

```text
config/                 本地服务配置（端口、SQLite、节点、超时）
samples/                两份策略快照、请求清单、源站初版/替换版内容
workbench/              FastAPI 服务、缓存引擎、SQLite、规范化、恢复
scripts/demo.py         一键可重复演示
scripts/setup_windows.ps1 / run_demo.ps1
tests/                  单元测试与真实回环 HTTP 集成测试
artifacts/              演示输出、完整轨迹、服务日志
data/                   运行期 SQLite（启动时创建）
```

## Windows 本地准备

要求 `python --version` 为 3.11 或以上，且 `pip --version` 可用。

```powershell
cd <工作区>
.\scripts\setup_windows.ps1
.\scripts\run_demo.ps1
```

如依赖来自离线 wheel 目录：

```powershell
.\scripts\setup_windows.ps1 -WheelDir C:\offline-wheels
```

等价的显式命令：

```powershell
python --version
pip --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pytest
python scripts\demo.py
```

Linux/macOS 可使用 `scripts/setup.sh` 和 `scripts/run_demo.sh`。代码本身不含平台专属路径。

## 本地启动

端口、SQLite 路径、边缘节点和回源超时都在 `config/default.json` 或 `config/demo.json` 中。默认只绑定 `127.0.0.1`。

```powershell
python -m workbench.server --config config\default.json
# 可选覆盖
python -m workbench.server --config config\default.json --port 8901 --sqlite data\run.db --reset
```

启动后：

- `GET /api/health`：配置、SQLite 可写性和就绪状态。
- `GET /api/diagnostics/baseline`：本次干净启动基线摘要。
- `GET /docs`：自动生成的 OpenAPI 页。

工作台不会自动提交策略；演示脚本会先提交 `samples/policy_v1.json`。源站内容在数据库为空时由 `samples/origin_content_v1.json` 初始化。

## 缓存键规则

缓存键由以下部分构成，并计算 SHA-256：

1. 规范化路径：以 `/` 开头、折叠重复斜杠、合并 `.`/`..`、解码非保留字符的百分号形式、其余百分号十六进制统一大写、非 ASCII 字符按 UTF-8 百分号编码。
2. 规范化查询串：保留重复键，按名称和值排序，规范化百分号编码；`+` 按表单空格处理并规范为 `%20`。
3. 当前策略声明的 Vary 请求头，头名大小写不敏感，缺失值规范为空字符串。

因此 `//assets/./app.js?v=1&lang=zh-CN` 与 `/assets/app.js?lang=zh-CN&v=1` 使用同一缓存键；不同 `Accept-Language` 会分离存储。非法转义、控制字符、反斜杠等返回 `E_KEY_INVALID`，不会写缓存。

## 缓存状态机

```text
MISS ----------> REVALIDATING ----> FRESH ----> STALE ----> REVALIDATING
                                      |          |
                                      |          +--------> PURGED
                                      +-------------------> PURGED
STALE ----------------------------------------------------> PURGED
REVALIDATING --------------------------------------------> PURGED
PURGED  (当前条目世代终态，后续回源会创建新 generation)
```

接口 `GET /api/state-machine` 返回机器可读迁移表。`POST /api/edges/{node}/entries/{cache_key}/transition` 可用于诊断验证；非法迁移返回 `E_INVALID_TRANSITION`，条目和派生索引不变。

## 策略快照

`POST /api/policies`（别名 `/api/policies/snapshots`）接受：

```json
{
  "version": 1,
  "generated_at": "2026-11-20T00:00:00+00:00",
  "nodes": ["edge-a", "edge-b", "edge-c"],
  "ttl_seconds": 20,
  "stale_while_revalidate_seconds": 10,
  "cache_control": "public, max-age=20, stale-while-revalidate=10",
  "vary": ["Accept-Language"],
  "purge": {"strategy": "atomic", "propagation_window_ms": 1000},
  "canonicalization": {
    "collapse_duplicate_slashes": true,
    "merge_dot_segments": true,
    "normalize_query": true,
    "uppercase_percent_encoding": true
  },
  "default_tags": ["policy-v1"],
  "digest": "<可选 SHA-256；缺省时服务计算>"
}
```

- `version <= 当前版本`：`E_STALE_SNAPSHOT`。
- 校验失败、摘要不符、未知节点、非法 Vary：`E_SNAPSHOT_INVALID`，现有策略不变。
- 新快照提交时，旧条目被标为 `PURGED`，索引删除，节点 epoch 增加；不会混用旧 TTL、Vary 或 purge 窗口。

## 请求 API

### 单请求

`POST /api/edges/{node_id}/requests`

```json
{
  "request_id": "req-fixed-001",
  "method": "GET",
  "path": "//assets/./app.js",
  "query": {"v": "1", "lang": "zh-CN"},
  "headers": {"Accept-Language": "zh-CN"},
  "force_revalidate": false,
  "injections": []
}
```

响应包含 `cache_key`、`key_canonical`、`vary`、`cache_status`、`origin_fetches`、`Age`/`Cache-Control` 响应头、内容、ETag、generation，以及完整 `trace.events`。

### 并发批量

`POST /api/edges/{node_id}/requests/batch`（别名 `/batch`）

```json
{"requests": [{"request_id": "r1", "path": "/api/catalog.json"}, {"request_id": "r2", "path": "/api/catalog.json"}]}
```

批量项并发执行，可用于验证 single-flight：同一节点同一缓存键通常只有一项 `origin_fetches=1`，其余为 0。

通用回放入口 `POST /api/replay` 可接受包含 `node_id` 的请求数组，按各自节点并发执行。

### 直接 GET

`GET /edge/{node_id}/{path}?普通查询参数` 返回真实内容响应头和内容，并在 `X-Cache-Key`、`X-Cache-Status`、`X-Origin-Fetches`、`X-Trace-Id` 中暴露诊断信息。

### 精确一次

同一 `request_id` 重复提交：

- 已完成：直接复用持久化响应，`replayed=true`、`origin_fetches=0`。
- 正在执行：等待原执行结果。
- 强制终止遗留：`E_REQUEST_INCOMPLETE`，不会再次执行。
- 同 ID 不同请求内容：`E_REQUEST_CONFLICT`。

## 异常注入

可在请求 JSON 的 `injections` 数组中使用：

| 类型 | 参数 | 行为 |
| --- | --- | --- |
| `origin_delay` | `delay_ms`, `timeout_ms` | 源站真实 sleep；边缘 HTTP 客户端超时并关闭连接，返回 `E_ORIGIN_TIMEOUT` |
| `origin_status` | `status` (5xx) | 源站真实返回指定 5xx，返回 `E_ORIGIN_5XX` |
| `body_length` | `pad_bytes` | 源站追加确定性字节，ETag/内容版本变化，用于验证整体替换 |
| `connection_refused` | 无 | 请求本机保留的拒绝端口，返回 `E_ORIGIN_CONNECT` |
| `purge_broadcast_interrupt` | `target_node` | purge 提交前中断，返回 `E_PURGE_BROADCAST_FAIL`，所有节点回滚 |

也可用保留查询参数：

```text
__sim_request_id
__sim_revalidate=true
__sim_inject_delay_ms=500&__sim_inject_timeout_ms=80
__sim_inject_origin_status=503
__sim_inject_body_pad=64
__sim_inject_connection_refused=true
__sim_inject_purge_fail=true
__sim_inject_target_node=edge-a
```

保留参数会从缓存键查询串中移除。未知或冲突的注入返回 `E_INJECT_INVALID` 并记录到 `/api/injections`。

## Purge

`POST /api/purges`（别名 `/api/purge`）

```json
{"request_id": "purge-1", "path": "/assets/app.js"}
// 或
{"request_id": "purge-2", "tag": "app"}
```

当前策略适用节点上的所有受影响条目、路由索引和标签索引在一个事务中更新。编辑完成后才可见，查询不会看到部分节点清除的中间态。超过策略传播窗口同样返回 `E_PURGE_BROADCAST_FAIL` 并回滚。

## 诊断 API

| 方法/路径 | 用途 |
| --- | --- |
| `GET /api/diagnostics/state` | 策略、节点条目状态、索引数、回源次数、意图和后台任务摘要 |
| `GET /api/diagnostics/baseline` | 启动基线 |
| `GET /api/policies/current` | 当前策略 |
| `GET /api/policies/snapshots` | 已提交版本 |
| `GET /api/edges/{node}/entries` | 条目和状态，可用 `cache_key`/`path`/`include_purged` 过滤 |
| `GET /api/entries/{cache_key}` | 跨节点查询同一缓存键 |
| `GET /api/edges/{node}/entries/{key}/events` | 条目持久化状态演进 |
| `GET /api/traces?request_id=...` | 判定轨迹 |
| `GET /api/requests/{request_id}` | 请求记录与全部轨迹 |
| `GET /api/injections` | 注入记录 |
| `GET /api/origin/fetches` | 真实源站回源尝试 |
| `GET /api/purges` | purge 提交/回滚记录 |
| `GET /api/background/{task_id}?wait=true&timeout=5` | 等待或查看后台回源 |
| `GET /api/recovery/report` | 最近一次启动恢复报告 |
| `GET /api/recovery/reports` | 历史启动报告 |
| `GET /api/origin/content` | 本地源站内容版本 |
| `POST /api/origin/content` | 脚本改写本地源站内容 |
| `POST /api/origin/load-manifest` | 装载内容清单 |

错误统一为：

```json
{"ok": false, "business_code": "E_ORIGIN_TIMEOUT", "message": "...", "details": {}}
```

主要业务码：`E_KEY_INVALID`、`E_INVALID_TRANSITION`、`E_STALE_SNAPSHOT`、`E_SNAPSHOT_INVALID`、`E_NODE_NOT_FOUND`、`E_NODE_NOT_COVERED`、`E_ORIGIN_TIMEOUT`、`E_ORIGIN_5XX`、`E_ORIGIN_CONNECT`、`E_INJECT_INVALID`、`E_PURGE_BROADCAST_FAIL`、`E_REQUEST_CONFLICT`、`E_REQUEST_INCOMPLETE`。

## 持久化与强制终止恢复

SQLite 使用 WAL、`synchronous=FULL` 和单写者事务。缓存写入、状态迁移和 purge 都在写锁和 `BEGIN IMMEDIATE` 事务中提交；读请求使用独立连接。

- 回源：先写 `intents(ORIGIN_FETCH, IN_PROGRESS)`，再发起真实 HTTP；旧条目先迁移到 `REVALIDATING`。提交内容与意图 `COMMITTED` 在同一事务。
- purge：先写 `PURGE_BROADCAST` 意图，再在一个事务内更新所有节点、索引、purge 记录和意图状态。
- 重启：扫描未完成意图并标记回滚；未完成请求标记 `INCOMPLETE`；遗留 `REVALIDATING` 条目按持久化 TTL 恢复为 `FRESH` 或 `STALE`；未完成回源尝试标记 `INTERRUPTED`。
- 报告只读取 SQLite 已持久化行，包含恢复前后事实快照。被强制终止的进程**不会**生成退出报告；报告由下一次启动生成。服务也不承诺终止时输出完整报告。

## 演示脚本

```powershell
.\scripts\run_demo.ps1
```

演示使用固定本地样本依次完成：

1. 检查 SQLite 可写性，记录干净启动基线。
2. 提交 v1 策略，执行冷启动请求回放，全部 MISS 并各自触发一次源站回源。
3. 改写源站 `app.js` 为 v2，强制重验证；输出 generation、旧/新 ETag 和 `version_changed=true`。
4. 先注入 purge 广播中断并验证回滚，再执行标签 purge；输出各节点受影响条目和索引失效结果。
5. 注入真实超时、5xx、连接拒绝和非法注入，输出不同业务码。
6. 建立稳定条目；启动一个 30 秒延迟回源请求，并等待其持久化意图；用 `process.kill()` 强制终止进程。
7. 使用同一 SQLite 重启，读取启动恢复报告，比较终止前与恢复后的条目版本，确认未完成意图回滚且最后一个完整版本保留。

输出：

- `artifacts/demo-output.json`：完整场景、请求响应、轨迹、状态摘要和恢复差异。
- `artifacts/demo-traces.json`：轨迹专用副本。
- `artifacts/demo-summary.txt`：人工核验摘要。
- `artifacts/demo-server.log`：本地服务日志。

## 实际验证结果

本次在当前执行环境使用 Python 3.14.4、FastAPI 0.165.0、uvicorn 0.54.0、httpx 0.28.1 完成验证；代码要求为 Python 3.11+，并提供 Windows PowerShell 启动脚本。

```text
pytest: 23 passed
demo cold start: 7 requests, all MISS, one origin fetch each
demo replacement: generation 1 -> 2, version_changed=true
demo purge: 4 affected entries across edge-a/edge-b/edge-c, route/tag indexes removed for affected keys
demo exceptions: E_ORIGIN_TIMEOUT, E_ORIGIN_5XX, E_ORIGIN_CONNECT, E_INJECT_INVALID, E_PURGE_BROADCAST_FAIL
demo forced termination: 1 in-progress origin intent rolled back
demo recovery: interrupted_origin_fetches=1, cache changed=false, last complete ETags/generations retained
```

## 范围与限制

- 仅支持本机回环地址和本地确定性样本；没有外部网络探测、真实 CDN 调用或容器依赖。
- 为保证单写者语义，服务必须以一个 uvicorn worker 运行；不要在多个 worker 间共享该 SQLite 工作台。
- 工作台模拟缓存语义和诊断边界，不实现 TLS、认证、租户隔离或公网部署加固。

