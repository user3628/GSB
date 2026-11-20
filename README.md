# Loopback Proxy Workbench

一个只使用 Python 标准库的本地回环正向/反向代理工作台。它把带版本号的 JSON 路由配置编译为不可变快照，并保证配置、匹配索引、连接池分组、健康探测计划和转发规则缓存原子发布。项目支持增量配置、过期基础版本拒绝、依赖级联失效、健康探测与故障切换、SQLite 意图日志和强杀恢复，以及内置中文诊断页面。

全部网络访问被限制在 `127.0.0.1` / `::1` / `localhost`，不访问真实站点、不使用账号或外部服务。

## 快速开始（Windows PowerShell）

要求 Python 3.11 或以上，不需要安装任何第三方包。

```powershell
cd <项目目录>
py -3.11 -m unittest discover -v
py -3.11 scripts\demo.py
```

演示脚本会先确认固定回环端口空闲，然后启动本地后端并完成全量提交、增量提交、过期版本拒绝、转发和错误链路验证。固定端口为：

- 工作台：`19080`
- 后端 A：`19101`
- 后端 B：`19102`
- 故意拒绝连接的端口：`19109`

启动常驻工作台：

```powershell
py -3.11 -m proxy_workbench serve `
  --db artifacts\workbench.db `
  --seed samples\full_snapshot.v3.json `
  --port 18080
```

打开 `http://127.0.0.1:18080/dashboard` 查看版本、路由产物、上游健康、最近错误和级联失效轨迹。`--seed` 只在数据库没有完整快照时使用；已有持久化配置始终优先。

## 交付内容

- `proxy_workbench/models.py`：路由、匹配、超时、重试、健康、故障策略的严格校验与快照解析。
- `proxy_workbench/compiler.py`：把每条路由编译为四项产物，并构建聚合索引和级联失效清单。
- `proxy_workbench/store.py`：SQLite 快照、版本历史、写前意图、进程状态和恢复事实报告。
- `proxy_workbench/transport.py`：真实异步 HTTP/1.1 上游交换、逐跳头部处理、连接关闭和异常分类。
- `proxy_workbench/health.py`：探测计划 generation、真实转发结果、不可用阻断和同组备用切换。
- `proxy_workbench/runtime.py`：增量/全量提交、并发串行化、不可变发布、请求绑定、诊断聚合。
- `proxy_workbench/server.py`：代理入口、管理 API、内置 HTML 工作台和安全退出排空。
- `proxy_workbench/cli.py`：Windows/Linux 可用的服务启动入口和跨平台信号处理。
- `samples/`：固定全量快照、仅修改单路由上游的增量、基于过期基础版本的增量。
- `scripts/demo.py`：固定本地样本端到端演示，输出失效/重建产物和最终版本。
- `tests/`：10 个自动化测试，包括真实子进程 `os._exit` 强杀恢复。
- `docs/DESIGN.md`：原子性、状态机、持久化顺序和失败语义说明。

## 配置格式

### 全量快照

```json
{
  "kind": "snapshot",
  "version": 3,
  "routes": [
    {
      "id": "echo",
      "kind": "reverse",
      "match": {
        "methods": ["GET", "POST"],
        "path_prefix": "/api/echo"
      },
      "upstream": "http://127.0.0.1:19101",
      "timeout": {"connect": 0.5, "read": 1.5},
      "retries": {
        "attempts": 2,
        "backoff_ms": 10,
        "retry_on": ["connection_refused", "read_timeout", "5xx"]
      },
      "headers": {"X-Route": "echo"},
      "health": {
        "enabled": true,
        "path": "/health",
        "interval": 2.0,
        "timeout": 0.4,
        "fail_threshold": 1,
        "success_threshold": 1
      },
      "failure_policy": {
        "mode": "fail",
        "backup_upstreams": [],
        "group": "echo-group"
      },
      "in_flight_policy": "continue"
    }
  ]
}
```

### 单路由增量

```json
{
  "kind": "incremental",
  "base_version": 3,
  "version": 4,
  "routes": [
    {
      "id": "echo",
      "kind": "reverse",
      "match": {"methods": ["GET", "POST"], "path_prefix": "/api/echo"},
      "upstream": "http://127.0.0.1:19102",
      "timeout": {"connect": 0.5, "read": 1.5},
      "retries": {"attempts": 2},
      "headers": {"X-Route": "echo"},
      "health": {"enabled": true, "path": "/health"},
      "failure_policy": {"mode": "fail", "group": "echo-group"},
      "in_flight_policy": "continue"
    }
  ]
}
```

增量中的 `routes` 是 affected-route upsert 集合，不会替换未提交的路由。可选的 `remove_route_ids` 用于删除路由。增量必须携带 `base_version`；只要它不等于锁内的当前版本，就返回 `E_STALE_BASE`，不写意图、不改变配置或编译状态。

完整快照整体替换路由集合；目标版本必须严格大于当前版本，否则返回 `E_VERSION_ROLLBACK`。允许版本号跳跃，但所有已提交版本都会持久化。

### 正向与反向路由

- `kind=reverse`：客户端向工作台发送 origin-form 请求（如 `GET /api/echo?q=1`），按方法和最长路径前缀匹配，再转发到 `upstream`。
- `kind=forward`：客户端向工作台发送 absolute-form 请求（如 `GET http://127.0.0.1:19102/forward/x?q=1`），按目标主机、目标端口、方法和路径前缀匹配。
- 正向路由只支持明文 `http://`，不支持 `CONNECT` 或 TLS 中间人。
- 若 `upstream` 包含基础路径，例如 `http://127.0.0.1:19101/status/500`，会与入站路径拼接；查询串原样保留。
- 方法、请求体、端到端请求头、上游状态码和响应体均转发；每次请求使用一条独立 HTTP/1.1 连接并以 `Connection: close` 结束。

## 逐跳头部边界

以下头部不会转发到上游，也不会回传给客户端：

`Connection`、`Keep-Alive`、`Proxy-Authenticate`、`Proxy-Authorization`、`Proxy-Connection`、`TE`、`Trailer`、`Transfer-Encoding`、`Upgrade`，以及 `Connection` 头中列出的扩展逐跳字段。

具体行为：

- 客户端 `Host` 和 `Content-Length` 被移除后按实际上游和读入后的请求体重建。
- 配置中的请求头不得使用上述逐跳字段；配置头可覆盖同名端到端入站头。
- 上游 `Transfer-Encoding: chunked` 会被解码，响应体以 `Content-Length` 和 `Connection: close` 回传。
- 上游 1xx 中间响应会被跳过；`101 Upgrade` 明确拒绝。
- 额外加入 `X-Forwarded-For`、`X-Forwarded-Proto`，以及诊断响应头 `X-Workbench-Version/Route/Upstream/Attempts`。

## 编译产物与级联失效

每条路由固定编译四项产物：

1. `matching_table`：方法、路径前缀/目标主机端口匹配项。
2. `connection_pool_group`：上游候选、超时和逻辑池分组。
3. `health_probe_plan`：探测目标、路径、间隔、超时和成功/失败阈值。
4. `forward_rule_cache`：上游、请求头、重试、故障策略和在途策略。

另有五个聚合产物：

- `index:artifact-manifest`
- `index:reverse-match`
- `index:forward-match`
- `index:upstream-groups`
- `index:health-plans`

路由指纹由完整规范化配置计算。路由新增、删除或任何字段变化时，该路由全部四项产物和依赖聚合索引进入失效清单；随后在本地构建完整新快照，通过原子校验后一次替换运行时指针。请求在读取请求头后绑定快照，因此只会看到旧快照或新快照，不会看到混合状态。未变化路由的产物可复用并继续显示原 `built_version`。

## 并发、在途请求与健康

- 所有配置更新由同一 `asyncio.Lock` 串行化；同一 `base_version` 的两个并发增量只有一个成功。
- 已在途请求绑定提交前的 `CompiledState`。`in_flight_policy=continue` 沿用旧上游完成；`abort` 在上游尝试前及响应发布前检测 generation，变化后返回 `E_CONFIG_CHANGED`。
- 更新完成后会取消旧健康探测任务并递增 generation；旧探测结果因 generation 不匹配被丢弃。新计划立即按新上游/时间字段重新探测。
- 健康状态由周期探测和真实转发共同决定。连续失败达到阈值后标记 `unhealthy`。
- `failure_policy.mode=fail` 时，已知不可用主上游不会再收到新请求，返回 `E_UPSTREAM_UNAVAILABLE`。
- `failure_policy.mode=failover` 时按 `backup_upstreams` 顺序选择首个非 `unhealthy` 候选；也允许本轮重试在失败后切到备用。

## 业务错误码

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_STALE_BASE` | 409 | 增量的基础版本不是当前版本 |
| `E_VERSION_ROLLBACK` | 409 | 快照或增量目标版本不大于当前版本 |
| `E_CONFIG_INVALID` | 400 | 配置字段、回环边界或逐跳头配置非法 |
| `E_CONFIG_CHANGED` | 409 | 在途请求按 `abort` 策略终止 |
| `E_ROUTE_NOT_FOUND` | 404/503 | 无匹配路由或尚未加载配置 |
| `E_UPSTREAM_UNAVAILABLE` | 503 | 主上游已知不可用且无可用备用 |
| `E_CONNECT_TIMEOUT` | 504 | 建立上游 TCP 连接超时 |
| `E_READ_TIMEOUT` | 504 | 上游响应头/响应体读取超时 |
| `E_WRITE_TIMEOUT` | 504 | 发送上游请求超时 |
| `E_CONNECTION_REFUSED` | 502 | 上游主动拒绝连接 |
| `E_CONNECT_FAILED` | 502 | 其他连接失败 |
| `E_UPSTREAM_5XX` | 上游 5xx | 上游返回 5xx；HTTP 状态沿用上游，JSON 中保留原始状态和有限响应体 |
| `E_UPSTREAM_PROTOCOL` | 502 | 上游响应协议错误 |
| `E_REQUEST_INVALID` | 400/408 | 客户端请求格式或读取问题 |
| `E_REQUEST_TOO_LARGE` | 413/431 | 请求头/请求体超限 |
| `E_UNSUPPORTED_TARGET` | 400/501 | 不支持的请求目标或 HTTPS absolute-form |
| `E_SHUTTING_DOWN` | 503 | 服务正在安全退出，明确拒绝新连接 |

错误体结构稳定为：

```json
{
  "ok": false,
  "code": "E_READ_TIMEOUT",
  "message": "upstream response header read timed out",
  "details": {"upstream": "http://127.0.0.1:19101/slow"},
  "config_version": 4
}
```

所有超时使用 `asyncio.wait_for` 真正取消 I/O；只要已取得 writer，就在 `finally` 中执行 `writer.close()` 和 `wait_closed()`。

## 管理 API

```text
POST /__workbench/config       提交 snapshot 或 incremental JSON
GET  /__workbench/state        当前版本、路由、产物、健康、恢复和退出信息
GET  /__workbench/diagnostics  完整诊断 JSON
GET  /__workbench/artifacts    全部产物及失效轨迹
GET  /__workbench/errors       最近转发/配置错误
GET  /__workbench/health       进程存活和工作台版本
GET  /dashboard                内置可视化页面
POST /__workbench/shutdown     请求安全退出
```

## 持久化、强杀恢复与退出

配置提交的持久化顺序：

1. `BEGIN IMMEDIATE` 写入并提交 `intents(state=prepared)`，形成写前意图。
2. 在另一事务中写入 `snapshots(complete=1)`、更新当前版本和提交历史，然后提交。
3. 标记意图 `committed`，再原子发布内存快照并启动新健康计划。

进程被 `SIGKILL`、任务管理器强杀或 `os._exit()` 终止后，重启会扫描 `prepared` 意图：

- 目标完整快照不存在：意图标记 `aborted`，保留最后一个完整版本。
- 目标完整快照已持久化：标记 `commit_already_durable`，加载该完整版本。
- 不完整快照行被删除；`meta.current_version` 根据完整快照最大值校准。
- 恢复报告只包含 SQLite 查询到的版本、意图和操作事实。

安全退出时先停止接受新连接，并等待已接纳请求在 grace timeout 内完成；超时才取消剩余任务并如实记录。退出记录持久化为 `last_shutdown`。因此重启诊断可区分：

- 强杀：上次 `process_state=running`，存在未完成意图，`previous_exit_clean=false`。
- 安全退出：上次 `process_state=stopped`，`previous_exit_clean=true`，并显示完成/取消数。

## 实际验证记录

验证日期：2026-11-20。当前执行环境为 Linux 上的 Python 3.14.4；代码只使用 Python 3.11 已提供的标准库 API，并提供 Windows ProactorEventLoop 信号回退路径。Windows 上无需 Docker、WSL 或第三方依赖。

```text
$ python3 -m unittest discover -v
Ran 10 tests ...
OK
```

覆盖结果：

- 反向 POST 的方法、路径、查询串、请求体和配置头正确到达上游。
- 正向 absolute-form POST 正确按目标主机/端口匹配并转发。
- `Connection` 及其提名头、`Content-Length` 边界处理正确。
- 仅修改单条路由上游时，该路由 4 项产物和 5 个聚合索引全部失效并重建，其余 20 个产物复用。
- 同一基础版本的两个并发增量一个有 `OK`，另一个稳定返回 `E_STALE_BASE`。
- 过期基础版本和版本回退均在有状态变化前拒绝。
- 读取超时为 `E_READ_TIMEOUT`，拒绝连接为 `E_CONNECTION_REFUSED`，5xx 为 `E_UPSTREAM_5XX`，连接超时映射为 `E_CONNECT_TIMEOUT`。
- 健康探测命中 503 后，目标请求不会再发送给已知不可用主上游；失效主上游会切换至同组备用。
- 旧探测计划在路由上游变化后被取消，新计划按新上游重新探测。
- 在途请求分别验证 `continue` 使用旧版本和 `abort` 返回 `E_CONFIG_CHANGED`。
- 真实子进程在意图写入后执行 `os._exit(99)`；重启后回到最后完整版本 1，恢复报告记录回滚事实。
- 安全退出的下一进程报告 `previous_exit_clean=true` 和 `shutdown.mode=graceful`。

演示脚本实际结果保存在 `artifacts/demo/report.json`：

```text
full_commit:        OK, v3, 29 artifacts rebuilt
incremental_commit: OK, v4, changed_route_ids=[echo]
invalidated:        9 项（echo 的 4 项产物 + 5 个聚合索引）
rebuilt:            9 项
reused:             20 项
stale_commit:       HTTP 409, E_STALE_BASE, current_version=4
final_version:      4
dashboard:          HTTP 200
```

## 范围与已知限制

- 仅面向本地回环明文 HTTP 测试，不实现 HTTPS `CONNECT`、WebSocket、HTTP/2。
- 请求与响应体在内存中缓冲，分别限制为 16 MiB 和 32 MiB；未实现流式背压。
- “连接池分组”作为可失效、可诊断的编译产物存在，并统一约束超时/候选集；当前传输层为资源可证明释放的一请求一连接，不跨请求复用 TCP 连接。
- 健康状态是进程内运行态，快照和探测计划会持久化配置，但不把瞬时健康状态当作配置事实写入 SQLite。

