# 服务网格治理策略仿真编排工作台

这是一个完全本地、确定性的服务网格流量治理策略仿真工作台。它接收不可变、带版本号和 SHA-256 校验摘要的策略快照，对离线请求轨迹逐条计算路由、限流、超时、熔断和最终响应，并生成可追溯的差异工单。系统只使用本地 JSON 样本和 Python 标准库 SQLite，不连接真实服务网格控制面。

核心约束是**策略版本一致性**与**裁决可复现性**：批次固定引用一个已提交快照版本及其摘要；同一快照、同一轨迹重复仿真时，逐条裁决完全一致；批次结论只有在完整事务提交后才可见。

## 交付内容

- FastAPI 本地 HTTP API，默认只看回环地址。
- 带版本、规范化和摘要校验的策略快照模型。
- 纯函数、整数毫秒时间语义的离线仿真裁决引擎。
- 限流固定窗口、连续失败熔断、半开探测、超时和连接失败裁决。
- 超时与熔断窗口冲突识别，按路由逐项比较多版本冲突集合。
- 去重、按确定性公式定级并稳定排序的差异工单。
- SQLite 快照、批次、写意图、暂存裁决、最终裁决、工单和恢复报告持久化。
- 外部强制终止后启动扫描、批次中止和基于已提交事实的恢复报告。
- 固定轨迹、两份策略快照、回环上游测试端点和端到端演示脚本。
- pytest 单元/API 测试和本次实际验证产物 `artifacts/demo-result.json`。

## 目录

```text
app/
  main.py          FastAPI 应用与统一错误响应
  models.py        严格输入模型与状态枚举
  digest.py        规范 JSON 与摘要
  engine.py        路由/限流/熔断/冲突/工单纯函数引擎
  storage.py       SQLite、写意图、原子发布、恢复
  service.py       批次编排、版本比较、严格歧义诊断
  crash_worker.py  暂存部分裁决后等待外部强杀
samples/
  trace_baseline.json                 18 条固定请求轨迹
  policy_v1.json                      版本 1 基线策略
  policy_v2_conflict_ambiguous.json   版本 2 冲突/歧义策略
tests/           引擎与 HTTP API 自动化测试
tools/
  local_test_endpoint.py  仅绑定 127.0.0.1 的上游测试端点
  upstream_demo.py        超时与连接失败演示
  demo.py                 全链路 HTTP 演示
scripts/          Windows PowerShell 安装、运行、验证脚本
```

## 环境与版本

本次实际验证环境：

| 组件 | 版本 |
|---|---:|
| Python | 3.14.4 |
| pip | 26.3 |
| FastAPI | 0.165.0 |
| Uvicorn | 0.54.0 |
| httpx | 0.28.1 |
| pytest | 9.2.0 |
| SQLite（Python 标准库） | 3.46.1 |

项目要求 Python 3.11 或更高版本。直接依赖固定在 `requirements.txt`，完整解析结果记录在 `requirements.lock.txt`。

## Windows PowerShell 快速开始

```powershell
# 1. 确认 Python 和 pip
python --version
python -m pip --version

# 2. 允许当前进程执行本地脚本
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 3. 创建虚拟环境并安装固定版本依赖
.\scripts\setup.ps1

# 4. 运行测试
.\.venv\Scripts\python.exe -m pytest

# 5. 启动本地 API（127.0.0.1:8765）
.\scripts\run.ps1

# 另一个 PowerShell 窗口：运行完整演示
.\scripts\demo.ps1

# 或一次执行测试和演示
.\scripts\verify.ps1
```

`scripts/run.ps1` 只绑定 `127.0.0.1`。演示脚本会自动选择空闲回环端口，避免与本机其他服务冲突。OpenAPI 文档位于 `http://127.0.0.1:8765/docs`。

数据库默认位于项目内 `data/workbench.db`；演示使用 `data/demo.db`。无需安装或启动外部数据库。

## 固定样本

`samples/trace_baseline.json` 包含 18 条请求，时间戳均为整数毫秒，包含方法、路径、匹配请求头，以及上游状态码、耗时和连接失败标志。请求按 `(timestamp_ms, request_id)` 排序后仿真。

| 快照 | 版本 | 摘要 |
|---|---:|---|
| `policy_v1.json` | 1 | `sha256:f1e6c91a67fb1fd28153c27075d57ea35b6aacb9507d0fe1499d67ded2de3293` |
| `policy_v2_conflict_ambiguous.json` | 2 | `sha256:f2021690a6916ae3a7e314145cbd0c77659d668867ddc578fda88a55313e21f3` |

摘要的计算规则：取快照的规范化业务字段（不含 `checksum`），请求头名称统一小写，JSON 键排序、使用紧凑分隔符、UTF-8 编码、不做 Unicode 转义，再计算 SHA-256，文本形式为 `sha256:<64位十六进制>`。

## HTTP API

所有业务错误统一返回：

```json
{
  "error": {
    "code": "E_STALE_SNAPSHOT",
    "message": "snapshot version must be greater than the current committed version",
    "details": {}
  }
}
```

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/health` | 运行状态、Python 版本、数据库和当前快照 |
| POST | `/api/policies` | 原子提交策略快照 |
| GET | `/api/policies/current` | 当前最高已提交快照 |
| GET | `/api/policies/{version}` | 读取指定不可变快照 |
| POST | `/api/simulations` | 提交并执行轨迹；`execution=prepare` 仅创建 PENDING 批次 |
| GET | `/api/simulations/{batch_id}` | 批次摘要；仅 COMPLETED 时附统计/冲突结论 |
| GET | `/api/simulations/{batch_id}/decisions` | 完整批次的逐条裁决 |
| GET | `/api/simulations/{batch_id}/orders` | 完整批次的差异工单 |
| GET | `/api/simulations/{batch_id}/conflicts` | 完整批次的参数冲突集合 |
| GET | `/api/simulations/compare?left=&right=` | 同轨迹不同快照的裁决和按路由冲突差异 |
| POST | `/api/simulations/{batch_id}/transition` | 本地诊断用显式状态迁移 |
| GET | `/api/orders/{order_id}` | 单张工单 |
| POST | `/api/routes/resolve` | 路由解析；`strict=true` 对平级歧义返回错误 |
| POST | `/api/recovery/scan` | 扫描未完成批次并生成恢复报告 |
| GET | `/api/recovery/reports` | 全部恢复报告 |
| GET | `/api/recovery/reports/{batch_id}` | 指定批次恢复报告 |

统一业务码至少包括：

| 业务码 | 典型 HTTP 状态 | 含义 |
|---|---:|---|
| `E_STALE_SNAPSHOT` | 409 | 版本号不大于当前已提交版本 |
| `E_SNAPSHOT_INVALID` | 400/500 | 提交摘要或持久化摘要校验失败 |
| `E_SNAPSHOT_NOT_FOUND` | 404 | 指定快照不存在 |
| `E_BATCH_NOT_FOUND` | 404 | 批次或恢复报告不存在 |
| `E_BATCH_INCOMPLETE` | 409 | 批次没有完整可查询结论 |
| `E_ORDER_NOT_FOUND` | 404 | 工单不存在 |
| `E_ROUTE_AMBIGUOUS` | 409 | 严格路由诊断遇到平级最高优先级冲突 |
| `E_INVALID_ARGUMENT` | 400/409/422 | 参数、迁移或操作不合法，状态不变 |

## 关键业务语义

### 快照与版本

1. 提交在 `BEGIN IMMEDIATE` 事务内先写 `write_intents` 意图记录。
2. 先检查版本。快照版本小于或等于当前已提交最高版本时返回 `E_STALE_SNAPSHOT`，事务回滚，不改变任何业务状态。
3. 再检查摘要。不符时返回 `E_SNAPSHOT_INVALID`，同样不改变状态。
4. 通过后整行写入不可变快照，提交后成为当前版本。历史版本不更新、不混用字段。
5. 批次同时保存 `snapshot_version` 与 `snapshot_checksum`；所有裁决只从该精确版本加载，因此不会出现新旧字段混用。

### 路由

规则同时满足路径前缀和全部必需请求头才算命中。规则按以下总序裁决：

```text
priority 降序
-> path_prefix 长度降序
-> 必需请求头数量降序
-> rule_id 字典序升序
```

全部整数和字符串比较，无随机性。无规则命中时使用 `default_route`。最高优先级分组内仍有多个规则时，裁决结果标记 `tied_at_top_precedence=true`；仿真仍按最终 `rule_id` 稳定裁决，严格诊断接口则返回 `E_ROUTE_AMBIGUOUS`。裁决明细包含全部候选规则和解析策略。

### 固定窗口限流

- 时间单位全部为整数毫秒，拒绝浮点时间。
- 窗口起点为 `floor(timestamp_ms / window_ms) * window_ms`，以 Unix epoch 对齐。
- 区间严格为 `[window_start_ms, window_end_ms)`：请求在起点属于新窗口，在终点不属于旧窗口。
- 同一路由同一窗口先计数再裁决；窗口内第 `threshold + 1` 条及之后请求返回 `RATE_LIMITED`/HTTP 429。
- 限流先于熔断判断；被限流请求不会尝试上游，也不会改变连续失败计数。

### 超时、熔断与最终响应

- 上游连接失败计为熔断失败；`upstream.latency_ms > timeout_ms` 为超时失败；HTTP 5xx 为熔断失败；HTTP 4xx 是上游错误但不计入熔断失败。
- 连续失败达到阈值时，当前请求已经完成上游尝试，随后状态 `CLOSED -> OPEN`，`open_until_ms = request.timestamp_ms + breaker.window_ms`。
- 时间戳严格小于 `open_until_ms` 的请求返回 `CIRCUIT_OPEN`/HTTP 503，不访问上游。
- 时间戳等于或大于 `open_until_ms` 时进入 `HALF_OPEN` 并允许一次探测；成功则 `CLOSED`，失败则重新 `OPEN`。
- 成功请求将连续失败数清零。所有状态切换都记录原因、请求、时间和前后状态。
- 最终响应类型包括 `SUCCESS`、`RATE_LIMITED`、`CIRCUIT_OPEN`、`TIMEOUT`、`UPSTREAM_ERROR`、`UPSTREAM_CONNECTION_ERROR`。超时使用网关超时值作为响应耗时，其余保留上游观测耗时。

### 参数冲突

当路由 `timeout_ms > circuit_breaker.window_ms` 时，定义存在 `TIMEOUT_BREAKER_CONFLICT`。其中 `breaker_window_ms` 表示熔断窗口可容忍的上游响应时长上界。冲突记录明确给出两个数值和超出量。

冲突检测只追加标记，不参与路由、限流、超时或熔断状态计算；测试通过开关冲突标记并逐条比较，确认裁决结果完全一致。冲突集合可按路由在两个版本间标记为 `ADDED`、`REMOVED`、`CHANGED` 或 `UNCHANGED`。

### 差异工单

支持的差异类型和权重：

| 类型 | 权重 | 触发条件 |
|---|---:|---|
| `TIMEOUT_BREAKER_CONFLICT` | 10 | 路由超时大于熔断窗口 |
| `CIRCUIT_FREQUENT_TRANSITION` | 8 | 路由熔断状态迁移次数大于等于 2 |
| `RATE_LIMIT_HIGH_REJECTION` | 7 | 限流拒绝数 / 该路由命中请求数大于等于 20% |
| `ROUTE_MATCH_AMBIGUITY` | 5 | 多个规则在最高优先级、路径长度和请求头数上平级 |

计算全部使用整数：

```text
affected_ratio_bp = affected_requests * 10000 // total_requests
priority_score   = affected_ratio_bp * type_weight
P1: score >= 50000, P2: score >= 20000, 其余 P3
```

排序键为 `(-priority_score, -type_weight, -affected_requests, route_id, difference_type)`，`rank` 从 1 开始。数据库对 `(batch_id, route_id, difference_type)` 再加唯一约束，保证同批次同路由同类型只有一张工单。

每张工单的 `metrics` 还保存受影响请求 ID、对应裁决类型/响应状态/触发参数等证据，可追溯到具体请求与具体裁决项。

## SQLite、写意图与强杀恢复

主要表：`snapshots`、`batches`、`write_intents`、`decision_staging`、`decisions`、`work_orders`、`recovery_reports`。

合法批次状态迁移：

```text
PENDING -> RUNNING -> COMPLETED
    \          \
     +----------> ABORTED
```

`COMPLETED` 只能由原子 `finalize_batch` 操作产生；非法迁移返回 `E_INVALID_ARGUMENT`，先写意图后回滚，原状态不变。

正常执行：

1. 创建 `PENDING` 批次并提交意图。
2. 提交 `RUNNING` 迁移，并留下 `SIMULATE_BATCH` 的 `STARTED` 意图。
3. 在内存中完成纯函数仿真，把逐条裁决写入对查询不可见的 `decision_staging`。
4. 最终事务从暂存复制到 `decisions`、写入工单、更新统计和 `COMPLETED`、删除暂存、结算意图，一次提交。

任意时刻被强制终止时，最终事务要么全部可见，要么完全不存在。重启时 lifespan 自动扫描，也可调用 `POST /api/recovery/scan`：未完成批次改为 `ABORTED`，根据已提交的暂存数、最后暂存请求、工单数和开放意图生成恢复报告。恢复报告只报告事实，明确 `complete_conclusion_available=false`，不会声称已经生成完整退出报告，也不会补造缺失裁决。未完成或已中止批次的裁决查询返回 `E_BATCH_INCOMPLETE`。

## 本次实际验证

自动化测试：

```text
14 passed
```

覆盖同版本重复仿真逐条一致、默认路由、平级歧义、固定窗口边界、浮点时间戳拒绝、严格超时边界、熔断半开探测、冲突标记不干扰裁决、工单去重/定级/排序、摘要与陈旧快照原子性、错误体、非法迁移、半成品不可见、显式恢复和启动恢复。

端到端演示实际结果：

- v1 共 18 条裁决：9 成功、6 超时、3 熔断拒绝；生成 1 张 `CIRCUIT_FREQUENT_TRANSITION` 工单。
- 同版本重复仿真的裁决 SHA-256 均为 `sha256:0a39bad21f5cfce1965ad75bbde390db477b5efe302eb0876a6c5fbcdbb46b15`，逐条完全一致。
- v2 共 18 条裁决：10 成功、2 限流拒绝、1 熔断拒绝、5 上游 5xx；3 条路由存在 `timeout_ms=6000 > breaker_window_ms=3000` 冲突；生成 6 张去重工单，其中 checkout 冲突工单为 P1。
- v1 与 v2 有 17 条请求的裁决发生差异；冲突比较按路由给出 3 个 `ADDED`。
- 严格路由诊断对 campaign/retry 平级冲突返回 `E_ROUTE_AMBIGUOUS`，非严格裁决得到 `checkout_campaign`，并保留完整候选与优先级。
- 强杀测试先暂存 5 条裁决，外部发送强制终止（WSL 中返回码 `-9`）；恢复扫描后批次为 `ABORTED`，报告保留 5 条暂存事实、0 条最终裁决，裁决查询返回 `E_BATCH_INCOMPLETE`。
- 回环端点验证：正常 200；150ms 客户端超时触发 `ReadTimeout`；关闭回环端口触发 `ConnectError`。

完整证据保存在 `artifacts/demo-result.json`。运行 `.\scripts\verify.ps1` 可重新生成同样语义的验证结果；批次 ID、时间和强杀返回码属于运行时元数据，不影响裁决与工单内容。

## 边界说明

- 这是策略仿真工作台，不向真实服务网格下发配置，不调用 Kubernetes/Istio 控制面。
- 上游测试端点只绑定 `127.0.0.1`；演示不会访问外部目标。
- 本地 API 面向可信单机环境，未实现用户认证或多租户隔离。
- SQLite 使用 WAL 和 `BEGIN IMMEDIATE` 保证本地事务一致性；不宣称跨主机或分布式一致性。
- 强制终止后只恢复“已提交事实”，不能恢复进程内存中尚未落库的裁决。
