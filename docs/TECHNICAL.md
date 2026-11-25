# 多环境部署一致性验证平台 — 技术说明

## 1. 运行与验收

要求 Go 1.22+。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`，Windows 下设置 `CGO_ENABLED=0` 即可构建，不需要 Docker、外部数据库或真实账号。

Windows PowerShell：

```powershell
.\scripts\demo.ps1
```

Linux/macOS/WSL：

```bash
./scripts/demo.sh
```

脚本会执行单元/集成测试、构建平台、通过真实 HTTP 提交样本、制造并发增量、级联重建、依赖环、环境漂移、探测超时、队列满、预算截断，并强杀平台进程后重启验证意图恢复。机器可读结果写入：

- `artifacts/results/e2e-result.json`
- `artifacts/results/demo-run.log`

手动启动：

```bash
go run ./cmd/platform \
  -addr 127.0.0.1:18080 \
  -db workspace/platform.db \
  -workdir workspace
```

仅生成确定性样本：

```bash
go run ./cmd/platform -write-samples-only -workdir workspace
```

交叉编译 Windows：

```powershell
$env:CGO_ENABLED="0"
$env:GOOS="windows"
$env:GOARCH="amd64"
go build -o bin/platform.exe ./cmd/platform
```

## 2. 代码结构

- `internal/model`：环境、部署单元、快照、增量、业务错误码、摘要和语义版本比较。
- `internal/spec`：JSON 解析、checksum 校验、环境内服务唯一、依赖引用、镜像摘要和增量合并校验。
- `internal/derive`：部署计划、Kahn 拓扑分层、环识别、受阻单元、一致度评分、制品引用和反向依赖级联集合。
- `internal/diff`：同服务跨环境镜像、配置键、期望版本比较，严重级别稳定排序和漂移判定。
- `internal/probe`：全局/每环境信号量、有界等待队列、轮次时间预算、稳定调度、去重、真实 HTTP 超时与连接取消。
- `internal/store`：modernc SQLite、关系表、派生产物、引用索引、探测轮次/结果、意图日志和崩溃恢复。
- `internal/api`：HTTP JSON API、统一错误信封和内置回环探测端点。
- `internal/demo`：确定性样本生成器。
- `cmd/platform`：平台进程入口。
- `cmd/e2e`：跨平台真实子进程 E2E，包含 `Kill()` 强杀和重启。

## 3. 主要 HTTP 接口

| 方法与路径 | 作用 |
| --- | --- |
| `GET /healthz` | 健康检查与当前完整版本 |
| `POST /api/snapshots` | 提交带版本、生成时间、checksum 的完整快照 |
| `POST /api/increments` | 提交带 `baseVersion` 的局部增量 |
| `GET /api/state` | 读取当前关系化快照 |
| `GET /api/plans?environment=prod` | 查询部署计划及探测状态 |
| `GET /api/topology?environment=staging` | 查询分层、环和受阻单元 |
| `GET /api/diagnostics/dependency-cycle?environment=staging` | 环存在时返回 `E_DEPENDENCY_CYCLE` 与部分拓扑 |
| `GET /api/scores` | 跨环境一致度评分 |
| `GET /api/refs?environment=prod&service=identity&configKey=SESSION_TTL` | 查询单元/配置键到制品的引用 |
| `GET /api/cascade/latest` | 最近一次级联失效/重建范围 |
| `GET /api/diffs?baseline=prod&toleranceUnits=1` | 差异诊断和漂移判定 |
| `POST /api/probe-rounds` | 发起并行探测轮次 |
| `GET /api/probe-runs/{id}` | 查询轮次、计数、预算和结果 |
| `GET /api/intents?status=pending` | 查询意图日志 |
| `POST /api/admin/recover` | 手动扫描未完成意图 |
| `/probe/ok`、`/probe/fail`、`/probe/slow` | 内置回环测试目标 |

探测轮次请求示例：

```json
{
  "environment": "prod",
  "globalConcurrency": 2,
  "envConcurrency": 1,
  "queueCapacity": 8,
  "budgetMs": 1500,
  "probeTimeoutMs": 300,
  "services": ["identity", "gateway"]
}
```

`services` 省略时选择环境内全部单元；显式传 `"allServices": false` 得到空轮次结论。请求也可用 `probePath` 指向 `/probe/slow?delay=2s` 等内置确定性场景。

## 4. 业务码

| 业务码 | 触发条件 | HTTP 状态 |
| --- | --- | --- |
| `E_SPEC_INVALID` | JSON/checksum/模型引用或格式校验失败 | 422 |
| `E_STALE_BASE` | 增量基础版本不是当前版本；同基础版本并发竞争的负者 | 409 |
| `E_ENV_DRIFT` | 落后单元数超过声明容差 | 409 |
| `E_DEPENDENCY_CYCLE` | 环境依赖图存在环，响应附部分分层和受阻清单 | 422 |
| `E_PROBE_QUEUE_FULL` | 运行中任务占满名额且有界等待队列已满 | 429 |
| `E_PROBE_TIMEOUT` | 单个探测超过独立超时 | 200 结果项 / 调度报告 |
| `E_PROBE_BUDGET_EXCEEDED` | 轮次总预算先于单探测超时到期 | 200 结果项 / 调度报告 |
| `E_NOT_FOUND` | 路由、版本、运行记录或环境不存在 | 404 |
| `E_INTERNAL` | 未预期错误 | 500 |

## 5. 一致性与恢复设计

每次快照、增量、探测结果和探测轮次都先向 `intents` 写 `pending` 记录。版本提交在同一个 SQLite 事务中完成：

1. 校验当前版本或 `baseVersion`。
2. 替换环境、单元、配置键和依赖边。
3. 重建计划、拓扑、评分和 `artifact_refs`。
4. 写快照版本与级联事件。
5. 更新 `meta.current_version`。
6. 把意图标记为 `committed` 并提交事务。

SQLite 单连接串行化写事务，配合条件版本检查保证同一 `baseVersion` 的两个并发增量只有一个成功。另一个在事务内看到新版本后返回 `E_STALE_BASE`，并把其意图标记为 `aborted`。

镜像摘要、配置键或依赖变化时，`derive.CascadeUnits` 沿反向依赖边计算全部受影响下游。例如 `identity` 变化会级联到依赖它的 `gateway` 和 `billing`。所有派生产物与引用索引在同一事务中删除并重建，外部查询不会看到“新产物 + 旧索引”的中间状态。

探测结果提交也在同一事务内写 `probe_results`、更新 `units.probe_status`、更新 `plans.status/payload_json` 和轮次预算消耗，并提交结果意图。因此不会出现探测成功而计划仍为 `pending` 的可见状态。

进程被 `Kill()`/`kill -9` 强制终止后，SQLite 中未提交事务自动回滚。重启时 `Store.Recover` 扫描 `pending` 意图：

- 未完成的版本/增量意图标记为 `aborted`；
- 未完成的探测轮次标记为 `interrupted`；
- `current_version` 保持为最后一个完整提交版本；
- 恢复动作和恢复版本写入 `recovery_events`。

E2E 报告中的 `force_kill_recovery` 步骤真实验证了该路径。

## 6. 调度语义

任务先按环境 `priority` 升序、环境名、服务名稳定排序。调度器维护：

- `globalConcurrency`：全局活动探测上限；
- `envConcurrency`：每个环境活动探测上限；
- `queueCapacity`：超名额任务的有界等待队列；
- `budgetMs`：整轮截止时间；
- 每个任务自己的 `probeTimeoutMs`。

有名额时立即启动；无名额时进入队列，队列满则返回 `E_PROBE_QUEUE_FULL`。任务完成后先释放全局/环境名额并触发排队任务，再原子持久化结果。同一 `environment/service` 在一轮中去重。无任务时输出 `status=empty_round` 与 `QUEUE_EMPTY` 结论。

HTTP 探测使用 `context.WithTimeout`，并设置 `request.Close=true` 与 `DisableKeepAlives=true`。超时或轮次预算取消会关闭请求上下文和底层连接；内置 `/probe/slow` 端点通过 `r.Context().Done()` 可观测到连接取消。
