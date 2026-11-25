# Registry Sentinel

本地微服务测试环境的服务注册、健康探测与诊断工作台。项目从空目录实现，Go 侧只使用标准库与一个项目内置的 `database/sql` SQLite 驱动，不依赖第三方 Go 模块。

## 交付内容

- `registry`：可集成的领域组件，包含注册快照、增量提交、五态健康机、HTTP/TCP/租约探测、三重预算调度、派生投影与崩溃恢复。
- `internal/sqlite`：基于 cgo + 系统 `libsqlite3` 的本地 `database/sql` 驱动。
- `workbench`：本地 HTTP API 与交互式诊断工作台。
- `sample` / `testfixture`：固定种子注册样本与回环 HTTP/TCP 测试端点。
- `cmd/registrylab`：`serve`、`sample`、`verify` 诊断命令。
- `docs/VERIFICATION.md` 与 `artifacts/verification-report.json`：本机实际验收记录。

要求 Go 1.22+、cgo、`libsqlite3-dev` 与 SQLite CLI。开发机已使用 Go 1.27.4、SQLite 3.46.1 验证。

## 快速开始

```bash
go build -o registrylab ./cmd/registrylab
./registrylab verify --json=false
go test ./...
go test -race ./...
```

启动工作台：

```bash
./registrylab serve --db /tmp/registry-lab.db --addr 127.0.0.1:8099
# 浏览器打开 http://127.0.0.1:8099/
```

工作台会同时启动只绑定 `127.0.0.1` 的实际 HTTP/TCP 测试端点。页面可以加载固定种子样本、发布完整快照、提交增量、记录租约心跳、运行真实探测轮次，并查看健康汇总、归一化权重、探测计划、意图日志和恢复报告。

在 `serve` 运行期间，可以从工作台端点取当前端口上的样本并提交。若本机有 `jq`：

```bash
# 终端 1
./registrylab serve --db /tmp/registry-lab.db --addr 127.0.0.1:8099

# 终端 2
curl -s http://127.0.0.1:8099/api/v1/sample \
  | jq '.data.snapshot' > /tmp/snapshot.json
curl -s -X POST http://127.0.0.1:8099/api/v1/snapshots \
  -H 'Content-Type: application/json' --data-binary @/tmp/snapshot.json

# 真实探测（工作台 fixture 保持运行）
curl -s -X POST http://127.0.0.1:8099/api/v1/probe-rounds \
  -H 'Content-Type: application/json' \
  -d '{"globalConcurrency":4,"perServiceConcurrency":2,"budgetMs":1500,"waitQueueCapacity":64}'
```

没有 `jq` 时直接使用页面上的 Load deterministic sample / Publish snapshot / Run real probe round 按钮。

`sample` 命令可独立输出带摘要的确定性快照：

```bash
./registrylab sample --seed 20261126 --version 1 > /tmp/snapshot.json
```

该文件包含摘要且可直接用于注册快照提交；`sample` 进程退出后其临时探针端点也关闭，因此真实探测请使用 `serve` 工作台或 `verify`。

## Go 集成

```go
engine, err := registry.Open("/tmp/registry.db")
if err != nil { /* handle */ }
defer engine.Close()

snapshot := registry.SnapshotRequest{
    Version:     1,
    GeneratedAt: "2026-11-26T00:00:00Z",
    Instances:   instances,
}
snapshot.Digest = registry.ComputeDigest(snapshot.Version, snapshot.GeneratedAt, snapshot.Instances)
_, domainErr := engine.SubmitSnapshot(ctx, snapshot)

round, domainErr := engine.RunProbeRound(ctx, registry.RoundOptions{
    GlobalConcurrency: 4, PerServiceConcurrency: 2,
    BudgetMS: 1500, WaitQueueCapacity: 64,
})

view, domainErr := engine.ReadComplete(ctx) // 一个读事务内的完整版本
```

所有公开领域错误的形态：

```json
{
  "ok": false,
  "error": {
    "code": "E_STALE_BASE",
    "message": "baseVersion is not the current committed version",
    "context": {"currentVersion": 2, "baseVersion": 1}
  }
}
```

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/healthz` | 存活检查 |
| `GET` | `/api/v1/overview` | 单事务读取当前快照、派生视图、恢复状态与默认预算 |
| `GET` | `/api/v1/snapshot` | 当前完整注册与健康快照 |
| `GET` | `/api/v1/derived` | 健康汇总、权重分配、探测计划 |
| `POST` | `/api/v1/snapshots` | 整份快照提交 |
| `POST` | `/api/v1/deltas` | 基础版本绑定的增量提交 |
| `POST` | `/api/v1/probe-rounds` | 执行真实探测轮次 |
| `GET` | `/api/v1/probe-rounds/{id}` | 查询持久化轮次结果 |
| `POST` | `/api/v1/instances/{service}/{id}/heartbeats` | 租约心跳 |
| `POST` | `/api/v1/instances/{service}/{id}/state` | 显式状态迁移 |
| `GET` | `/api/v1/intents` | 写前意图日志 |
| `GET` | `/api/v1/recovery` | 最近恢复报告及当前提交指针 |
| `GET` | `/api/v1/sample` | 动态端口上的固定种子样本 |

请求幂等键可放在请求体的 `requestId` 或 `X-Request-ID`。同一键与同一规范化内容只执行一次；并发重复请求等待首个结果并复用；同一键携带不同内容返回 `E_CONFLICTING_REQUEST`。

## 注册与摘要

完整快照必须递增版本并携带 `generatedAt`、`digest` 和完整实例集合。摘要算法是确定性的：

1. 按 `serviceName`、`instanceId` 排序实例。
2. 对 `{version, generatedAt, instances}` 做紧凑 JSON 编码；实例只包含注册字段。
3. 计算 SHA-256 小写十六进制摘要。`digest` 与 `requestId` 不参与摘要。

`ComputeDigest` 是唯一实现。声明摘要不匹配返回 `E_VALIDATION`；版本不大于当前版本返回 `E_STALE_SNAPSHOT`；两种失败都不会改变旧版本。

增量携带 `baseVersion`、新 `version` 以及 `upserts` / `removes`。`baseVersion` 必须精确等于当前提交版本，否则返回 `E_STALE_BASE`。写者串行化保证同一基础版本只有一个增量可成功，失败方不会留下部分写入。

## 真实探测

- **HTTP**：发送真实 GET，请求同时核验约定状态码与响应体标记。状态或标记不符分别返回 `E_PROBE_STATUS_MISMATCH`、`E_PROBE_MARKER_MISMATCH`。
- **TCP**：使用真实 `net.Dialer` 拨号；成功立即关闭 socket，拒绝返回 `E_TCP_REFUSED`。
- **LEASE**：读取持久化的最近心跳与 TTL；缺失、过期、有效分别返回 `E_LEASE_MISSING`、`E_LEASE_EXPIRED`、`E_LEASE_VALID`。
- **超时**：每实例独立超时由 context 控制；HTTP 连接不保持复用，超时后请求 context 取消底层连接并调用 `CloseIdleConnections`；并发名额随探测 goroutine 结束释放。超时统一标记 `E_PROBE_TIMEOUT`。

状态机完整迁移图：

| 当前状态 | 合法目标 |
|---|---|
| `UNKNOWN` | `HEALTHY`、`SUSPECT`、`UNHEALTHY`、`DRAINING` |
| `HEALTHY` | `SUSPECT`、`UNHEALTHY`、`DRAINING` |
| `SUSPECT` | `HEALTHY`、`UNHEALTHY`、`DRAINING` |
| `UNHEALTHY` | `HEALTHY`、`SUSPECT`、`DRAINING` |
| `DRAINING` | `UNKNOWN` |

同状态请求是幂等的。非法迁移返回 `E_INVALID_TRANSITION`，不写入实例计数与派生 revision。自动探测按快照声明的连续成功/失败阈值选择合法边。

## 派生视图与原子级联

每个提交 revision 包含三组派生数据：

- 每服务健康汇总 `health_summary`。
- 每个实例的权重行 `weight_allocations`。只有 `HEALTHY` 属于可用集合；可用实例的 `sharePpm` 使用最大余数法归一化，总和严格为 `1,000,000`。不可用实例保留行但份额为 0。
- 每个实例的 `probe_plans`，显式引用端点、探测参数、优先级、配置指纹与租约到期/剩余时间。

注册字段（端点、权重、TTL、探测参数、阈值、优先级）变化会改变配置指纹，将实例健康重置为 `UNKNOWN`；配置未变则保留健康与租约。新的派生 revision 先以 `STAGING` 完整写入，随后活动实例与 `current_derived_revision` 指针在同一事务中切换。读者使用 `current_derived_revision`，不会看到“实例已 UNHEALTHY 但权重仍包含份额”的中间态。

## 三重预算调度

轮次任务排除 `DRAINING`，先按服务名、实例标识稳定排序。调度器每一轮扫描都按该稳定次序选择当前同时满足全局名额和每服务名额的任务，阻止单个慢服务占满全局名额。

有界等待队列容量在启动前确定性判定：

```text
maxActive = min(globalConcurrency, Σ_service min(serviceTaskCount, perServiceConcurrency))
pendingAtAdmission = taskCount - maxActive
if pendingAtAdmission > waitQueueCapacity => E_PROBE_QUEUE_FULL
```

预算从轮次开始计时。已启动探测的 context 同时受实例超时与轮次 deadline 约束；预算耗尽后不再启动任务。延后输出按优先级降序、服务名/实例标识升序稳定排序，并包含 `reason`、`admissionIndex`、`consumedBudgetMs`、`remainingBudgetMs`，因此结果可复现、可解释。

## 持久化与恢复协议

SQLite 使用 WAL 与 `synchronous=FULL`。所有写操作先提交 `intents` 记录，再进入暂存/激活阶段。关键持久化表包括：

`snapshots`、`snapshot_instances`、`instances`、`leases`、`derived_revisions`、`health_summary`、`weight_allocations`、`probe_plans`、`probe_rounds`、`probe_results`、`intents`、`requests`、`recovery_reports`。

注册/增量提交：

1. 写 `RUNNING` 意图和请求记录。
2. 提交 `STAGING` 快照、快照实例和 `STAGING` 派生 revision。
3. 单事务激活活动实例、租约、快照状态、派生指针、请求结果和意图完成态。

探测轮次：

1. 意图、`RUNNING` 轮次和全部 `PENDING` 探测任务先落盘。
2. 每个真实探测完成后单独持久化结果，但 `committed=0`，不触碰健康状态。
3. 派生 revision 先完整暂存，随后在一个事务中更新实例健康、发布派生指针、标记结果 `COMMITTED` 并完成轮次。

启动时扫描所有 `RUNNING` 意图：未提交探测结果重置为 `PENDING` 并清空结果载荷；删除活动指针之后的暂存派生和暂存快照；校正不完整指针；写入恢复报告。报告只统计当时磁盘上真实存在、被重置或删除的行。

## 确定性样本和诊断

`sample.Generate` 使用固定 seed 生成 3 个服务、9 个实例，覆盖 HTTP 正常/坏状态/坏标记/慢响应、TCP 成功/拒绝与 LEASE。回环端口由 `127.0.0.1:0` 动态分配以便并行运行；除端口地址外，权重、TTL、阈值、优先级、顺序和摘要均对同一 seed/version 稳定。

`verify` 会启动真实回环端点、临时 SQLite 数据库，并运行 18 项检查。强制终止检查通过重新执行同一个 CLI 二进制并发送 `SIGKILL`，随后由父进程重新打开数据库，核验活动版本、暂存清理和探针回滚。

实际结果见 `docs/VERIFICATION.md`。完整 JSON 保留在 `artifacts/verification-report.json`。

## 边界

- 服务只监听本地回环，不含认证/授权，不应暴露到不可信网络。
- 单进程内由一个写者串行化；SQLite 适合本地实验室，不是跨主机注册中心。
- 回环端口动态，样本字节仅在同一组 fixture 地址下完全一致；固定 seed 的注册语义稳定。
- WAL 文件、二进制与本地数据库已在 `.gitignore` 中忽略。
