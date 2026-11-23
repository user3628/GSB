# 网关后端权重调度与并行探测预算裁决工作台

面向本地回环环境的网关后端工作台：在全局并发、每实例并发与每轮时间预算三重约束下，
按确定性权重把客户端请求轨迹真实调度到后端实例，同时并行探测实例可用性并驱动
权重归一化与可用区亲和决策。所有状态写入纯 Go SQLite（`modernc.org/sqlite`，无 CGO），
通过意图日志两阶段提交支持强杀后恢复。

## 1. 环境与构建

- Windows 10+，Go 1.22+（开发验证使用 Go 1.27，`go.mod` 声明 `go 1.22`）。
- 纯 Go SQLite 驱动，`CGO_ENABLED=0` 即可构建；不使用 Docker，不访问外部网络。
- 依赖固定在 `go.mod` / `go.sum`，拉取可用 `GOPROXY=https://goproxy.cn,direct`。

```powershell
# Windows
$env:CGO_ENABLED="0"
go build ./...
go test ./...
.\scripts\demo.ps1
```

```bash
# Linux/macOS 等价验证
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go test ./...
./scripts/demo.sh
```

## 2. 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/model` | 实例/请求模型、样本校验、五阶段状态机、FNV-1a 确定性哈希、按区权重归一化与区间选择 |
| `internal/probe` | 真实 HTTP/TCP 探测执行器，独立超时、连接真实关闭、拒绝/超时错误分类 |
| `internal/store` | SQLite schema、意图日志、两阶段提交、强杀恢复裁决与全部数据访问 |
| `internal/engine` | 探测轮次、人工摘除/恢复、阶段迁移与快照原子推进 |
| `internal/scheduler` | 三重并发约束、有界队列、轮次预算、单飞复用与真实上游派发 |
| `internal/api` | 本地 HTTP API 与统一 JSON 业务错误响应 |
| `cmd/server` | 工作台服务（启动即执行恢复） |
| `cmd/testupstream` | 回环测试端点：`/healthz`、`/upstream`、`/slow` |
| `cmd/demo` | 可重复端到端演示编排器（跨平台，纯 Go 启停子进程） |
| `samples` | 确定性实例清单、请求轨迹、亲和配置与合并样本 |
| `data` | SQLite、WAL、意图日志与演示输出（运行期生成，已 gitignore） |

## 3. 样本与回环端点

`samples/sample.json`（由 `instances.json`、`trace.json`、`affinity.json` 合并）声明：

- `inst-a1`(19091)、`inst-b1`(19092)：正常 HTTP 端点，状态码 200 且含标记 `UPSTREAM-OK`。
- `inst-timeout`(19093)：`/slow` 5 秒不响应，探测超时 300ms -> `E_PROBE_TIMEOUT`。
- `inst-refused`(19099)：端口不监听，TCP 连接拒绝 -> `E_PROBE_REFUSED`。
- 8 条确定性请求轨迹，客户端标识唯一；亲和比例 `zone-a=500, zone-b=500`。

校验规则（任一失败返回 `E_INSTANCE_INVALID`，整份样本不生效，旧数据保留）：
端口 1-65535、权重 1-1000 正整数、可用区非空、实例 ID 不重复、客户端 ID 唯一、
探测参数完整、亲和比例合计严格为 1000。

## 4. 可用性阶段机

五阶段与合法迁移（同阶段重复提交视为非法，返回 `E_INVALID_TRANSITION` 且不改已提交状态）：

```
UNKNOWN   -> HEALTHY | UNHEALTHY | DRAINING
HEALTHY   -> SUSPECT | DRAINING
SUSPECT   -> HEALTHY | UNHEALTHY | DRAINING
UNHEALTHY -> HEALTHY | SUSPECT | DRAINING
DRAINING  -> UNKNOWN            # 仅人工恢复入口
```

- 连续成功/失败阈值由每个实例的探测参数声明，计数随阶段迁移重置。
- `DRAINING` 是人工摘除入口；`DRAINING` 与 `UNHEALTHY` 不接收新请求，
  其权重从归一化分配中移除，并在 `instance_state.reason` 记录摘除/迁移原因。
- `HEALTHY` 与 `SUSPECT` 为可调度阶段（SUSPECT 仍保留有限流量以便自愈确认）。

## 5. 确定性权重调度

- 哈希：`FNV-1a64(clientID + NUL + requestSeq)`，同输入在任何进程结果一致。
- 选区：`hash % 1000` 映射到亲和比例累积区间，得到首选可用区。
- 选区：区内权重先按原始权重做千分比归一化（最大余数法保证合计严格 1000，
  ID 排序打破平手），再用同一哈希落入实例权重区间。
- 回退：首选区无可用实例时，按可用区名确定性顺序逐区回退，决策中记录
  `fallback=true` 与 `fallback_reason`；全区无候选返回 `E_NO_CANDIDATE`。
- 相同快照版本 + 相同可用集合 + 相同请求，调度结果必然相同（有单元测试固化）。

## 6. 并行探测与预算

- 探测并发由 `WB_PROBE_CONCURRENCY` 信号量限制，每个探测使用独立 `context.WithTimeout`。
- HTTP 探测同时校验期望状态码与响应体标记；TCP 探测验证可连接性。
- 超时触发后 `context` 立即关闭底层连接（`DisableKeepAlives`、读完即 `Body.Close`），
  goroutine 退出时 `defer` 释放信号量名额，不存在“以等待冒充超时”。
- 错误分类：连接拒绝 `E_PROBE_REFUSED`、超时 `E_PROBE_TIMEOUT`、其他失败 `E_PROBE_FAILED`。

## 7. 并行调度器三重约束

1. 全局并发：全局信号量，容量 `WB_GLOBAL`。
2. 每实例并发：每实例独立信号量，容量 `WB_PER_INSTANCE`。
3. 轮次时间预算：`context.WithTimeout(WB_BUDGET_MS)`，预算耗尽后未启动任务计入
   `skipped_budget`，已发起请求携带同一预算 context。
- 计划阶段按时间戳顺序读取、再按首选可用区确定性轮转打散。
- 有界等待队列容量 `WB_QUEUE`，超出立即返回 `E_DISPATCH_QUEUE_FULL`（HTTP 429）。
- 同一轮并发触发时，基于每轮互斥锁实现单飞：仅一个实际执行，其余等待并复用结果，
  `dispatches(round_id, request_seq)` 唯一索引在存储层兜底防重复派发。
- 派发为真实 `POST http://127.0.0.1:<port>/upstream`，请求体承载请求字节数。

## 8. 跨模块一致性与持久化

SQLite 表：`instances`、`instance_state`、`requests`、`snapshots`、`rounds`、
`probes`、`dispatches`、`intents`、`recovery_reports`、`meta`。单写连接 +
`_txlock=immediate`，查询只能读到完整提交版本。

- 探测结论或人工摘除导致阶段变化时，在**同一事务**内依次更新：探测历史 ->
  实例阶段与计数 -> 可用集合 -> 按区归一化权重 -> 自增快照版本 -> 轮次状态 ->
  意图 `COMMITTED`。不会出现“实例 UNHEALTHY 但分配仍含其权重”的中间可见状态。
- 所有写操作先在事务内落 `intents` 记录（状态 `OPEN`）再提交数据。
- 调度采用两阶段：
  1. 计划事务提交轮次、派发占位（PENDING）、请求决策，意图保持 `OPEN`；
  2. 全部真实派发结束后，独立事务把该意图推进为 `COMMITTED`，构成完整提交边界。

## 9. 强杀恢复（只基于已落盘事实）

工作台启动时执行 `RecoverOnStartup`：

1. 扫描所有 `OPEN` 意图，标记为 `ABORTED`；
2. 关联探测/调度轮次状态推进为 `ABORTED`；
3. 未完成派发（PENDING/RUNNING）标记 `ROLLED_BACK`，关联请求回滚为 `PENDING`、
   清空 `round_id` 与决策；
4. 权重分配无需反向修补——未提交事务从未对外可见，保持最后一个 `COMMITTED` 快照；
5. 差异明细写入 `recovery_reports`，可通过 `GET /v1/recovery/latest` 查询；
6. 恢复幂等：再次启动无 OPEN 意图时仅记录干净报告。

演示通过 `WB_KILL_AFTER_PLAN=<roundID>` 在计划落盘、部分派发后调用 `os.Exit(97)`，
模拟无清理机会的 SIGKILL。

## 10. HTTP API

统一失败响应：`{"code","message","context","timestamp"}`。

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/samples/load` | 校验并整份装载样本（失败 `E_INSTANCE_INVALID`，旧数据不变） |
| `POST /v1/probe-rounds` | 触发一轮并行探测并原子推进快照 |
| `POST /v1/dispatch-rounds` | 触发一轮调度（队列满返回 `E_DISPATCH_QUEUE_FULL`） |
| `GET  /v1/instances` | 实例阶段、原始/归一化权重与可调度标志 |
| `GET  /v1/instances/{id}` | 单实例阶段、摘除原因、所属区与分配权重 |
| `POST /v1/instances/{id}/drain` | 人工摘除（非法迁移 `E_INVALID_TRANSITION`） |
| `POST /v1/instances/{id}/resume` | DRAINING -> UNKNOWN 人工恢复 |
| `GET  /v1/requests/{seq}` | 按请求查询调度决策依据（哈希、区间、回退原因） |
| `GET  /v1/snapshots/latest` | 当前完整提交的可用集合与权重视图 |
| `GET  /v1/rounds/{id}/probes` | 探测历史（含 `E_PROBE_TIMEOUT`/`E_PROBE_REFUSED`） |
| `GET  /v1/rounds/{id}/dispatches` | 派发结果与上游状态码 |
| `GET  /v1/recovery/latest` | 最近一次恢复报告差异明细 |

业务码覆盖：`E_INSTANCE_INVALID`、`E_PROBE_REFUSED`、`E_PROBE_TIMEOUT`、
`E_DISPATCH_QUEUE_FULL`、`E_INVALID_TRANSITION`（另有 `E_NO_CANDIDATE`、
`E_BUDGET_EXCEEDED`、`E_NOT_FOUND` 等）。

## 11. 实际验证结果

验证环境：Linux 沙箱（Go 1.27.4，`CGO_ENABLED=0`），并交叉编译出
`GOOS=windows GOARCH=amd64` 的三个 PE32+ 可执行文件。

- `go vet ./...` 无告警；`gofmt -l .` 无输出。
- `CGO_ENABLED=0 go test -count=1 ./...` 全部通过：
  - `internal/model`：状态机非法迁移、阈值演进、哈希确定性、归一化剔除、回退选择、样本校验；
  - `internal/probe`：HTTP 成功/标记不匹配、真实超时（~80ms 触发而非等 500ms）、
    超时后二次探测无连接泄漏、TCP 拒绝与成功；
  - `internal/store`：OPEN 意图恢复、轮次中止、请求/派发回滚、快照边界保持、恢复幂等；
  - `internal/scheduler`：真实上游调用、全局并发不超限、队列满业务码、同轮单飞不重复；
  - `internal/api`：统一错误信封、无效样本、真实探测阶段演进、429 队列满、409 非法迁移。
- `cmd/demo` 端到端（日志保存在 `data/demo-run.log`）：
  - 全量并行探测断言 `inst-timeout -> E_PROBE_TIMEOUT`、
    `inst-refused -> E_PROBE_REFUSED`；
  - 摘除 `inst-a1` 后，首选 `zone-a` 的请求确定性回退到 `zone-b/inst-b1` 并记录原因；
  - 有界队列返回 HTTP 429 与 `E_DISPATCH_QUEUE_FULL`；
  - DRAINING 重复摘除返回 HTTP 409 与 `E_INVALID_TRANSITION`；
  - 强杀后重启恢复报告包含：`aborted_rounds=["dispatch-kill"]`、
    `rolled_back_dispatches=[8]`、`rolled_back_requests=[1]`、
    `snapshot_kept_version=5`，回滚请求恢复为 `PENDING` 后可重新完整派发。

## 12. Windows 上手工运行

```powershell
# 终端 A：两个正常端点 + 一个慢端点（19099 不监听即连接拒绝）
go run ./cmd/testupstream -port 19091
go run ./cmd/testupstream -port 19092
go run ./cmd/testupstream -port 19093 -slow-millis 5000
# 终端 B：工作台
$env:CGO_ENABLED="0"
go run ./cmd/server -addr 127.0.0.1:18080 -db data/workbench.db
# 终端 C：调用 API（或直接 go run ./cmd/demo 自动编排）
curl.exe -X POST http://127.0.0.1:18080/v1/samples/load -d "@samples/sample.json"
curl.exe -X POST http://127.0.0.1:18080/v1/probe-rounds -d "{\"round_id\":\"probe-1\"}"
```
