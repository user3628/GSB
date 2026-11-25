# driftengine

`driftengine` 是一个纯 Go、无 CGO 的声明式配置漂移收敛裁决服务。它读取固定种子生成的 Kubernetes 风格对象与策略快照，完成漂移判定、依赖分层、预算拆批、回环 HTTP 写回/回读、乐观版本提交、并发写回控制，以及基于 SQLite 意图表的强制终止恢复。

不依赖 Docker；SQLite 使用 `modernc.org/sqlite` 纯 Go 驱动，可交叉构建 Windows amd64 可执行文件。

## 实际验证结果

固定种子：`245245`。标准样本包含 24 个对象，快照摘要为：

```text
sha256:a67f17dd1a053d87297f16b6acfa976a5645e6c0a8cf4ec049589004b837d78d
```

已在 2026-11-25 完成以下验证：

- Linux/WSL2：`go test -count=1 ./...`、`go vet ./...`、`./scripts/verify.sh` 全部通过。
- Linux 验收器：9/9 项通过，报告见 `artifacts/verification-report.json`。
- Windows：从 Linux 交叉构建 `GOOS=windows GOARCH=amd64` exe 后，通过本机 Windows PowerShell 5.1 实际执行 `drift-engine.exe verify`；9/9 项通过，报告见 `artifacts/windows-verification-report.json`，其中恢复检查的 `platform` 为 `windows`。
- Windows 环境的 `PATH` 中没有 `go.exe`，因此未在 Windows 上实际运行 `scripts\verify.ps1` 的 `go test` 段；已用 PowerShell Parser 校验脚本语法，并实际运行同一 exe 的内置验收。

9 项验收覆盖：漂移类别/容差边界、依赖环与环外分层、2000 对象确定性大图、真实 `127.0.0.1` 写回回读、并发增量 `E_STALE_BASE`、非法规格保留旧版本、队列满/超时/名额释放、回读不一致重试耗尽、真实 `Kill()` 后 SQLite 恢复。

## Windows 快速开始

要求 Go 1.22+。在 PowerShell 中：

```powershell
go version
go mod tidy
.\scripts\verify.ps1
```

也可以手动执行：

```powershell
go test ./...
go vet ./...
go build -o bin\drift-engine.exe .\cmd\drift-engine
.\bin\drift-engine.exe verify `
  -exe "$PWD\bin\drift-engine.exe" `
  -workdir "$PWD\.verify-work\manual" `
  -report "$PWD\artifacts\verification-report.json"
```

如果本机禁止脚本，可使用：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

启动回环测试端点和引擎 API：

```powershell
.\bin\drift-engine.exe test-endpoint -addr 127.0.0.1:18081 -state samples\endpoint-state.json
.\bin\drift-engine.exe serve -addr 127.0.0.1:18080 -endpoint http://127.0.0.1:18081 -db data\drift-engine.db
```

提交标准快照并推进一轮收敛：

```powershell
curl.exe -X POST http://127.0.0.1:18080/v1/snapshots `
  -H "Content-Type: application/json" `
  --data-binary "@samples\snapshot-v1.json"

curl.exe -X POST http://127.0.0.1:18080/v1/rounds `
  -H "Content-Type: application/json" -d "{}"

curl.exe http://127.0.0.1:18080/v1/summary
```

Linux/macOS 等价入口为 `./scripts/verify.sh`。

## CLI

`drift-engine <command> [flags]`

- `gen-samples -dir samples -seed 245245`：生成固定种子 JSON 样本。
- `serve -addr 127.0.0.1:18080 -endpoint ... -db ...`：启动引擎 HTTP API。
- `test-endpoint -addr 127.0.0.1:18081 -state ...`：启动仅允许回环客户端的写回/回读端点。
- `verify -exe ... -workdir ... -report ...`：运行 9 项可重复本地验收。
- `recover -db <path>`：扫描 SQLite 中未完成意图并输出 JSON 恢复报告。
- `crash-worker -db <path> -scenario write|snapshot-intent`：强制终止恢复验收使用的内部子进程。

## HTTP API

- `GET /healthz`
- `GET /v1/summary`：当前完整版本的汇总视图。
- `GET /v1/objects`、`GET /v1/objects/{id}`：对象规格、漂移报告和持久化收敛状态。
- `GET /v1/drift`：当前版本全部漂移明细。
- `GET /v1/plan`：依赖分层和批次计划；存在依赖环时返回 `E_DEPENDENCY_CYCLE` 详情，同时保留环外计划。
- `GET /v1/batches`：批次定义、依赖依据、预算占用和批次状态。
- `POST /v1/batches/{id}/run`、`POST /v1/batches/{id}/trigger`：幂等触发同一批次。
- `POST /v1/snapshots`：提交整份快照。
- `POST /v1/increments`：提交带 `base_version` 的局部增量。
- `POST /v1/rounds`：按依赖层推进一整轮写回。

## 漂移判定

稳定主类别优先级为：

```text
source_conflict -> field_missing -> field_extra -> value_offset
```

一个对象可以保留全部命中类别和逐字段差异，`primary_category` 只是最高优先级类别。

数值比较不使用裸 float 容差判断。JSON 数字经 `encoding/json` 的 `UseNumber` 保留，再由 `internal/fixednum` 转为 `big.Int` 分子/分母或 `big.Rat`，按相对容差做精确分数比较：

```text
|actual - desired| <= tolerance * |desired|
```

- `tolerance_boundary=inclusive`（默认）：相等边界视为容差内。
- `tolerance_boundary=exclusive`：相等边界仍判为漂移。
- 固定样本中 desired=200、actual=202、tolerance=1/100 正好落在边界：inclusive 不漂移，exclusive 漂移。

## 依赖与批次

`internal/batch` 先使用 Tarjan SCC 找环，再对环外节点做最长依赖路径分层：

- 被依赖对象的批次号严格小于依赖者。
- 环 ID 通过 `E_DEPENDENCY_CYCLE` 返回；环外对象仍输出确定的分层批次。
- 每层按对象 ID 排序，保证同版本结果可复现。
- 批次同时受 `max_batch_size` 和 `max_batch_budget_units` 约束；容量不足时拆出新批次，不丢弃对象。
- 单个对象预算超过上限时仍保留在单对象批次中，并设置 `budget_overflow=true`，避免无法推进。
- 每个批次输出对象引用、字段摘要、依赖边 ID/摘要、依赖批次号和 CPU/内存/预算占用。

标准样本为 4 层、15 个批次；2000 对象大样本当前生成 48 个批次，重复规划的规范化 JSON 完全一致。

## 写回与回读

`internal/writeback` 对 PUT 写回和 GET 回读分别创建独立的 `context.WithTimeout`，HTTP transport 使用 `http.DefaultTransport.Clone()`、关闭 keep-alive，并在超时路径调用 `CloseIdleConnections()` 释放连接。调度器并发名额在作业结束时释放，因此超时后后续批次仍可运行。

每个对象的收敛条件是：

1. PUT 期望字段到 `http://127.0.0.1/loopback/v1/objects/{id}/fields`。
2. GET 回读实际字段和来源标记。
3. 用同一套精确漂移引擎重新判定。
4. 只有 `in_sync=true` 才标记 `converged`。
5. 回读差异写入状态和 `write_attempt`；按 `max_retries` 重试，耗尽后返回 `E_RETRY_EXHAUSTED`。

测试端点在监听器和 HTTP 中间件两层限制 loopback；验收中的慢端点还验证了服务端能观察到客户端取消。

## 版本、并发与幂等

- 整份快照必须携带单调递增 `version` 和 `generated_at`，并校验整体 digest。
- JSON 解析、重复 ID、缺失标识、非法策略或不存在的依赖引用均返回 `E_SPEC_INVALID`；解析发生在视图替换前，旧版本继续可读。
- 增量只包含变化对象并携带 `base_version`。基础版本不等于当前版本时返回 `E_STALE_BASE`，不移动当前版本。
- 两个并发提交同一基础版本的增量时，单写者锁串行化：第一个成功并把版本从 1 推进到 2，第二个看到旧 base，返回 `E_STALE_BASE`。
- 新快照的漂移报告、批次计划和汇总在同一个 SQLite 提交束中写入 `committed_versions`、`object_state`、`batch_state`，成功后才通过 `atomic.Pointer[View]` 切换读视图，避免新旧结论混合。
- 批次触发按 batch ID 合并进行中的运行：重复请求等待同一结果；已完成批次直接复用持久化结果。若首次内容哈希与后续非空 `request_hash` 不一致，返回 `E_CONFLICTING_REQUEST`。
- `max_parallel_batches` 控制真实并行批数，`write_queue_size` 控制有界等待队列；队列满立即返回 `E_WRITE_QUEUE_FULL`。
- 单轮 `round_time_budget_ms` 形成总 `context` 截止时间；单对象 HTTP 调用仍有独立 `write_timeout_ms`。

## SQLite 意图与崩溃恢复

数据库路径可由 `-db` 指定；Windows 下 DSN 会转换为显式 `file:///C:/...` 形式并启用 WAL、busy timeout 和外键。

关键表：

- `committed_versions`：每个完整版本的快照、漂移、计划、汇总 JSON。
- `intent`：`snapshot_switch`、`organize_batch`、`write_object` 等意图。
- `object_state`、`batch_state`：可查询的推进状态。
- `write_attempt`：每次写错误、回读错误、回读差异或收敛事实。
- `recovery_report`：最近恢复报告。

写入顺序遵循 write-ahead intent：

- 快照切换：先写 `snapshot-switch:vN`（`prepared`），再原子提交完整版本，最后标记 `committed`。
- 批次触发：先写 `organize-batch:vN:<batchID>`（`running`），再持久化 running 状态并进入调度器；终态写 `completed` 或错误码。
- 对象写回：先写 `write-object:vN:<batchID>:<objectID>`（`running`），再发 HTTP 请求和记录尝试。

强杀进程后执行 `drift-engine recover -db <path>` 或调用 `engine.Recover`：

- 未完成的 `write_object` 意图重置为 `pending_write`，并基于最后已提交快照重新计算期望与当前规格差异。
- 未完成的 `snapshot_switch` 标记为 `aborted`；只有 `committed_versions` 中的当前版本仍被视为可用快照。
- 未完成的批次组织意图标记为 `aborted`，重启后可用同一 batch ID 重新触发。
- 报告设置 `exit_report_claimed=false` 和 `external_state_reconciled=false`。因为被 kill 时 HTTP 请求可能已被远端接受，SQLite 不能证明外部端点最终状态；必须重新回读才能宣称收敛，也不承诺一定存在退出报告。

内置崩溃检查会启动真实子进程、等待 `READY`、调用 `Process.Kill()`，再重新打开 SQLite 校验上述事实。

## 代码结构

- `cmd/drift-engine`：可执行入口。
- `internal/cli`：CLI 命令和验收报告输出。
- `internal/spec`：JSON 规格、归一化、校验、摘要。
- `internal/fixednum`：精确小数/分数容差比较。
- `internal/drift`：四类漂移和稳定优先级。
- `internal/batch`：Tarjan 环检测、依赖分层、数量/预算拆批。
- `internal/writeback`：真实 HTTP PUT/GET、独立超时和连接释放。
- `internal/loopendpoint`：仅 loopback 的写回/回读测试端点。
- `internal/store`：纯 Go SQLite schema、版本束、意图和恢复持久化。
- `internal/engine`：单写者提交、原子读视图、调度器、幂等触发、重试和恢复。
- `internal/api`：HTTP JSON API。
- `internal/sample`：固定种子样本和 `go:embed` JSON 资产。
- `internal/verify`：9 项同名验收检查。
- `samples`：可直接检查和提交的 JSON 样本。
- `scripts/verify.sh`、`scripts/verify.ps1`：跨平台验证入口。
- `artifacts/verification-report.json`：最近 Linux 验收报告。
- `artifacts/windows-verification-report.json`：最近真实 Windows exe 验收报告。

## 样本重新生成

```bash
go run ./cmd/drift-engine gen-samples -dir samples -seed 245245
```

`internal/sample/assets` 中嵌入了验收所需样本；修改 `samples/*.json` 后需同步嵌入资产。当前大样本中的依赖边均指向存在的对象，因此能通过完整依赖引用校验。
