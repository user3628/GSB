# 权重账本工作台实现说明

本项目以「分流账本」为核心，使用 Go 标准库 HTTP/SQLite 实现。SQLite 驱动为纯 Go 的 `modernc.org/sqlite`，`CGO_ENABLED=0` 可构建，不依赖 Docker。

## 快速运行

```bash
go version                 # 需要 Go 1.22+
go mod tidy
go test ./...
# 若 proxy.golang.org 超时，可使用：
# GOPROXY=https://goproxy.cn,direct go mod tidy

bash scripts/demo.sh
bash scripts/recovery-demo.sh
```

固定样本生成器：

```bash
SAMPLE_DIR="$(mktemp -d)/samples"
go run ./cmd/gen-samples --dir "$SAMPLE_DIR" --seed 20261125
```

本地回环探测服务：

```bash
go run ./cmd/probe-target --addr 127.0.0.1:18082
# 另一个终端：
curl 'http://127.0.0.1:18082/probe?route_key=route-probe&mode=ok&owner_version=owner'
```

主演示会在临时目录构建三个二进制、启动随机端口的工作台和探测服务，并在结束时保留临时目录路径，便于用 `sqlite3` 复查。

## HTTP 接口

| 方法与路径 | 用途 |
| --- | --- |
| `POST /v1/snapshots` | 提交单调权重快照，校验摘要、版本连号和千分比总和 |
| `POST /v1/entries` | 整批校验并提交账本条目 |
| `POST /v1/migrations` | 提交带 `base_version` 的迁移片段，识别陈旧基础版本和迁移竞态 |
| `POST /v1/rebuild` | 单写者串行重建指定路由或全部路由的守恒视图 |
| `GET /v1/ledger/{route_key}` | 查询原始版本账本和按命中版本的累计值 |
| `GET /v1/conservation/{route_key}` | 查询守恒视图；支持 `observe_version`、`as_of_ms`、`tolerance`、`strict=1` |
| `POST /v1/probes/single` | 提交单路由探测轮次并返回统一错误体 |
| `POST /v1/probes/round` | 提交多路由真实回环探测批次 |
| `GET /v1/probes/batches/{batch_id}` | 从 SQLite 读取持久化探测批次 |
| `GET /v1/recovery` | 读取最近一次崩溃恢复报告 |
| `POST /v1/admin/crash` | 写入意图/staging 事实后模拟 SIGKILL；`no_kill:true` 可只造数据 |

写接口支持 JSON 字段 `commit_id` 或 `Idempotency-Key` 请求头。相同提交标识和相同内容只执行一次；相同标识、不同内容返回 `E_CONFLICTING_LEDGER`。

## 固定确定性参数

- 种子：`20261125`
- 固定基准时间：`1795564800000`（2026-11-25T00:00:00Z）
- `route-balanced`：stable/canary = 700/300，累计承接权重 7000/3000，共 100 条。
- `route-migrate`：基线 v1/v2 = 800/200；迁移 300 后过渡期为 500/500，样本累计承接 5000/5000。
- 固定观察版本：3。
- `route-balanced` 在 `as_of_ms=1795564830000` 的视图哈希：
  `1d0861bb85dab283a92e6c4a842849c364849a554f33774681f7b7acbae31d31`
- `route-migrate` 过渡期 `as_of_ms=1795564880000` 的视图哈希：
  `c449af2591a61d9d8c5b2e7282188b35296b8c9ef1791bd06f07a0b00e4261d8`

视图哈希覆盖除 `view_hash` 外的完整规范化视图 JSON，因此相同 SQLite 事实和相同观察时间会得到逐位一致的结果。

## 比例与舍入

- 比例统一保留 6 位小数。
- 舍入方式：`ROUND_HALF_UP`。
- `expected_cumulative_rounding_error` / `actual_cumulative_rounding_error` 为各版本舍入比例之和减 1。
- 守恒判定使用 `math/big.Rat` 精确比较，不依赖浮点误差。
- `expected_ratio` 对照当前 `as_of_ms` 的生效权重；`declared_expected_ratio` 保留按每条账本自带候选权重加权得到的审计值。

## 迁移、竞态与陈旧读

- `ratio_permille` 表示过渡期内从源版本权重中切给目标版本的千分点数。
- `[start_at_ms, start_at_ms + transition_ms)` 为过渡期：源权重减迁移量，目标权重加迁移量。
- 到达 `end_at_ms` 后，源版本生效权重归零，剩余权重全部归目标版本。
- 账本条目始终保留原始 `hit_version`，迁移不会重写历史记录。
- 同路由两个已提交迁移的半开区间重叠，且 `ratio_permille` 之和不等于 1000 时，返回 `E_MIGRATION_RACE`，上下文包含双方 ID、比例和 `[start_at_ms,end_at_ms)`。
- 请求的 `observe_version` 大于 SQLite 中完整提交的 `view_version` 时，返回 `E_STALE_READ`。

## 探测与连接释放

`Workbench.PerformProbe` 对每个路由发起真实 HTTP 请求，并使用独立 `context.WithTimeout`。并发名额来自容量为 `ProbeConcurrency` 的 buffered channel，返回结果前释放。

超时路径会取消请求上下文并显式关闭被自定义 Dialer 跟踪的底层 `net.Conn`；测试通过 `WaitProbeConnectionsClosed` 观察连接关闭 channel。探测服务支持：

- `mode=ok`：正常归属响应；
- `mode=timeout`：阻塞到客户端超时；
- `mode=mismatch`：返回错误归属标识；
- `mode=late_response&delay_ms=...`：在硬超时内但超过 `late_threshold_ms` 返回，产生独立的 `E_PROBE_LATE_RESPONSE`；
- 连接拒绝使用刚释放的回环端口或请求中的 `endpoint`。

## SQLite 血缘与恢复

主要表：

- `intents` / `idempotency`：持久化意图、提交内容哈希和复用结果；
- `ledger_entries`：原始条目、状态（`staged`/`committed`/`discarded`）、`view_version`、`snapshot_version`；
- `snapshots` / `route_weights` / `migrations`：权重事实和版本化生效权重；
- `route_views`：按提交版本、路由和 `as_of_ms` 持久化的守恒 JSON 与哈希；
- `derived_lineage`：条目数、累计权重、守恒哈希、快照版本、探测批次和有效状态；
- `probe_batches` / `probe_results`：探测批次、端点、结果和连接/名额释放事实；
- `recovery_reports`：最近恢复报告。

正常提交流程先独立提交 intent，再在一个 SQLite 事务中完成版本号推进、领域写入、级联失效、视图重建和幂等结果落盘。事务失败整体回滚。

重启时 `Store.Recover` 只扫描 SQLite：

- `staged` 账本条目改为 `discarded`，不进入任何查询；
- 未完成 migration intent 落为 `status=pending` 的待生效迁移；
- 未完成 probe batch 标记为 `interrupted`；
- 恢复报告中的 ID、计数全部来自已落盘行。

## 错误码

统一错误体：

```json
{"ok":false,"error":{"code":"E_...","message":"...","context":{}}}
```

已覆盖：`E_LEDGER_INVALID`、`E_WEIGHT_IMBALANCE`、`E_STALE_BASE`、`E_MIGRATION_RACE`、`E_STALE_READ`、`E_PROBE_TIMEOUT`、`E_CONFLICTING_LEDGER`、`E_SNAPSHOT_INVALID`、`E_DIGEST_MISMATCH`、`E_VERSION_GAP`、`E_MIGRATION_RATIO_INVALID`、`E_PROBE_REFUSED`、`E_PROBE_MISMATCH`、`E_PROBE_LATE_RESPONSE`、`E_REBUILD_FAILED`。

## 代码地图

- `model.go` / `normalize.go`：领域模型、摘要和整批校验；
- `decimal.go`：定点比例和精确容差；
- `store.go`：SQLite schema、meta 和崩溃恢复；
- `workbench.go`：单写者、in-flight 合并、幂等提交模板；
- `ledger.go` / `view.go`：快照、条目、迁移、守恒视图和原子重建；
- `probe.go` / `probe_round.go`：真实 HTTP 探测、连接跟踪、探测批次；
- `server.go`：HTTP 路由和统一 JSON 错误；
- `sample/sample.go`：固定种子样本；
- `probetarget/probetarget.go`：随机端口回环探测服务；
- `scripts/demo.sh` / `scripts/recovery-demo.sh`：本地复核脚本。
