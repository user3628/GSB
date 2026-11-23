# Partition Workbench

面向本地微服务测试环境的分区有序消费、位点租约与事务可见性裁决工作台。应用使用 Go + SQLite，无容器、无外部账号；诊断页、事务协调测试端点、消费者 HTTP API 都在本机运行。

## 快速开始（Windows）

要求 Go 1.22 或更高版本，并使用纯 Go SQLite 驱动（依赖锁定为 `modernc.org/sqlite v1.34.5`），无需 CGO、容器或外部服务。

```powershell
go version
go mod download
go test ./...
powershell -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

演示脚本会构建本机二进制，启动真实 HTTP 服务，执行 12 项检查，强杀进程并重启验证恢复，最后写出：

- `artifacts/demo-verification.json`
- `artifacts/demo-server.log`

启动常驻诊断工作台：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run.ps1
```

打开 <http://127.0.0.1:8080/>。页面每 2 秒读取一次 SQLite 只读快照，展示：

- 每分区 `next_seq`、事务可消费前沿、重放有效前沿、消费位点、租约持有者/版本/到期时间、待决事务和积压；
- 事务提交/回滚后的消息可见性与前沿推进；
- 调度器队列、全局在途数、预算轮次、逐任务耗时、跳过原因和剩余预算；
- 恢复报告、异常注入参数及真实观测记录。

Linux/macOS 可用于本地复核：

```bash
go test -race ./...
go run ./cmd/demo -port 18080
go run . -addr 127.0.0.1:8080 -db data/workbench.db -seed samples/demo.json -allow-crash
```

## 核心语义

每个分区维护三个不同概念：

- `visible_frontier`：事务可见性的排他前沿。序号小于它的事务结论已确定（`VISIBLE`、`ROLLED_BACK` 等），不是“已消费位点”。
- `effective_frontier`：诊断视图中的可消费前沿。重放期间被钳制到 `replay_limit`，所以新到消息不会越过重放屏障被拉取。
- `consume_cursor`：消费者下一次应检查的位点。提交推进它；回退把重放区间内的 `CONSUMED` 恢复为 `VISIBLE` 并将它移回起点。

拉取严格从 `consume_cursor` 开始，遇到 `PENDING_INVISIBLE` 立即停止，绝不跳到后继消息。终态 `ROLLED_BACK`/`DEAD_LETTERED` 会被跳过，但不会重复推进可见前沿。

租约状态为 `FREE -> LEASED -> EXPIRED -> LEASED`，提交走 `LEASED -> FREE`。每次状态迁移递增 `lease_version`；拉取/提交/回退绑定版本，旧版本返回 `E_LEASE_STALE`。空闲超时由后台 reaper 与操作前检查共同落盘释放；重获租约从原 `consume_cursor` 继续。

## 事务与协调器

事务消息先以 `PENDING_INVISIBLE` 写入，并创建 `txn_visibility(PENDING)`。裁决通过真实本机 TCP HTTP 请求访问：

```text
GET /test/txn/{txn_id}?mode=commit|rollback|refuse|hang|conflict
```

客户端查询使用独立超时并关闭连接；`hang` 返回 `E_TXN_TIMEOUT`，连接拒绝/无响应断开返回 `E_COORD_REFUSED`。提交在同一 SQLite 事务内完成：

```text
message PENDING_INVISIBLE -> VISIBLE
txn_visibility PENDING -> COMMITTED
recompute visible_frontier
intent + request idempotency result committed
```

回滚同理写为 `ROLLED_BACK`。已裁决事务再次请求默认直接复用持久化结论；`force_recheck=true` 时若协调器给出相反结论，返回 `E_TXN_CONFLICT` 且不覆盖原状态。

## 调度与预算

调度器支持 `txn` 和 `lease_expire` 任务：

- 全局并发上限控制同时在途任务数；
- 每分区在途租约任务上限独立限制；
- 有界等待队列满返回 `E_QUEUE_FULL`；
- 同一分区同一事务去重，不会并行查询；
- 排序按分区优先级、分区轮转、入队顺序确定；
- 每轮使用时间预算；预算不足时剩余任务保持 `QUEUED`，轨迹记录 `SKIPPED`、`round_budget_exhausted`、预算前后值与确定性顺序；
- 已经开始的任务不会被预算降级取消，因此不会无谓中断在途租约。

## 意图与恢复

拉取、提交、回退、事务裁决（以及发布等写操作）先单独提交 `PREPARED` 意图与 `RUNNING` 请求，再执行最终事务。最终事务同时提交领域状态、意图完成和请求结果。因此进程在意图后被杀时，领域变更不可能“半提交”。

重启时扫描 `PREPARED` 意图并只依据落盘事实生成报告：

- 拉取意图：消息保持可消费，租约未被该意图获取；
- 提交意图：消费位点保持旧值；
- 回退意图：若存在对应活跃重放则中止并恢复 `replay_limit`；
- 事务意图：消息/事务保持 `PENDING`；
- 对应 `RUNNING` 请求标记为失败并返回 `E_INTENT_RECOVERED`。

可通过 `POST /api/debug/crash-after-intent`（启动时需 `-allow-crash`）真实执行“写入意图后 `os.Exit`”演示。

## 业务码

每个异常类别有独立码，主要包括：

| 码 | 含义 |
|---|---|
| `E_ROUTE_INVALID` | 路由规则或分区号非法 |
| `E_CONFLICTING_MESSAGE` | 消息标识复用但内容不同 |
| `E_CONFLICTING_REQUEST` | 请求标识复用但请求指纹不同 |
| `E_INVALID_TRANSITION` | 消息/租约/重放状态迁移非法 |
| `E_LEASE_HELD` | 分区租约被其他消费者持有 |
| `E_LEASE_STALE` | 租约版本已过期 |
| `E_TXN_TIMEOUT` | 协调查询超时且连接已关闭 |
| `E_COORD_REFUSED` | 协调连接被拒绝/断开 |
| `E_TXN_CONFLICT` | 协调结论与已落盘结论矛盾 |
| `E_REPLAY_EXPIRED` | 回退目标早于最早保留序号 |
| `E_REPLAY_INVALID` | 回退目标尚不可见或不属于可重放区间 |
| `E_QUEUE_FULL` | 有界调度队列已满 |
| `E_INTENT_RECOVERED` | 请求因进程终止被恢复流程中止 |

## 数据与样本

SQLite 表包括 `topics`、`partitions`、`messages`、`consumer_offsets`、`txn_visibility`、`state_transitions`、`requests`、`intents`、`injections`、`injection_observations`、`scheduler_rounds` 与 `recovery_reports`。`consumer_offsets` 与分区状态在每次写事务内同步更新。

固定样本位于 `samples/demo.json`：`orders` 为显式分区，含提交、回滚、超时、矛盾事务；`payments` 为热点/长尾多分区待决队列；`audit` 使用哈希路由。`samples/scenarios.json` 固定列出协调、租约、两轮预算与崩溃恢复场景参数。

## 验证结果

当前实现的实际验证：

```text
go test ./...          PASS
go test -race ./...    PASS
go run ./cmd/demo      12/12 checks PASS
```

逐项结果见 `artifacts/demo-verification.json`，包含顺序消费、租约冲突、事务提交/回滚、超时重试、矛盾裁决、重放屏障、租约抢占、队列满、预算降级与强杀恢复。详细设计见 `docs/DESIGN.md`，接口说明见 `docs/API.md`。

## 边界

- 这是本地测试工作台，不含鉴权、多节点协调或生产级 SQLite 高可用。
- 协调测试端点与 API 在同一进程，但事务查询始终经过真实 loopback TCP HTTP 连接。
- 所有时间、端口、样本、数据库路径都在本机；不依赖容器、云账号或线上服务。
