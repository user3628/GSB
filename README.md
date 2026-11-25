# 本地事件总线流式集成中间件

面向 Windows 单机场景的事件总线、分区租约投递、可回拨消费位点、确定性聚合与误差审计实现。项目只使用 Go 标准库和纯 Go SQLite 驱动 `modernc.org/sqlite`，不依赖 Docker、外部消息中间件或网络服务。

## 快速开始

环境要求：Windows 10 及以上、Go 1.22 及以上、`go` 在 `PATH` 中。

```powershell
go mod download
go test ./...
go run ./cmd/demo -seed 20261125 -events 64
```

也可以使用演示脚本：

```powershell
.\scripts\demo.ps1
```

启动常驻本地 HTTP 服务：

```powershell
go run ./cmd/eventbus -addr 127.0.0.1:8080 -db .\tmp\eventbus.sqlite -lease 5s
```

SQLite 文件由调用方指定。测试和演示始终使用临时目录，不写入仓库工作区。

## 交付结构

- `internal/bus`：主题、事件、租约投递、确认、位点回拨、意图日志和恢复。
- `internal/agg`：三种确定性求和、误差界与状态摘要。
- `internal/httpapi`：统一 JSON 的本地 HTTP API。
- `internal/sample`：固定种子、热点/长尾分区、跨数量级度量样本。
- `cmd/eventbus`：常驻 HTTP 服务。
- `cmd/demo`：通过真实 localhost HTTP 跑完整验收场景的演示程序。
- `scripts`：Windows PowerShell 与 Bash 演示入口。

## 核心语义

### 主题、分区与事件

主题声明分区数和消费者组。事件包含 `event_id`、`partition_key`、生产序号、事件时刻、体摘要、批次标识、聚合键和双精度度量。

- 每个 `(topic, partition)` 的生产序号从 1 开始单调递增且无缺口。
- `partition_key` 使用 FNV-1a 64 确定性哈希路由到分区。
- 同一 `event_id` 重复发布且内容一致时返回既有分区/序号，并标记 `idempotent=true`。
- 同一 `event_id` 内容不一致时返回 `E_CONFLICTING_EVENT`。

### 事件/订阅状态机

持久化状态为 `ACCEPTED`、`DISPATCHED`、`ACKED`、`REDELIVERED`、`DEAD_LETTERED`。合法迁移如下：

| 起点 | 合法终点 |
|---|---|
| `ACCEPTED` | `DISPATCHED`, `DEAD_LETTERED` |
| `DISPATCHED` | `ACKED`, `REDELIVERED`, `DEAD_LETTERED` |
| `REDELIVERED` | `DISPATCHED`, `DEAD_LETTERED` |
| `ACKED` | `REDELIVERED`（仅显式位点回拨） |
| `DEAD_LETTERED` | `REDELIVERED`（仅显式位点回拨） |

非法迁移返回 `E_INVALID_TRANSITION`，事务不提交，已持久化状态不变。

### 位点、投递与租约

- 消费位点定义为“最后一条已确认的生产序号”，初始为 0。
- 拉取选择位点之后的最小未确认事件，可在一次租约内拉取连续的多个事件。
- 同一 `(topic, partition, group)` 同时最多一个投递租约；并发拉取只有一个成功，其余返回 `E_LEASE_HELD`。
- 租约过期后的下一次拉取先把旧投递标记为 `REDELIVERED`，再从原序号重投，不跳过后继事件。
- 确认必须从 `acked_offset + 1` 开始且连续；否则返回 `E_OUT_OF_ORDER`。
- 持有租约时可对位点后的首条事件调用死信接口；事件进入 `DEAD_LETTERED`，位点显式推进，且该事件不进入聚合。

### 回拨与重放

`POST /v1/rewind` 将位点回拨到指定生产序号：

- 目标早于分区最早保留序号：`E_OFFSET_EXPIRED`。
- 目标大于当前已确认位点：`E_OFFSET_INVALID`。
- 校验失败不改变事件、位点或聚合状态。

回拨成功后，旧位点记为 `replay_until`。重放期间只投递 `target+1..replay_until`，新发布事件暂缓；最后一条重放事件确认后自动解除暂停。

## 数值精度与误差传播

### 聚合顺序

聚合范围是 `(topic, group, partition)`。事件总是按生产序号逐条应用；确认批次只影响审计摘要，不影响最终累加器状态。因此同一事件序列无论一次确认还是一批一事件确认，最终状态摘要逐位一致。

实现三种算法：

- `naive_sequential`：逐事件加法。
- `kahan_compensated`：逐事件 Kahan 补偿加法。
- `magnitude_buckets`：按二进制指数分桶，桶内 Kahan 累加，再按指数顺序 Kahan 归并。

分组聚合为每个事件键保存计数、求和、`sum_abs`、算法状态和误差界。计数使用整数，不存在浮点计数误差。

### 可量化绝对上界

设 `u = 2^-53`，`gamma(n) = n*u/(1-n*u)`，`sum_abs = Σ|x_i|`。由于持久化的 `sum_abs` 自身也由浮点累加得到，计算上界时使用 L1 高估量 `L1upper = sum_abs / (1 - gamma(n))`。实现声明并为每个键持久化以下保守上界：

- 朴素顺序：`gamma(n) * L1upper`
- Kahan：`(2u + gamma(n)*u) * L1upper`
- 量级分桶：各桶 Kahan 上界之和，再加桶归并的 Kahan 上界

绝对误差界以 L1 量级为尺度，因此在正负抵消导致结果接近 0 时仍有定义。相对误差界为 `absolute_bound / max(|sum|, tiny)`；分母为 0 且存在非零误差时以 `math.MaxFloat64` 表示。HTTP JSON 不输出 NaN/Inf 原始值。

每个确认批次写入 `agg_batches`，保留算法、事件批次、序号区间、逐键部分和、`sum_abs`、绝对/相对误差界、状态摘要以及激活/撤销状态。误差查询同时返回：

- 每批历史摘要。
- 当前累计聚合状态和上界。
- 可组合的账本上界：活跃批次上界之和，再加批次归并的 `gamma(B)` 项。

### 回拨回退一致性

回拨事务在同一 SQLite 事务中完成：

1. 从最新到最旧逐批标记受影响的聚合批次为撤销。
2. 撤销目标位点之后的确认记录。
3. 把目标位点之后的 `ACKED`/`DISPATCHED` 订阅状态转为 `REDELIVERED`。
4. 从仍有效的确认记录按序号重建聚合状态和快照。
5. 若回拨点切穿了某个确认批次，保留其 `start_seq..target` 的活跃审计分段。
6. 原子更新位点、重放边界和版本。

因此读请求只会看到回拨前或回拨后的完整版本，不会看到“计数已退、度量未退”的中间状态。重放按原序号重新应用；测试验证其摘要与从未回拨、直接消费到同位点的结果逐位一致。

## 并发、幂等与持久化

- 物理分区级写锁串行化所有消费者组在该分区上的租约、确认、位点与聚合写操作。
- 进程内 SQLite 写闸门避免并发写事务争用；WAL + 只读事务允许查询并发读取完整提交快照。
- 所有业务状态变更先提交 `intents` 意图记录，再提交包含 `intent_id` 的业务事务，最后标记意图完成。
- 请求标识持久化在 `idempotency` 表。相同请求并发到达时只有一个执行，其余等待并复用结果；相同请求标识配合不同内容返回 `E_CONFLICTING_REQUEST`。

业务表包括：主题/组、事件、订阅状态、确认、位点历史、聚合状态、聚合批次、聚合快照、意图和恢复报告。

### 崩溃恢复

重启时执行 `Recover`：

- 扫描 `PENDING` 意图。
- 依据已落盘的确认、位点历史、聚合批次、事件、投递状态等证据区分“已提交”与“已回滚”。
- 未提交确认没有业务行残留；所有旧进程 `DISPATCHED` 投递转为 `REDELIVERED`，租约清空，随后按原序号重投。
- 位点推进和聚合提交因 SQLite 事务原子性，不会出现半行状态。
- 恢复动作和证据写入 `recovery_runs`，只基于已落盘事实。

`internal/bus/crash_test.go` 使用真实子进程在 `ack_after_intent` 和 `ack_before_commit` 阶段直接 `os.Exit(42)`，重启后核对意图回滚、租约释放、无残留确认/聚合状态以及原序号重投。

## HTTP API

所有成功响应为：

```json
{"ok": true, "result": {}}
```

所有失败响应为：

```json
{"ok": false, "error": {"code": "E_OFFSET_INVALID", "message": "..."}}
```

写接口可通过请求体 `request_id` 或 `Idempotency-Key` 头提供幂等标识。

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/topics` | 创建主题、分区和消费者组 |
| `GET` | `/v1/topics?topic=...` | 查询主题 |
| `POST` | `/v1/events/publish` | 发布事件 |
| `POST` | `/v1/pull` | 获取分区租约并拉取事件 |
| `POST` | `/v1/ack` | 确认连续事件 |
| `POST` | `/v1/dead-letter` | 将当前租约的首个事件显式转为死信并推进位点 |
| `POST` | `/v1/rewind` | 位点回拨并进入重放 |
| `POST` | `/v1/algorithm` | 首次确认前切换聚合算法 |
| `POST` | `/v1/retention` | 设置分区最早保留序号 |
| `GET` | `/v1/offsets?topic=&group=&partition=` | 查询位点与租约 |
| `GET` | `/v1/events/state?topic=&group=&partition=&seq=` | 查询订阅状态 |
| `GET` | `/v1/aggregate?topic=&group=&partition=` | 查询聚合结果与状态摘要 |
| `GET` | `/v1/errors?topic=&group=&partition=` | 查询逐批误差账本 |
| `GET` | `/v1/recovery` | 查询最近恢复报告 |
| `GET` | `/healthz` | 健康检查 |

创建主题示例：

```json
{
  "request_id": "topic-1",
  "topic": "orders",
  "partitions": 2,
  "groups": [
    {"name": "analytics", "algorithm": "kahan_compensated"}
  ]
}
```

发布事件示例：

```json
{
  "request_id": "publish-1",
  "event_id": "evt-000001",
  "topic": "orders",
  "partition_key": "customer-42",
  "event_time_ms": 1700000000000,
  "batch_id": "batch-0001",
  "aggregate_key": "region-east",
  "metric": 1234.5,
  "body": {"source": "local-sample"}
}
```

主要业务码：`E_INVALID_ARGUMENT`、`E_UNKNOWN_TOPIC`、`E_UNKNOWN_GROUP`、`E_CONFLICTING_EVENT`、`E_CONFLICTING_REQUEST`、`E_INVALID_TRANSITION`、`E_LEASE_HELD`、`E_LEASE_EXPIRED`、`E_OUT_OF_ORDER`、`E_NO_EVENT`、`E_OFFSET_EXPIRED`、`E_OFFSET_INVALID`、`E_ALGORITHM_LOCKED`、`E_NOT_FOUND`、`E_INTERNAL`。

## 固定种子样本

`internal/sample.Generate(seed, count, batchSize)` 生成：

- 两个热点分区键（`hot-a`、`hot-b`）和 200 个长尾键。
- 8 个聚合键。
- `1e-12..1e12` 范围内跨 25 个十进制数量级的双精度度量，含正负号与周期性精确抵消样本。

## 实测结果

详细命令、阈值与输出见 [docs/VERIFICATION.md](docs/VERIFICATION.md)。本环境已验证：

- `go test ./...`：全部通过。
- `go test -race ./...`：全部通过。
- 强制终止子进程测试：两个事务阶段均正确恢复。
- 固定种子 `20261125`、64 条事件：三种算法的“逐条确认”与“整批确认”状态摘要完全相同；回拨与重放摘要分别匹配目标位点直接消费和回拨前完整状态。

## 设计边界

- 当前实现是单进程单机协调器；跨进程部署应由外部协调器保证单写者。
- 聚合状态按消费者组和物理分区维护；跨分区汇总需在上层对分区结果再做确定性归并。
- 聚合算法只允许在首次确认前切换，避免在同一状态摘要中混合算法历史。
- 最早保留序号是逻辑水位；审计行、快照和事件证据不会自动物理删除，以支持回拨与恢复核验。
