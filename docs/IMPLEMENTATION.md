# 本地列式日志聚合执行服务 — 实现说明

Go module 为 `logagg`，Go 指令版本为 `1.21`，实际验证工具链为 Go `1.27.4 linux/amd64`。SQLite 驱动使用 `github.com/mattn/go-sqlite3 v1.14.24`，通过 CGO 链接本机 SQLite `3.46.1`。

## 结构

- `internal/model`：分片清单、块头、列定义、离线事件与摘要计算。
- `internal/sample`：固定种子 `20261126` 生成器；包含热点分区 `0`、普通分区 `1/2` 和长尾分区 `3`。
- `internal/codec`：`fixed`、RLE、dictionary 三种确定性编解码，支持从指定行开始的区间解码。
- `internal/plan`：按声明解压字节、最小行宽、保留字节和单块内存上限计算整体/分段方案。
- `internal/agg`：int64 定点计数、求和、最小值、最大值、溢出检查、显式舍入轨迹和朴素浮点误差界。
- `internal/sched`：按分区号、块号、段起点稳定排序的预算规划，以及有界并发解码池。
- `internal/shard`：分片校验和原子索引快照；无效提交不会替换当前快照。
- `internal/store`：SQLite schema、意图、部分和、批次幂等记录、分片索引和崩溃恢复。
- `internal/engine`：会话单写者、多读者快照、跨批次增量、续算和提交流程。
- `internal/api`：本地 HTTP/JSON 接口。
- `cmd/logagg`：`serve`、`prepare-crash`、`recover`、`resume-crash` 命令。

## 需求对应

### 分片版本与摘要

`shard.Validate` 校验分片 ID、正版本、生成时间、列定义、块号唯一性、编码、行数、声明字节和逐块 SHA-256 摘要；失败统一返回 `E_SHARD_INVALID`。

`Engine.CommitShard` 先检查 SQLite 当前版本，再写 `shard_switch` pending 意图，随后在同一事务中写 `shards`、`shard_block_data`、`current_shard` 并把意图置为 committed。事务成功后才通过 `Registry.Commit` 原子替换内存索引。旧版本仍保留在 `Registry` 的版本表和 SQLite 中，可供固定版本续算。

### 编码与损坏定位

二进制格式：

- `fixed`：连续小端 int64，长度必须为 `row_count * 8`。
- RLE：若干 `value int64 + count uint32`，每项 12 字节。
- dictionary：`dict_count uint16 + int64 字典 + uint32 索引数组`。

解码前校验块摘要、编码类型和行数。结构错误返回 `E_BLOCK_CORRUPT`，错误体含 `block_no`、`offset` 和 `details`。区间解码使用 `start_row` 与 `max_rows`，测试验证分段结果等于全量解码的相同行区间。

### 单块内存裁决

`plan.PlanBlock` 输出：

- `available_budget_bytes = memory_limit_bytes - reserved_bytes`
- `effective_row_width = ceil(declared_bytes / row_count)`，且不小于最小行宽 16（分组 int64 + 度量 int64）
- 整体方案或 `segment_rows = floor(available_budget / row_width)`
- 每段 `start_row/end_row/estimated_bytes`
- `formula`、`basis[]` 和 `estimated_peak_bytes`

当声明字节小于最小行宽、`declared_bytes < row_count * min_width`，或可用预算小于最小行宽时，返回 `E_BLOCK_UNBOUNDED`，并在详情中附 `min_row_width_bytes` 与 `available_budget_bytes`。

### 部分和与续算

`agg.SegmentSum` 携带 `session_id`、`shard_id`、`base_version`、`partition`、`block_no`、`segment_start/end` 和 `definition_hash`。每个分组维护 count、sum、min、max。所有 sum/count 合并均用带溢出判断的 int64 加法；溢出错误码为 `E_NUMERIC_OVERFLOW`，并带 `window` 与 `group`。

会话从 SQLite 载入已提交部分和。续算时，相同 `(session, shard, version, block, segment_start)` 且定义哈希一致的段直接进入 `reused_segments`，不会重新解码。完成后的批次再次 Continue 也直接复用已存结果。

### 并发、队列和预算

`sched.PlanRound` 先按 `(partition, block_no, segment_start, id)` 稳定排序，再依次消耗字节预算。首个放不下的任务及其后所有任务进入 `deferred`，每项给出 `remaining_budget`、`continue_from_row` 和稳定序号。

并发槽位数为 `max_concurrency - active_count`，其余预算内任务为 `queued`；等待数超过 `queue_capacity` 时返回 `E_DECODE_QUEUE_FULL`。`sched.Pool` 用 mutex 调度保证活动任务不超过并发上限，运行中任务结束后串行接管排队任务。

### 数值契约

原始度量为 `input_scale` 位小数的 int64 定点值。累加与合并不做中间浮点转换；只有最终结果按 `result_scale` 和舍入模式（`half_up`、`half_even`、`down`）做显式缩放。

`QueryResult.rounding_trace` 逐分组输出输入原值、缩放方向、模式、舍入后值、以输入精度计的残差和累计误差；`cumulative_rounding_error` 与 `max_abs_rounding_error_value` 可直接扫描。

固定样本测试同时执行逐行 float64 朴素求和，使用
`(n-1) eps * sum(abs(x_i)) / (1-(n-1)eps)` 作为累加误差上界，再加上结果精度的 0.5 ULP。实测最大浮点累加偏差为 `1.14e-12`，上界为 `4.8e-11`；舍入后输出偏差为 0，输出总上界为 `0.005`。

### 单写者、幂等和读一致性

每个 `engine.Session` 有：

- `writeMu`：串行化同一会话的 Advance/Continue 和批次检查。
- `stateMu`：保护已提交部分和与内存总量；多个 Query 可共享 RLock 并发读取完整版本。

部分和、`segment_work` 完成状态和意图状态在同一个 SQLite 事务中提交，因此不会出现“块完成但部分和未落盘”。分片元数据、块摘要和当前索引也在同一 SQLite 事务中切换，提交后原子替换 registry 快照。

`batches` 表以 `batch_id` 为主键并保存 `request_hash/result_json`。相同 ID 和相同内容返回 `reused=true`；相同 ID 但 Items、查询列、精度或内存裁决参数不同，返回 `E_CONFLICTING_BATCH`。轮次 `byte_budget` 不进入内容哈希，因此 Continue 可提供新预算。

### SQLite 意图与恢复

意图类型：

- `segment_start`：解码前与 `segment_work(status=pending_decode)` 一起提交。
- `partial_commit`：写部分和事务前先提交 pending 意图；随后与 `partial_sums`、`segment_work=committed` 一起置为 committed。
- `shard_switch`：分片事务前写 pending；随后与新索引一起提交。

`store.Recover` 只查询 SQLite 已落盘行：

- pending 意图能匹配 `partial_sums` 或 `current_shard + shards` 事实：归入 `committed` 并补齐意图状态。
- 找不到提交事实：把意图置为 `rolled_back`，归入 `rolled_back`。
- 所有仍为 `pending_decode` 的段工作归入 `needs_recompute`。
- 所有已存在的 `partial_sums` 放入 `reusable_partial_sums`。

## HTTP 接口

默认监听 `127.0.0.1:18080`：

- `GET /healthz`
- `GET /v1/events?count=12&session_id=...`
- `POST /v1/demo/shards`，body：`{"shard_id":"shard-local-001","version":1}`
- `POST /v1/shards`：提交 base64 编码的完整分片 DTO
- `GET /v1/shards/{shardID}?version=1`
- `POST /v1/plan/block`
- `POST /v1/sessions/{sessionID}/advance`
- `POST /v1/sessions/{sessionID}/continue`
- `GET /v1/sessions/{sessionID}/query?result_scale=2&rounding_mode=half_up`
- `POST /v1/recovery`

预算不足时 Advance 返回 HTTP `202` 和确定性 `deferred`；冲突返回 `409`，队列满返回 `429`，分片/块/数值契约错误返回 `422`。

## 本地复现

```bash
go test ./...
go test -race -count=1 ./...
go build -o bin/logagg ./cmd/logagg

# 真实 SIGKILL：命令会以 137 退出
./bin/logagg prepare-crash --db artifacts/recovery/logagg.db
cp artifacts/recovery/logagg.db artifacts/recovery/logagg-crashed-preserved.db

# 查看三类恢复报告
./bin/logagg recover --db artifacts/recovery/logagg.db

# 扫描待重算项，复用 block 0，只续算 block 1
./bin/logagg resume-crash --db artifacts/recovery/logagg.db

# HTTP 服务
./bin/logagg serve --addr 127.0.0.1:18080 --db artifacts/service/logagg.db
```

如果默认 Go proxy 超时，可使用：

```bash
GOPROXY=https://goproxy.cn,direct go mod tidy
```
