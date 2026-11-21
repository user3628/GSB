# 技术说明

## 1. 项目结构

- `colscan/codecs.py`：定长、游程、字典三种确定性编码及块头/分段完整性校验。
- `colscan/snapshot.py`：快照文档规范化、SHA-256 摘要、列定义/空值位图/行块索引校验。
- `colscan/predicate.py`：解压后列数组上的 SQL 三值谓词求值。
- `colscan/scanner.py`：预算估算、分段拆分计划、并行轮次调度、过滤投影和稳定归并。
- `colscan/repository.py`：SQLite WAL 状态、快照原子切换、分片两阶段提交、恢复报告。
- `colscan/service.py`：查询规范化、幂等、后台执行、结果读取和恢复编排。
- `colscan/api.py`、`colscan/server.py`：FastAPI 路由与回环地址启动器。
- `colscan/reference.py`：不经过调度器和 SQLite 的单线程参考扫描。
- `scripts/generate_sample.py`：固定种子语义的本地样本生成器。
- `scripts/e2e_verify.py`：真实 HTTP 进程、低预算和强杀恢复验收。
- `tests/`：编码、谓词、快照版本和扫描服务测试。

## 2. 快照格式

快照为 UTF-8 JSON 文件，扩展名 `.csnap`。顶层字段包括：

- `format`: `colscan-snapshot/1`
- `checksum_algorithm`: `sha256`
- `version`: 单调递增整数
- `generated_at`: 生成时间
- `row_count`、`block_size`
- `row_blocks`: 连续覆盖所有行的行块索引
- `columns`: 每列的类型、可空性、空值总数、空值位图、编码及逐行块数据
- `checksum`: 除该字段外整个文档的规范化 JSON SHA-256

规范化方式为 `json.dumps(sort_keys=True, separators=(",",":"))`。安装流程先在锁外读取并完整验证候选快照，再用 SQLite `BEGIN IMMEDIATE` 比较活动版本并同时写入 `snapshots` 与 `active_snapshot`。查询固定使用其 `snapshot_version` 对应的不可变文档，因此活动指针替换不会混合版本。

## 3. 块编码

### 3.1 单流块头

```text
CSB1 | encoding:u8 | dtype:u8 | row_count:u32 | raw_size:u32 |
body_len:u32 | body_crc32:u32 | body
```

编码编号为 `fixed=1`、`rle=2`、`dict=3`；类型编号为 `int64=1`、`float64=2`、`bool=3`、`string=4`。解压时校验魔数、编码、类型、行数、原始大小、体长度和 CRC32。任一不一致都返回 `E_BLOCK_CORRUPT`，`details.block_id` 保留列名和行块标识。

### 3.2 可拆分分段容器

为让“拆分为子块重试”真实降低单任务解压峰值，样本块使用分段容器：

```text
CSBC | encoding:u8 | dtype:u8 | total_rows:u32 | total_raw:u32 |
segment_count:u32 | body_len:u32 | body_crc32:u32 | body

每个分段记录：
start_row:u32 | row_count:u32 | payload_len:u32 | raw_size:u32 | CSB1 payload
```

外层校验总行数、总原始大小、编码/类型和容器 CRC；内层逐段校验 `CSB1` 头。只请求某一行范围时，仅解码与该范围相交的分段，但会校验所有段的头部和 CRC，避免静默截断。

### 3.3 编码语义

- `fixed`：数值和布尔按固定字节宽度编码；字符串先写最大宽度、逐值长度，再补齐到固定宽度。
- `rle`：连续相同值记录为 `value_len + value + run_len`；确定性保留首次出现顺序。
- `dict`：字典按编码值字节序排序，随后写字典项和每行 `u32` 索引。

块元数据记录 `raw_size`、`compressed_size`、`compression_ratio = raw_size / compressed_size` 和编码统计。分段/块头开销可能使小块的压缩率小于 1。

## 4. 预算与调度

### 4.1 估算

调度器对每个“行块 + 行子范围”任务估算：

- 内存：基础工作区 + 所需列解码后原始字节 + 每行数组开销 + `rle` 游程或 `dict` 字典项开销。
- 解压时间：按压缩后大小、原始大小、编码类型、行数、运行数/字典大小分别加权。
- 压缩信息：逐列输出编码、原始大小、压缩大小、压缩率和编码元数据。

`columns` 是输出投影；谓词引用但未投影的列也会解码参与过滤，但绝不会进入输出。

### 4.2 拆分

规划阶段按 `(block_seq, start, end)` 稳定顺序处理。若估算超过单任务内存或单轮时间预算且行范围可继续拆分，则在分段容器边界一分为二，并持久化一条 `phase=plan, action=split` 裁决，包含父范围、子范围、逐列依据和受影响块清单。

若达到最小分段粒度后仍超限：

- `strict` 模式返回 `E_TASK_MEMORY_EXCEEDED` 或 `E_ROUND_BUDGET_EXCEEDED`。
- `split` 模式才允许显式记录 `admit_forced_overrun` 和最小粒度原因，不会把单纯排队伪装成预算已解决。

验收中的低预算参数为 1200 字节/任务，16 行原始块的估算为 2829 字节，因此产生 18 条拆分裁决；拆分后的任务仍满足预算，没有强制超限。

### 4.3 并行轮次

每轮按确定顺序从待处理队列取任务，同时满足：

- 已准入任务数不超过 `max_concurrent_tasks`。
- 每个任务估算内存不超过 `per_task_memory_bytes`。
- 本轮累计估算时间不超过 `round_time_budget_ms`。
- 同一逻辑行块在同一轮最多出现一次；其余同一块子范围明确排队到后续轮次。

超过并发上限或本轮时间预算的任务进入有界等待队列。提交时若选中块数超过“并发数 + 队列容量”立即返回 `E_SCAN_QUEUE_FULL`；运行期间等待队列溢出也返回该业务码。实际执行使用 `ThreadPoolExecutor`，轮次结束后才归并下一轮，保证裁决轨迹可重放。

## 5. SQLite 状态与恢复

数据库默认位于 `data/state.db`，启用 WAL、外键和 `synchronous=FULL`。核心表为 `snapshots`、`active_snapshot`、`queries`、`scan_shards`、`shard_results`、`budget_decisions` 和 `recovery_reports`。

分片状态机：

```text
READY -> RUNNING -> INTENT -> COMMITTED
```

- `RUNNING`：扫描任务已认领，记录尝试次数。
- `INTENT`：过滤投影结果已计算，结果摘要已持久化，但结果分片尚未提交。
- `COMMITTED`：结果行与状态在同一事务内提交，查询的 `committed_blocks` 同步增加。

启动恢复事务扫描所有未完成查询：

- `COMMITTED` 分片标记 `reused=1` 并复用。
- 有结果的异常 `INTENT` 分片按已存在结果重新识别为 `COMMITTED`。
- 其余 `RUNNING/INTENT/READY/FAILED` 分片恢复为 `READY`，清除错误并增加 `recovery_count`。
- 查询进入 `RECOVERING`，写入一条仅引用 `scan_shards`、`shard_results`、`queries` 事实的恢复报告，然后自动启动续扫。

历史进程被 `SIGKILL`/TerminateProcess 终止时不可能生成正常退出报告，因此恢复报告明确返回 `complete_exit_report_available=false`，不会推测未持久化的内存状态。

## 6. 一致性与归并

每个查询只有一个执行线程写分片，SQLite 事务保证单分片“结果 + 状态 + 进度”原子提交。查询全部结束前，结果接口返回 `E_QUERY_NOT_READY`；全部提交后，归并事务再原子写入完整结果、总行数和全局 `commit_generation`。因此读者看到的只能是某一完整代次，不会看到“部分分片已提交但归并序号仍旧”的中间状态。

归并顺序为行块序号升序、块内 `row_offset` 升序。查询请求的 `offset/limit` 保存为默认窗口；读取时可再次指定分页参数，从完整稳定结果上切片。

## 7. 验证边界

服务按单 Uvicorn 进程运行，使用 SQLite WAL 支持多读者和查询间事务串行。`max_concurrent_tasks` 是该查询扫描器的全局并发上限；多查询可并发读取已完成结果，扫描提交由 SQLite 写事务串行化。若需要跨多个服务进程部署，应替换为外部协调与单写者租约，本项目未声称支持该模式。

