# 技术说明

## 1. 设计目标与边界

系统在单机、单进程、回环网络内完成以下闭环：

1. 读取左右两份不可变列式快照并验证版本和摘要；
2. 按请求分区数做确定性哈希 Shuffle；
3. 分区内稳定排序后执行 inner/left Join；
4. 以固定规范顺序归并度量并计算 sum、mean、count；
5. 通过 SQLite 持久化快照版本、分区计划、部分和、聚合结论与恢复事实；
6. 通过本地 HTTP API 和 CLI 实验脚本复现全部验证。

不包含跨机网络 Shuffle、多 writer 进程协调、用户认证或外部数据源访问。

## 2. 快照与校验

### 2.1 文件布局

每份快照是一个不可变目录：

```text
manifest.json
keys.bin      <i8, little-endian
measures.bin  <f8, little-endian
nulls.bin     u8, 1=NULL, 0=非 NULL
```

manifest 记录 `snapshot_id`、`side`、`version`、`generated_at`、`row_count`、dtype、维度、单位、每个数组的文件名/字节数/SHA-256，以及覆盖整个核心 manifest 的 `content_sha256`。

### 2.2 加载校验顺序

1. manifest 存在、可解析、格式与字节序匹配；
2. manifest 核心 JSON 的 canonical SHA-256 与 `content_sha256` 一致；
3. 字段类型、版本正整数、时间可解析、side 为 left/right；
4. 度量维度必须为 `dimensionless`，单位必须属于 `score|ratio|count`；
5. 每个数组文件长度、SHA-256 与 manifest 一致；
6. 三列均为一维且等长；
7. NULL 位图只能取 0/1；
8. NULL 位置的度量必须为规范 `0.0`，非 NULL 度量必须有限。

所有失败统一映射为 `E_SNAPSHOT_INVALID`，details 中带确定性原因和位置。写入快照时使用同父目录 staging + 原子 rename，避免暴露半写目录。

### 2.3 版本与统计隔离

SQLite 的 `active_datasets` 保存当前左右版本对，如 `L2:R2`。加载时：

1. 先完整校验两份新快照；
2. 在单个 `BEGIN IMMEDIATE` 事务中检查左右版本都严格大于当前版本；
3. 写入 `snapshot_history` 并更新 `active_datasets` 指针。

每个请求在登记时绑定当时的 `pair_version`。执行记录、分区、部分和、聚合与误差合同都关联该版本。因此换版只影响后续请求，不会混合新旧统计。旧请求结果仍可查询。

## 3. 确定性 Hash Shuffle

对 int64 键按 little-endian 布局转换为 uint64 位模式，执行 SplitMix64：

```text
z = key + 0x9E3779B97F4A7C15
z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
z = (z ^ (z >> 27)) * 0x94D049BB133111EB
z = z ^ (z >> 31)
partition = z mod P
```

使用固定常量和无符号 64 位回绕，不依赖 Python 的进程随机化 `hash()`。因此：

- 同一键在任何表、任何请求中均落入同一分区；
- 左右表可独立 Shuffle 后正确 Join；
- 结果在不同 Python 进程、Windows 本地运行中保持一致。

分区统计包含左右记录数、总记录数、估计字节数、输入占比、`skew` 标记与热点键。字节估计为每行 `8(key)+8(measure)+1(null)+8(row locator)=25` 字节，加 128 字节固定分区信封。

偏斜阈值由请求的 `skew_limit` 声明；`share > skew_limit` 时标记。`skew_action=report` 保留结果并返回热点键；`reject` 返回 `E_SKEW_LIMIT`。热点键按分区内记录数降序、键升序排列。

## 4. 分区内 Join

每分区执行：

1. 取本分区左右行号；
2. 以键做稳定排序，重复键内部保持原始行号顺序；
3. 构建右侧 `key -> sorted right row ids` 映射；
4. 按左侧稳定顺序遍历；
5. inner Join 只产生匹配组合对；left Join 的未匹配左行产生一条右侧 NULL 记录；
6. 连接输出按 `(key, left_row_index, right_row_index)` 排序。

度量来源：

- `left`：取左表度量与空值位图；
- `right`：取右表度量与空值位图；左外未匹配侧因此全为 NULL；
- `both`：任一侧 NULL 则 NULL，否则双精度相加。

计数语义：

- `count`：连接输出行数，NULL 度量也计入；
- `non_null_count`：参与 sum/mean 的度量数；
- `mean = 高精度或快速路径的 sum / non_null_count`；
- 全部为 NULL 时 `sum=0.0`、`mean=null`，这是确定值而不是错误。

连接输出键升序写出。

## 5. 精度合同

### 5.1 基础记号

- `u = 2^-53` 为 double 的 unit roundoff；
- `N` 为某组分中非 NULL 度量个数；
- `S = Σ|x_i|` 是该组的绝对值和；
- 高精度路径把每组规范序列按固定 4096 条切块，每块 Kahan 求和，再按固定块序用 Kahan 合并。

### 5.2 误差界

高精度路径声明：

```text
|error_high| <= (4u + 16*N*u^2) * S
```

快速路径使用经典朴素求和界：

```text
|error_fast| <= N*u/(1-N*u) * S
```

实现将非有限界编码为 `unbounded=true`，JSON 中不写非标准 `Infinity`。每个分组输出其 `error_bound`，精度端点同时给出两条路径的公式、界、操作数与适用结论。

### 5.3 分区无关的规范归并

高精度路径不依赖 Shuffle 分区内浮点累加结果作为最终跨分区结果，而是：

1. 所有连接贡献带稳定行标识 `(group_key, left_row_index, right_row_index)`；
2. 全量规范排序；
3. 每组按固定 4096 元素 chunk 做 Kahan；
4. 按 chunk 序号固定合并。

该顺序与请求分区数无关。因此测试中 P=1/2/3/7/17/64 得到完全相同的 double 位模式。分区部分和仍持久化到 `partial_sums`，用于审计快速路径与展示每分区 Kahan/朴素部分和。

这里也明确了业务约束：正确 co-partition 后同一键在一个 reducer 分区内，跨分区合并的是互斥分组列表；规范归并顺序进一步保证即使实现使用预聚合块，也不会因并行调度改变位模式。

### 5.4 均值

均值从补偿后的总和与非空计数推导：

```text
mean = sum_value / non_null_count
```

不使用流式 `mean += x/n` 或逐项均值累加。

## 6. 精度预算与路径选择

请求精度字段：

```json
{"mode": "auto|high|fast", "target_abs_error": 10.0, "max_operations": 2000}
```

成本模型包含：

- 连接行数；
- 分组数；
- 分区固定开销 `P * 64`；
- 偏斜惩罚 `int(joined_rows * max(0, max_partition_share - 1/P))`；
- 高精度路径 chunk 与补偿开销。

选择逻辑：

1. 计算两条路径的最大分组误差界；
2. 若显式 `high` 且界超过目标，返回 `E_PRECISION_BUDGET`；
3. 若显式 `fast` 且界超过目标，返回 `E_PRECISION_BUDGET`，绝不静默改用 high；
4. `auto` 在快速路径满足目标时优先快速路径，否则选择满足目标且预算足够的高精度路径；
5. 预算不足或双方都不满足目标时返回 `E_PRECISION_BUDGET`，details 同时给出两条路径的界与操作数。

精度合同中还记录 `partitions`、`max_partition_share`、`skew_excess`、`skew_penalty_operations`、`partition_overhead_operations` 与 `max_group_sum_abs`，使路径选择依据可审计。

## 7. 单写者与并发一致

`EngineService` 使用容量为 1 的 `ThreadPoolExecutor` 作为 FIFO writer：

- Shuffle/Join 执行串行；
- 多个查询线程读取 SQLite WAL；
- HTTP 使用 `ThreadingHTTPServer`，POST 可等待，GET 可并行；
- 提交时绑定活动 `pair_version`；
- 相同 request id + 相同规范化 payload 只登记一次；
- 不同 payload 复用 request id 返回 `E_REQUEST_CONFLICT`；
- 等待中的重复请求复用同一个 Future 与最终持久化结果；
- 执行期间新请求进入 writer 队列，返回的 `queue_position_at_submit` 与状态端点记录排队语义。

可见性规则：

- 分区计划与部分和先持久化，但请求仍为 `running`；
- 最终聚合、误差合同、execution 状态与 request 状态在一个事务中提交；
- 查询只在 `requests.status='committed'` 时读取结果；
- 因此查询不会看到“部分分区已写但聚合仍是旧中间状态”的混合视图；
- 运行中查询返回 `E_REQUEST_NOT_COMMITTED`。

## 8. SQLite 状态机与恢复

主要表：

| 表 | 事实 |
|---|---|
| `snapshot_history` | 已加载快照对及摘要 |
| `active_datasets` | 当前原子活动指针 |
| `requests` | 请求幂等键、状态、绑定版本 |
| `executions` | 执行阶段、最后完整分区 |
| `partition_plans` | 完整分区边界与偏斜统计 |
| `partial_sums` | 每分区每组 Kahan/朴素部分和 |
| `aggregates` | 最终分组结果 |
| `precision_contracts` | 误差声明 |
| `intents` | pending/completed/failed/aborted 写意图 |
| `recovery_reports` | 仅基于持久化事实的恢复报告 |

事务顺序：

```text
BEGIN 请求登记
  INSERT requests(status=queued)
COMMIT

BEGIN 执行开始
  INSERT execution(status=starting)
  INSERT intent(begin_execution, pending)
  UPDATE request(status=running)
COMMIT
  UPDATE intent(completed), execution(status=running)

对每个分区：
BEGIN
  INSERT intent(shuffle_partition, pending)
COMMIT
  计算分区
BEGIN
  INSERT partition_plan
  INSERT partial_sums
  UPDATE intent(completed)
  UPDATE execution(last_complete_partition)
COMMIT

最终：
BEGIN
  INSERT intent(final_merge, pending)
COMMIT
  计算/确定归并结果
BEGIN
  INSERT aggregates
  INSERT precision_contract
  UPDATE intent(completed)
  UPDATE execution(committed)
  UPDATE request(committed)
COMMIT
```

SQLite 使用 `journal_mode=WAL`、`synchronous=FULL`、`busy_timeout=30000`。

启动恢复：

1. 扫描 `executions.status IN ('starting','running')`；
2. 读取已提交 `partition_plans` 与 pending intents；
3. 将请求/执行标记 `aborted`；
4. 将 pending intent 标记 `aborted`；
5. 写入 `recovery_reports`，记录执行前状态、阶段、已持久化分区列表、最后完整分区与 pending intent；
6. 同时处理没有 execution 的遗留 queued 请求。

恢复报告只包含数据库事实，`exit_report_complete=false`，不声称进程强制终止时写下了完整退出报告。

## 9. 实验与验收对照

### 9.1 分区数逐位一致

固定 demo 样本，inner Join，left measure，高精度路径：

```text
P=1  signature=558caba1d3a8fff0 identical=True
P=2  signature=558caba1d3a8fff0 identical=True
P=3  signature=558caba1d3a8fff0 identical=True
P=7  signature=558caba1d3a8fff0 identical=True
P=17 signature=558caba1d3a8fff0 identical=True
P=64 signature=558caba1d3a8fff0 identical=True
```

签名覆盖每组 key、sum 的 IEEE-754 位、mean 的位或 NULL 标记、count、non_null_count。

### 9.2 Kahan vs 朴素

固定样本 `[1e16, 1, 1, ..., 1]`（1000 个 1）：

```text
math.fsum   = 1.0000000000001e16
high path   = 1.0000000000001e16, error = 0.0, bound = 4.440892098503045
fast path   = 1e16,               error = 1000.0, bound = 1111.3332476500163
```

两条实际误差均在声明界内。高精度目标 `10.0` 通过；快速路径目标 `10.0` 返回 `E_PRECISION_BUDGET`。自动路径在 2000 操作预算下也返回该错误，因为高精度所需操作数超预算，而快速路径不满足目标。

### 9.3 空值

`precision` 左外 Join、right measure：

- 未匹配 group 2002：`sum=0.0`, `mean=null`, `count=2`, `non_null_count=0`；
- group 3003 对应右度量全 NULL：同样确定返回 0/NULL；
- 常规组 count 包含 NULL 行，但 sum/mean 只使用非 NULL 值。

### 9.4 偏斜

`skew` 样本 8 分区：

```text
partition 7 share=0.789861, hotspot key=7, partition share=0.958122
report: skew=true，返回热点键
reject: E_SKEW_LIMIT
```

### 9.5 中断恢复

分区中断（`crash-at=3`）：

```text
child exit = 96
request = aborted
last_complete_partition = 2
persisted_partition_ids = [0,1,2]
pending_intent = shuffle_partition(partition_id=3)
aggregates = 0
result query = E_EXECUTION_ABORTED
```

归并中断：

```text
child exit = 98
request = aborted
last_complete_partition = 7
persisted_partition_count = 8
pending_intent = final_merge(path=high)
aggregates = 0
result query = E_EXECUTION_ABORTED
```

### 9.6 版本与幂等

- v1 请求绑定 `L1:R1`；加载 v2 后新请求绑定 `L2:R2`；旧请求结果不受影响；
- 再次加载同版本返回 `E_SNAPSHOT_INVALID`；
- 4 线程并发重复提交同一 request id，execution 计数为 1，所有调用复用同一结果；
- 偏斜/精度失败通过结果查询返回原始业务码，而不是只返回通用 404。

## 10. 测试清单

`python -m unittest discover -s tests -v` 共 8 项：

1. 快照摘要篡改、非法量纲、非法空值位图、版本非单调；
2. 哈希确定性与同键同分区；
3. inner/left Join、NULL 计数、均值语义、键升序；
4. 高精度跨分区逐位一致、Kahan/朴素误差、目标与预算边界；
5. 偏斜报告与拒绝；
6. 单写者排队、运行中不暴露中间结果、并发重复提交幂等；
7. 两种强制终止的恢复报告；
8. HTTP 端到端、统一错误码、精度声明、恢复报告查询。

实测结果：`Ran 8 tests in 2.2s - OK`。

## 11. 结论与限制

- 高精度路径满足“任意分区数逐位一致”的验收目标；
- 快速路径明确声明保守误差界，越界即失败；
- 偏斜、空值、版本、恢复和并发可见性均有确定性测试覆盖；
- Kahan 是误差有界的补偿算法，不等价于正确舍入；合同使用保守界，不夸大精度；
- 快照从生成后必须保持不可变；
- 生产化若需要多机或高吞吐，可替换 writer/存储层，但需要重新证明跨进程幂等和恢复协议。
