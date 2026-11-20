# 设计说明

## 1. 核心不变量

组件围绕一个不可变的 `RouteState` 工作：

```text
RouteState = Snapshot(version, checksum, table specs)
           + caches[logical_table -> TableCache]
```

`Snapshot`、`TableSpec`、`PhysicalTable`、`TableCache` 和 `RouteState` 均不再原地修改。任意查询在开始时取得一个 `RouteState` 对象，之后的提交只能替换活动引用，不能改写该对象。因此：

1. 一个查询的所有路由计算、物理表执行与归并只使用同一快照版本。
2. 缓存版本与快照版本不会在请求内部交错。
3. 如果查询期间发生提交，响应仍完成旧版本，并记录替换后的当前版本。

## 2. 快照校验

1. 顶层 `checksum` 必须是 64 位小写十六进制。
2. 去掉 `checksum` 后以键排序、紧凑分隔符和 UTF-8 编码序列化，计算 SHA-256。
3. 校验 `schema_version`、正整数版本、生成时间、分片键类型、算法结构、SQL 安全标识符。
4. 解析命名算法和 `ref` 链；使用递归栈检测算法环。
5. 解析逻辑表 `route_ref`；使用递归栈检测表映射环，并继承目标表路由属性。
6. 校验物理表映射：空列表、重复物理表、重复分片、算法分片集合不匹配。
7. 校验范围：边界类型、空区间、任意两个区间的开放/闭合并集是否重叠。

只有以上步骤全部成功，才允许进入提交路径。校验失败不会写快照、不会更新活动版本，也不会改动内存状态。

## 3. 提交流程

完整快照和增量补丁共享同一写者锁，提交顺序为：

1. 校验候选快照；补丁先比较 `base_version == current.version`。
2. 写入 `SNAPSHOT_COMMIT`/`SNAPSHOT_PATCH` 意图，状态 `PREPARED`。
3. 写入 `CACHE_REBUILD` 意图，状态 `PREPARED`。
4. 构建新的不可变缓存集合。补丁只重建语义发生变化的逻辑表；其他逻辑表保留物理映射和算法内容，但生成新版本标记。
5. 在一个 `BEGIN IMMEDIATE` 事务中插入完整快照、更新 `active_state`、插入本版本全部缓存清单，并把两个意图置为 `COMMITTED`。
6. 数据库提交成功后，在读写锁的写临界区一次性替换 `RouteState` 引用。
7. 若第 5 步前进程死亡，数据库中没有新快照和活动版本；重启只看到未完成意图并标记中止。

因此内存可见性与持久化提交边界都以“完整快照 + 完整缓存集合”为单位，不存在部分表已换新、部分表仍旧的公开状态。

## 4. 算法与路由

### 取模

`shard = normalize_integer(key) % modulus`。Python 的模运算保证负数结果非负。

### 哈希

```text
payload = seed + "\x00" + key_type + "\x00" + normalized_value
digest  = SHA-256(payload)
shard   = uint64_be(digest[0:8]) % modulus
```

`seed` 来自快照，同一版本重复计算逐字节一致，不依赖 Python 进程随机哈希。

### 范围

每个分片区间显式声明 `lower`、`upper`、`lower_inclusive`、`upper_inclusive`。默认是左闭右开。`null` 边界表示无界。区间重叠在快照校验时拒绝。查询区间的相交判断也保留开放边界，因此相邻的 `[0,100)` 和 `[100,200)` 不会因接触点产生错误重叠。空查询区间不会命中任何物理表。

### 有序计划

命中物理表按 `(shard, logical_db, table)` 排序。`plan_checksum` 对请求 ID、绑定版本、逻辑表、查询类型、物理表描述、缓存签名和轨迹计算摘要，便于复现执行计划。

## 5. 执行与归并

计划中的每个物理表按固定顺序读取。每行保留来源元数据：

- 冲突裁决键是逻辑表与声明主键值的规范类型键。
- `physical_order_first` 取物理顺序最早记录。
- `physical_order_last` 取物理顺序最晚记录。
- `max_updated_at_physical_order` 先比较 `updated_at`，相同时取物理顺序较早记录。
- 冲突裁决后再按 `(shard_key, primary_key, physical_order)` 稳定排序。
- LIMIT/OFFSET 在全局去重和排序后应用，因此同一静态数据集的分页拼接不重不漏。

## 6. 查询幂等

请求 ID 的并发状态保存在进程内 `Future` 表中：

1. 第一个线程创建占位并成为执行者。
2. 其他相同 ID、相同载荷的线程等待该 `Future`。
3. 执行者在执行前写入 `query_batches(RUNNING)` 和 `QUERY_BATCH` 意图。
4. 完成后在同一事务中写结果/错误并终态化意图。
5. 重启后，若批次仍为 `RUNNING`，恢复流程写入 `ABORTED` 和统一错误体；之后相同 ID 直接得到该错误，不会再次执行。

请求载荷摘要不包含 `request_id`。相同 ID 搭配不同载荷在任何路径都返回 `E_REQUEST_CONFLICT`。

## 7. 恢复

启动顺序：

1. 插入新的 `service_runs` 行，并记录此前未优雅退出的运行。
2. 读取 `active_state` 和所有快照行，逐条重新验证文档摘要与版本一致性。
3. 选择版本最高的完整有效快照。若它不同于活动版本，写 `RECOVERY_ROLLBACK` 意图并修复 `active_state`。
4. 扫描 `query_batches` 的 `RUNNING` 行，将批次与对应意图标记为 `ABORTED`。
5. 扫描其他未完成意图并标记 `ABORTED`。
6. 先写 `CACHE_REBUILD` 意图，再从最后完整快照重建全部内存缓存并完成意图。
7. 写入恢复报告。报告字段直接来自上述持久化行，不推断未落盘的内存状态。

恢复报告中的 `complete_exit_report_available` 只有当上一个运行具有持久化的 `graceful_stop_at` 时才为真。强杀运行时该字段为假，并给出报告限制说明。

## 8. 并发边界

- 写者锁：所有快照、补丁和缓存生成串行。
- 读写锁：查询只在绑定 `RouteState` 时短暂持有读锁；SQLite 执行和归并期间不持锁。
- 状态替换：写锁临界区内单引用赋值。
- 元数据 SQLite：WAL、`synchronous=FULL`、`busy_timeout=30000`、写事务使用 `BEGIN IMMEDIATE`。
- 物理 SQLite：每个目标使用独立只读连接，可并发读取。

组件定位为单写服务进程；同一元数据库不建议启动多个服务进程。SQLite 会串行化跨进程写事务，但活动内存路由只能由单进程权威更新。

## 9. 故障注入点

- `invalid_shard_key` -> `E_SHARD_KEY_INVALID`
- `empty_physical_mapping` -> `E_EMPTY_PHYSICAL_MAPPING`
- `version_rollback` -> `E_VERSION_ROLLBACK`
- `range_overlap` -> `E_ROUTE_CONFLICT`
- `execution_interrupt` -> `E_EXECUTION_ABORTED`，批次持久化为中止。
- `crash_after_intent` -> 查询意图提交后 `os._exit(86)`。
- `crash_mid_merge` -> 读取第一个物理表后 `os._exit(86)`，用于证明未完成批次恢复。
- 快照/补丁端点支持 `?crash_after_prepare=1`，用于在意图写入后、元数据提交前终止。

