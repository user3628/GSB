# 技术设计

## 目标与边界

本应用模拟“多逻辑库 + 每库多分表”的路由控制面，重点不在真实数据行读写，而在路由快照、增量提交、版本绑定读、派生缓存一致性和崩溃恢复。所有数据与样本都在本地，SQLite 使用 `modernc.org/sqlite` 纯 Go 驱动。

核心约束：

- 同一时刻只允许一个提交单写者修改路由状态；
- 多个只读查询可并发读取某一完整提交版本；
- 查询不能观察到“快照已换、缓存未换”的混合状态；
- 失败或中断不能破坏最后一个完整快照；
- 恢复结论只能来自已落盘事实。

## 组件

```text
Browser UI / HTTP API
        |
        v
internal/httpapi  ---> internal/store
                          |  单写者 commitMu
                          |  不可变 runtimeState + RWMutex
                          |  intent -> SQLite main transaction -> state swap
                          v
                    SQLite WAL (snapshots, tables, ranges,
                                route/aggregate caches, refs,
                                commits, intents, recovery reports)

internal/routing 纯函数：Snapshot + Query -> 稳定物理表集合
internal/model   规范化、摘要、语义校验
```

`runtimeState` 在每次成功提交后整体替换；旧状态不被原地修改，因此读请求即使持有旧指针，也只会看到完整旧版本。

## 领域模型

### Snapshot

`Snapshot` 包含：

- 单调递增 `version` 与 RFC3339 `generated_at`；
- 默认 `shard_key` / `shard_key_type` / `algorithm`；
- 物理表注册表 `physical_tables`；
- 逻辑表 `logical_tables`，可覆盖默认分片键/算法；
- 逻辑表内的 `ranges`：上下界、开闭端点、物理表、权重；
- 下游聚合定义 `aggregates`：ID 与 `depends_on` 逻辑表；
- `checksum`：除自身外全部字段的规范化 SHA-256。

逻辑表允许映射到同一物理表，因此合并存储不会触发唯一性错误；路由仍以逻辑表为缓存和版本裁决单位。

### Increment

`Increment` 只携带：

```json
{
  "commit_id": "...",
  "base_version": 1,
  "generated_at": "...",
  "add_physical_tables": [],
  "changes": [
    {"op": "upsert_table", "table_id": "...", "table": {}},
    {"op": "replace_ranges", "table_id": "...", "ranges": []},
    {"op": "delete_table", "table_id": "..."}
  ],
  "aggregates": []
}
```

增量不是直接改库，而是先应用到当前快照的深拷贝，构造 `current+1` 的候选快照，再完整执行一次快照校验。任何一项失败都使整批不生效。

## 校验与摘要

`model.CanonicalizeSnapshot` 会：

- 按物理表 ID 排序；
- 按逻辑表 ID 排序；
- 每个逻辑表的区间按 lower、upper、range id 排序；
- 聚合按 ID 排序，依赖列表排序；
- 保留字段内容但统一顺序，避免 JSON 空白和数组顺序影响摘要。

`SnapshotChecksum` 清除 `checksum` 后编码规范化结构并计算 SHA-256。提交时：

- `seal_checksum=true` 或 `checksum="auto"`：服务端重算并写回；
- 否则必须与请求摘要完全一致，否则 `E_SNAPSHOT_INVALID`。

语义校验包括：

- 快照版本大于当前已提交版本；
- 时间可解析、cluster 非空、算法和键类型合法；
- 物理表 ID 唯一且数据库/表名完整；
- 逻辑表 ID 全局唯一；
- 区间 ID 表内唯一、lower <= upper、单点区间必须双闭；
- 按排序后的相邻区间检测重叠：`prev.upper > cur.lower`，或端点相等且双方都包含；
- 每个区间引用存在的物理表；
- 聚合视图 ID 唯一、依赖表存在且不重复；
- 增量 `base_version` 必须等于锁内读到的当前版本。

### 错误码分层

需求同时要求“快照非法统一码”和“五类注入不同码”。实现采用：

- 通用结构/摘要错误：`E_SNAPSHOT_INVALID`；
- 物理表映射缺失：顶层 `E_PHYSICAL_TABLE_MISSING`，同时 `category=E_SNAPSHOT_INVALID`；
- 区间重叠：顶层 `E_RANGE_OVERLAP`，同时 `category=E_SNAPSHOT_INVALID`。

这样调用方既能按精确原因分流，也能按快照非法大类统一处理。

## 提交协议

### 单写者与幂等

所有 `CommitSnapshot` / `CommitIncrement` 都先获取 `commitMu`。在锁内：

1. 计算请求内容指纹；
2. 查询 `commits` 回执；
3. 相同 `commit_id` 且内容指纹一致：返回原结果并标记 `reused=true`；
4. 相同 `commit_id` 但指纹不同：`E_CONFLICTING_REQUEST`；
5. 新请求继续校验和提交。

因此同一基础版本的两个并发增量严格串行：第一个提交把当前版本推进到 N+1，第二个在锁内看到 `base_version != current`，返回 `E_STALE_BASE`。相同 `commit_id` 的并发请求则一个执行、另一个等待后复用回执。

### 意图与主事务

成功校验后：

1. 在独立 SQLite 事务插入 `intents(status=pending)` 并提交；
2. 若注入 `rebuild_interrupt`，返回 `E_REBUILD_INTERRUPTED`，保持 pending 后强杀进程；
3. 开启主事务；
4. 写入新版本 `snapshots`、`logical_tables`、`shard_ranges`；
5. 用一套完整的缓存集合替换 `route_cache`、`route_cache_refs`、`aggregate_cache`、`aggregate_cache_refs`；
6. 写 `commits` 回执；
7. 在同一主事务更新意图为 `committed`；
8. 提交事务；
9. 在 `RWMutex` 写锁下替换整个 `runtimeState`。

读请求在主事务期间仍看到旧 `runtimeState`，不会看到缓存半更新。提交成功后才会一次性看到新快照、新缓存和新版本。

## 路由缓存与级联

每张逻辑表有一份 `RouteCache`：

- `physical_tables`：去重排序的物理表清单；
- `boundary_index`：区间 ID、上下界、开闭端点、物理表、权重；
- `cache_version`：该派生缓存最近构建版本；
- `expected_version`：该表定义最近一次变化应达到的版本；
- `checksum`：物理表清单 + 边界索引的规范化摘要；
- `refs`：对物理表、分片键、算法、边界摘要和版本的引用行。

每张表还有 `expected_version`，允许未变表继续保留旧缓存版本。正常提交后 `expected_version <= cache_version`；只有传播滞后故障态会出现 `expected_version > cache_version`，此时读请求拒绝 `E_STALE_READ`。

增量提交时：

- 变更表：删除/重建 route cache，版本设为新版本；
- 未变表：保留旧 `cache_version` 和 `expected_version`；
- 受影响聚合视图：根据 `depends_on` 与变更表集合求交，重建 aggregate cache；
- 未受影响聚合：保留；
- 所有写操作与快照、区间、回执在同一事务中完成。

聚合缓存的 payload 包含来源表、来源路由缓存版本、物理表并集和视图类型；引用表记录 `logical_table` 与 `route_cache_version`。因此可从任一缓存反查其物理表、版本和依赖关系。

## 路由计算

`routing.Compute` 是纯函数，不访问时间、随机数或数据库。

### 槽位与键值

- `mod`：`slot = floorMod(int64(key), modulus)`；
- `hash`：`slot = FNV-1a64(seed_le_bytes || key_bytes) mod modulus`；
- `range`：键值本身就是匹配坐标，不做哈希或取模。

固定种子保存在表算法参数中，因此跨进程一致。

### 等值与 IN

等值先将键映射到槽位/坐标，再收集包含该点的区间。IN 对每个值执行等值后合并 hits。

### 范围

查询必须包含 `lower`、`upper`、`include_lower`、`include_upper`。

- `range` 算法：把查询区间与每个分片区间分别转换为整数闭区间 `[first,last]`，判断两个整数集合是否相交；显式处理开闭边界和空区间。
- `mod` 算法：把连续键区间映射成最多两段连续余数区间；若跨度覆盖完整模数则覆盖 `[0,modulus-1]`。跨模边界不会退化为“全部分片”。
- `hash` 算法：无法从有界数值范围推导所有字符串原像，返回全部映射分片并标记 `conservative=true`。

hits 按 physical、range、slot 稳定排序；物理表集合去重后再排序。

## 版本绑定读

```
if observed > current                -> E_OBSERVED_AHEAD
if observed < current                -> E_STALE_READ(observed_version_replaced)
if expected > cache                  -> E_STALE_READ(cache_propagation_incomplete)
otherwise                            -> compute + annotate versions
```

成功响应包含：

- `observed_version`：请求版本；
- `current_version`：当前完整提交版本；
- `cache_version` / `used_version`：实际路由缓存版本；
- `expected_version`：表定义应达到版本；
- `version_bound=true`、`deterministic=true`。

旧观察版本即使对应某张未变表的旧缓存，也不返回结果，因为全局提交版本已被替换。当前版本下未变表的旧缓存可安全返回，但 `used_version` 会明确暴露实际版本，避免冒充新版本。

## 崩溃恢复

启动顺序：

1. 打开 SQLite，启用 WAL、`synchronous=FULL`、busy timeout 和外键；
2. 建表；
3. 扫描 `intents.status='pending'`；
4. 查询目标版本是否存在于 `snapshots`：
   - 存在：更新为 `committed / recovery_reconciled`；
   - 不存在：更新为 `aborted / recovery_aborted`；
5. 读取最后完整快照和所有缓存；
6. 从快照重新构建“预期 route cache/aggregate cache”并逐项比较；
7. 缺失、数量不符、摘要/边界/依赖漂移时，在单事务内用最后完整快照重建全部缓存；
8. 将恢复报告写入 `recovery_reports`；
9. 加载 `runtimeState`。

恢复期发现缓存漂移时，也会先独立写入 `kind=cache_recovery` 的 pending 意图，再在缓存重建主事务内把它标为 committed；若再次中断，下一次恢复仍可依据目标版本是否已落盘做出事实裁决。

报告包含实际读取到的当前版本、pending 数、每个意图的决策、缓存动作和表/视图清单。`facts_only=true` 明确报告不包含推测数据。

`rebuild_interrupt` 故意发生在意图独立事务之后、主事务之前；SIGKILL 后主事务从未开始，因此快照仍为上一完整版本，pending 意图在重启时被中止。这种设计也覆盖了主事务中途进程死亡：SQLite 自动回滚整个主事务，意图保持 pending，恢复逻辑同样判定并修复。

## SQLite 表

| 表 | 用途 |
|---|---|
| `snapshots` | 版本、摘要、规范化完整 JSON、创建时间 |
| `logical_tables` | 每版本逻辑表字段与算法 JSON |
| `shard_ranges` | 每版本区间上下界、端点、物理表、权重 |
| `route_cache` | 当前派生路由缓存 |
| `route_cache_refs` | 缓存对物理表/键/算法/版本/边界的引用 |
| `aggregate_cache` | 下游聚合物化缓存 |
| `aggregate_cache_refs` | 聚合对来源表与来源缓存版本的引用 |
| `commits` | commit_id、内容指纹、类型、版本、响应回执 |
| `intents` | 提交前意图、阶段、状态、目标版本、payload |
| `recovery_reports` | 持久化恢复报告 |

## 可复现异常注入

- `mapping_missing`：改写首个区间引用不存在物理表，触发 `E_PHYSICAL_TABLE_MISSING`；
- `range_overlap`：制造相邻区间重叠，触发 `E_RANGE_OVERLAP`；
- `base_regression`：提交时把增量基础版本改为 `current-1`，触发 `E_STALE_BASE`；
- `observed_ahead`：查询时把观察版本设为 `current+1`，触发 `E_OBSERVED_AHEAD`；
- `rebuild_interrupt`：意图落盘后返回 `E_REBUILD_INTERRUPTED` 并调度进程强杀。

前四类在状态变更前失败；第五类只留下可审计 pending 意图，不改变已提交快照。

## 已知取舍

- 这是本地单进程应用；跨进程多写者依赖文件锁/SQLite 写锁，但应用层 `commitMu` 只在单进程内生效。
- `hash` 范围查询采用显式保守全分片，而非枚举无限键空间；响应通过 `conservative` 标注。
- 路由只计算目标物理表；真实行级读写、事务分发和跨库分布式事务不在本应用范围。
- 物理表权重持久化并参与缓存摘要，但不改变目标表计算；权重为后续负载均衡策略保留。
