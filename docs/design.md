# 技术设计

## 1. 组件边界

请求路径从外到内为：

1. `internal/httpapi`：本地 JSON API、统一错误信封、方法/路径校验。
2. `internal/service`：快照原子替换、请求规范化与指纹、并发幂等、精度路径选择、`1..P` 分区对照、事务编排、崩溃注入。
3. `internal/engine`：确定性哈希分区、分区内稳定排序、Join、偏斜统计、分组部分和与稳定归并。
4. `internal/sum`：朴素、Kahan、DoubleDouble 定点三种求和器，误差模型和状态摘要。
5. `internal/snapshot`：确定性列式二进制格式、内嵌/外部 SHA-256 和原子写文件。
6. `internal/store`：SQLite schema、意图日志、请求状态、规范化结果表、恢复扫描。

`cmd/samplegen`、`cmd/server` 和 `cmd/experiment` 分别提供样本、服务和端到端可重复实验入口。

## 2. 快照格式

所有整数为小端。布局如下：

| 偏移/长度 | 字段 |
| --- | --- |
| 0 / 8 | magic `CLSNP01\0` |
| 8 / 8 | `uint64 version` |
| 16 / 8 | `int64 generated_at_unix_nano` |
| 24 / 8 | `uint64 rows` |
| 32 / 4 | `uint32 side_len` |
| 36 / 4 | `uint32 flags`，当前必须为 0 |
| 40 / side_len | `left` 或 `right` |
| 后续 / rows*8 | `int64 keys[]` |
| 后续 / rows*8 | `float64 measures[]` 的 IEEE-754 位模式 |
| 后续 / ceil(rows/8) | 空值位图，置 1 表示空 |
| 末尾 / 32 | 前面全部正文的 SHA-256 |

外部 `<file>.sha256` 使用标准 `sha256sum` 行格式，摘要覆盖完整文件。加载同时验证：

- magic、flags、side、行数和精确文件长度；
- 内嵌正文摘要；
- 外部完整文件摘要；
- 请求携带的期望摘要（若有）；
- 非空度量必须有限；
- 左右 side 匹配，版本为正且在 SQLite `int64` 范围内。

样本生成使用自实现 SplitMix64 和固定生成时间，避免 Go `math/rand` 版本差异。左/右表使用不同派生种子。

## 3. 快照替换一致性

`Service.LoadSnapshots` 串行化加载：

1. 完整读取并解析左右文件；任何错误立即返回 `E_SNAPSHOT_INVALID`。
2. 与当前不可变快照对比较，要求两侧版本严格大于当前两侧版本。
3. 在一个 SQLite 事务中 upsert `snapshots` 两行；失败则不替换。
4. 事务成功后在单次指针写锁内整体替换 `*SnapshotPair`。

读取方只取得不可变指针快照；加载方不会修改旧列切片，因此查询/计算不会混用新旧列。失败路径在写锁前返回，旧指针和旧数据库元数据都保持原样。

## 4. 哈希分区、排序与 Join

对每个整数键执行 SplitMix64，取模得到分区号。相同键在左右两侧天然得到相同分区。

分区内每侧使用 `sort.SliceStable`，比较顺序为：

1. 非空度量优先；
2. `abs(measure)` 升序；
3. 键升序；
4. 源索引升序。

右表排序后构建 `key -> []recordRef` 索引，匹配列表保持稳定排序结果。每个左记录按稳定顺序访问：

- `inner`：无匹配则跳过；
- `left`：无匹配输出一行，右度量视为空，组计数加一。

匹配行的合成度量为：左/右有效分量相加；只有一个有效时使用该分量；都无效时为空。`group_count` 统计连接输出行数，`value_count` 统计至少一个有效分量的行数。

分区统计包括左右记录数、连接输出数、字节估算、记录占比、是否偏斜和受影响键。字节估算固定为 `records*16 + joined*8 + distinct_keys*8`，保证同一输入可复现。

## 5. 求和与误差

部分和对象保存：

- `count`、`value_count`、`abs_sum`；
- 朴素和或 Kahan `sum/compensation`；
- DoubleDouble 的 `hi/lo`、定点整数十进制字符串；
- 对算法、参数、键、分区和全部中间状态计算的 SHA-256 摘要。

DoubleDouble 定点路径：

1. 每个有限 `float64` 由符号、尾数和二进制指数精确分解。
2. 按 `scale_bits` 将值舍入到 `2^-scale_bits` 网格，得到任意精度整数。
3. 分区内用 `big.Int` 精确累加；同时维护 DoubleDouble 的误差自由变换状态用于审计。
4. 归并时直接相加定点整数，因此结果与部分和到达/排序顺序无关。
5. 最终用高精度 `big.Float` 除以 `2^scale_bits` 并转回 `float64`。

误差声明按分组保存。对 `n` 个值和 `u=2^-53`：

- 朴素：`gamma_(n-1) * sum_abs`；
- Kahan：`(2u+n*u^2) * sum_abs`；
- 定点：`n*2^-(scale_bits+1) + u*|sum|`。

均值上界为 `sum_bound/n + u*|average|`，相对上界按均值归一；和与均值上界都进入分组结果、全局误差声明和 SQLite `group_results`。

所有可能为 `Inf/NaN` 的保守上界在进入 JSON 前被替换为有限最大值，避免响应编码失败。

## 6. 稳定归并与分区对照

支持的归并规则：

- `partition_ascending`：分区号升序，再按键；适合快速路径。
- `magnitude_bucket_partition_asc`：`floor(log2(abs(partial)))` 量级桶升序，再按绝对部分和、分区号、键；适合高精度路径。

两者都只依赖已提交输入，不依赖 goroutine 完成顺序或查询时刻。对每个分组生成 `merge_order_digest` 依据的部分排序摘要与 `compensation_state_digest`。

每个请求都强制执行分区数 `1..P` 的完整 Run，不提供关闭开关。以单分区结果作为参考，对每个键计算相对偏差 `abs(actual-reference)/abs(reference)`。有限非零参考使用相对值；参考为零且值不同时直接判定失败。任一偏差超容差，整个请求在当前内存阶段失败；由于尚未进入最终事务，数据库中没有任何可见的部分和或聚合。

注意：任务同时要求“同一键所有记录同分区”和“分区改变求和顺序”。在本实现中，前者意味着特定键的原始记录不会跨分区拆分，因此通常只有一个非空部分和。引擎仍完整实现并验证多分区部分和收集、声明顺序归并和 `1..P` 对照；三种算法的数值差异通过同分区内稳定排序后不同累加策略以及和参考 DoubleDouble 的对比来展示。

## 7. 精度路径选择

`algorithm=auto` 使用以下输入生成显式 `RouteDecision`：

- 请求相对容差；
- 分区数；
- 最大分区记录占比与偏斜阈值；
- 非空度量 `max_abs/min_positive_abs` 量级比；
- 左右记录数与估算连接输出行数；
- `compute_budget`。

严格容差或极端量级比优先 DoubleDouble；中等严格容差、多分区或偏斜优先 Kahan；宽容量级且预算紧张可下降为朴素。每次决定都输出理由数组，不静默改变用户显式指定的算法。`budget_satisfied` 单独报告，预算不足不会覆盖更严格精度目标。

## 8. SQLite 状态机与事务

请求状态：

```text
(none) --begin attempt--> running --final txn--> completed
                               \------business failure--> failed
running --startup/recovery scan--> aborted --same-content retry--> running
```

`completed` 和 `failed` 是最终状态，相同请求直接复用存储响应/错误。`aborted` 表示上次尝试没有完整提交，可用相同指纹重新尝试；不同指纹始终返回 `E_CONFLICTING_REQUEST`。

事务一（意图）：

1. 新建请求或把 `aborted` 请求更新为 `running`、`attempt+1`；
2. 写 `pending` 意图；
3. 提交后才开始计算。

事务二（原子结果）：

1. 校验请求仍为 `running`；
2. 原子递增 `meta.commit_seq`；
3. 删除该请求可能存在的旧规范化结果（防御性）；
4. 写 `job_plans`、全部 `partitions`、全部 `partial_sums`、全部 `group_results`；
5. 写完整响应 JSON，把请求更新为 `completed`，意图更新为 `completed`；
6. 单事务提交。

所以不可能观察到“部分和已写入但分区统计未更新”或“聚合可查询但部分和缺失”。进程在事务二任一位置被强杀时 SQLite 回滚整个事务，只留下 `running + pending`，启动恢复将请求和意图标记为 `aborted` 并写 `recovery_events`。

## 9. 幂等与并发

规范化请求后计算 SHA-256 指纹。进程内 `request_id -> inflight` 表提供等待通道：

- 首个调用者成为 owner，执行并提交；
- 相同指纹的并发调用者等待 owner 关闭通道，然后重新读取数据库并复用结果；
- 不同指纹在等待前或等待后都检查并返回冲突；
- 若数据库显示 `running` 但没有进程内 owner（例如另一进程遗留），最多轮询等待 30 秒，随后返回 `E_EXECUTION_IN_PROGRESS`。本地单实例在启动时已完成恢复扫描，因此正常运行不会落入该分支。

重复提交已提交请求不会增加 `attempts` 或 `commit_version`。

## 10. 恢复报告

启动时执行 `RecoverRunning`：

- 在单事务内找出所有 `running` 请求；
- 更新为 `aborted`，写入含请求、尝试次数、最后完整提交版本、未提交部分和行数为 0 的原因；
- 将对应 `pending` 意图更新为 `aborted`；
- 写 `recovery_events`；
- 提交后再读取统计。

`GET /v1/recovery/report` 只从 SQLite 的 requests、intents、commit 元数据、规范结果表和 recovery_events 汇总，不根据日志或推测生成。

## 11. 本地实验器

`cmd/experiment` 在临时/指定目录中：

1. 生成两次样本并逐位比较；
2. 构建并启动 `cmd/server`；
3. 通过真实 HTTP 加载快照并提交任务；
4. 对三种算法和 `P={1,4}` 比较；
5. 验证 `auto`、幂等、冲突、损坏快照；
6. 以环境变量匹配指定请求 ID，写入一次性 marker 后 `os.Exit(70)` 强杀；
7. 用同一数据库重启，检查启动恢复事件和中止查询；
8. 相同内容重新提交，比较续算结果与先前 DoubleDouble 提交是否逐位一致；
9. 输出 `report.json`，任何断言失败退出非零。

该流程不依赖 Docker、远程服务或交互式账户。
