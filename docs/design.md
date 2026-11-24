# DriftLoom 技术设计

## 1. 目标与边界

DriftLoom 在本地读取确定性 JSON 事实，完成“快照校验 -> 漂移裁决 -> 依赖批次 -> 定点代价 -> 写回调度 -> HTTP 写回 -> 读回复核 -> SQLite 留痕/恢复”的完整闭环。

设计边界：

- 仅面向 Kubernetes 风格的本地确定性对象模型，不是通用 Kubernetes API Server。
- 仅使用 Go 标准库与纯 Go SQLite 驱动 `modernc.org/sqlite`，不依赖 CGO、Docker 或外部网络。
- 写回与复核端点只允许 HTTP 回环地址；不提供 TLS、认证或外部目标访问。
- 漂移事实、批次和预算均为确定性结果；HTTP 延迟、实际调度墙钟时间和 SQLite 自增 ID 是唯一非确定输入。

## 2. 组件与数据流

```text
samples/*.json
    |
    v
internal/model       严格解码、类型归一化、预算/依赖/摘要校验
    |
    v
internal/detect      精确路径 + 类别分桶路径 + 全对象对账
    |
    +--> 逐对象漂移结论（字段、双方值、来源、容差、原因）
    |
    v
internal/batch       Tarjan SCC、依赖分层、稳定切批、批次签名
    |
    v
internal/plan        整数微秒、定点比例、舍入审计、预算占用
    |
    v
internal/store       SQLite WAL、意图先行、快照/批次/写回/轮次
    |
    v
internal/engine      预算准入、并行/队列、HTTP 写回、读回复核、单飞
    |
    v
internal/web         嵌入式中文控制台与 JSON API
```

CLI 入口位于 `cmd/driftctl`；固定样本位于 `samples/`；验证产物位于 `artifacts/`。

## 3. 输入模型与事务边界

### 3.1 对象和字段

对象包含 `id`、`kind`、`namespace`、`expected`、`actual`、`sources`、`dependsOn`、`resourceBudget`。字段值显式区分 `string`、`int`、`float`：

```json
{"type": "float", "value": "0.7502"}
```

解析阶段：

- JSON 使用 `DisallowUnknownFields` 并拒绝尾随数据。
- 对象 ID 去空白后必须非空且全局唯一。
- `kind`、`namespace` 必须非空。
- 依赖去重、稳定排序，且每个依赖必须指向快照内对象。
- `resourceBudget` 必须为非负 int64；逐对象累加时检查 `MaxInt64` 溢出。
- 整数字段规范化为十进制 int64 文本；浮点字段保留原始十进制文本，并以 `big.Rat` 验证。
- 字段来源标记不得引用不存在的字段。
- 对象按 ID 稳定排序，为后续批次和报告提供唯一顺序。

任何校验失败均返回 `E_SPEC_INVALID`，且在快照提交事务前终止，不留下部分已提交状态。

### 3.2 策略快照

策略包含版本号、生成时间、精确路径阈值、批次上限、依赖顺序规则、单轮预算、时间预算相对容差、浮点相对 ppm 容差、最大并行批数、写回队列容量、重试上限、写回超时和预览样本数。

策略校验包括：

- 版本号、生成时间、阈值、批次上限、并行数和超时为正且在安全范围。
- 队列容量、重试数和 ppm 容差在声明范围内。
- 依赖顺序规则必须是 `dependencies_first`。
- `checksum` 必须是规范化 JSON（将 checksum 置空后）的 SHA-256。
- 集群快照版本必须与策略版本一致。

提交时若 `incomingVersion <= currentVersion`，返回 `E_STALE_SNAPSHOT`。状态检查位于写入事务之前；并发插入冲突也会按 stale 处理。

## 4. 漂移判定

### 4.1 四类漂移

对象字段按名称稳定排序，逐字段比较：

| 条件 | 类别 |
|---|---|
| 期望有、实际无 | `field_missing` |
| 期望无、实际有 | `field_extra` |
| 两侧都存在且值/类型不等 | `value_offset` |
| 两侧都存在且来源标记不同 | `source_conflict` |

同一字段可同时产生来源冲突和值偏移；同一对象输出全部类别。类别按严重度稳定排序：

```text
source_conflict(4) > value_offset(3) > field_missing(2) > field_extra(1)
```

因此来源冲突始终是首要漂移。每个字段结论携带字段名、双方值、双方来源（适用时）、容差、允许差、实际差和原因。

### 4.2 浮点数值契约

字符串和整数严格相等。浮点不使用裸 `float64`：

```text
abs(expected - actual) <= max(abs(expected), abs(actual)) * relativePpm / 1,000,000
```

两侧十进制文本转为 `big.Rat`，允许差和实际差以十进制有理数比较。相等等价于“在容差内”，边界值也被接受。`big.Rat` 仅作为比较表示，原始文本保留，避免把有效数字格式化丢失。

### 4.3 精确路径与类别分桶路径

- 精确路径：逐对象扫描字段并直接形成结论，记录每对象耗时和 `per_object` 依据。
- 分桶路径：先独立形成对象结论，再按四个类别维护对象 ID 集合，按类别输出桶大小和抽样。

主路径选择规则：

```text
objectCount <= exactObjectLimit  -> exact
objectCount >  exactObjectLimit  -> bucketed
```

为满足“两路径结论一致”，实现始终同时计算两路并对全部对象逐项比较：`hasDrift`、类别列表、首要类别、字段漂移数量、字段名、类别、容差、原因、双方值。任何差异返回 `E_PATH_DIVERGENCE`，不提交分析或批次状态。

在对象结论一致后，引擎还会分别用精确结论和分桶结论组织批次，并比较两条批次 SHA-256 签名；`exactBatchSignature`、`bucketedBatchSignature`、`batchPathReconciled` 直接进入分析报告。若签名不同，同样返回 `E_PATH_DIVERGENCE`。

## 5. 依赖批次

### 5.1 环检测与范围

1. 用 Tarjan SCC 找强连通分量；大小大于 1 的分量或自环视为依赖环。
2. 环上对象不进入批次，统计为 `cyclicObjects`。
3. 直接或间接依赖环的对象不可安全调度，统计为 `blockedByCycleCount`。
4. 其余 DAG 仍生成批次。
5. 若存在环，报告仍包含环外批次，同时返回 `E_DEPENDENCY_CYCLE` 并附环上对象。

### 5.2 稳定分层与拆分

对无环对象计算最长依赖深度：

```text
level(object) = 1 + max(level(dependency))
```

被依赖对象层级严格小于依赖者。每层内按对象 ID 字典序排序，再按 `batchSizeLimit` 顺序切块。无漂移对象不进入批次，但计入 `cleanObjects`。

批次索引从 1 单调递增；批次依赖索引重新按对象依赖边计算并稳定排序。每个批次包含对象 ID、依赖批索引、最高严重度、首类集合。

### 5.3 稳定签名

批次稳定签名：

```text
sha256(canonical_json(batches))
```

签名可由对象集合、依赖边、漂移结论和策略参数唯一重现。验收同时比较精确路径、分桶路径和选中路径的签名。

## 6. 定点写回计划

### 6.1 单位与公式

所有时间使用整数微秒；比例保留 6 位定点。每个对象代价：

```text
object_cost = 20,000us
            + 10,000us * affected_drift_fields
            + round_half_away(budget * 3,000 / 1,000,000)

batch_cost  = 750,000us + sum(object_cost)
```

`3,000ppm = 0.3%` 是资源预算权重。比例使用整数分子/分母，舍入模式为 `half_away_from_zero`。

每个预算加权步骤记录分子、分母、舍入结果、余数和误差。误差单位是 `1e-6us`，批次和总计输出 `TotalRoundingErrorFixedMicros`，因此可以逐项复核误差传播。

### 6.2 占有率与预算比较

批次和总体预算占有率均以 `cost / budget` 计算到 6 位小数，并记录定点商和精确余数。

单轮预算准入使用：

```text
candidate = acceptedEstimatedCost + batchCost
allowed   = roundTimeBudget + roundHalfAway(roundTimeBudget * tolerancePpm / 1,000,000)
```

候选值大于允许值时跳过；容差内接受。每个决定记录双方微秒值、容差微秒、ppm、是否在容差内、批次序号和说明。就绪批次按 `severity_rank_desc -> batch_index_asc` 稳定排序，容差内以批次序号稳定打破平局。

## 7. 调度、队列与执行

### 7.1 预算准入

批次调度前先按依赖就绪顺序建立确定性预算计划。预算拒绝一个批次后，其依赖批次以 `dependency_skipped` 进入跳过清单；未得到准入状态的批次也稳定列入跳过清单。已接受批次的估计值不会因后续跳过而回退。

### 7.2 并行与有界队列

- 同时运行批次数不超过 `maxParallelBatches`。
- 额外就绪批次进入容量为 `writeQueueCapacity` 的等待集合。
- 等待集合溢出时停止准入，返回 `E_WRITE_QUEUE_FULL`。
- 已经开始的批次继续完成并保留结果；未开始批次列入 `write_queue_full` 跳过原因。
- 每轮另有按墙钟计时的 deadline；超出预算加容差后，未派发批次以 `round_wall_budget_exhausted` 跳过。

每个批次内对象按写回计划顺序执行。每对象每次尝试先写 `pending` 意图和 `prepared` 写回记录，再发起 HTTP。

### 7.3 HTTP 写回与读回复核

写回请求为 `POST /v1/write`，成功后立即 `GET /v1/read?objectId=...`。每次 HTTP 尝试使用独立 `context`、独立 `http.Transport`、禁用 keep-alive 和重定向，并设置连接、响应头和总超时。

- 超时或网络超时取消请求并关闭底层连接，分类为 `E_WRITE_TIMEOUT`。
- Windows/Linux 常见拒绝连接文本分类为 `E_ENDPOINT_REFUSED`。
- HTTP 2xx 和响应 `ok=true` 只表示传输成功，不表示收敛。
- 读回字段必须使用同一个漂移内核重新判定；仍有任何字段漂移则对象为 `unresolved`，保留残留类别和字段。
- 读回失败分类为 `E_READBACK_FAILED`；其他写失败为 `E_WRITE_FAILED`。

对象终态写回记录和读回后的实际字段会在同一 SQLite 事务中提交。

## 8. SQLite 持久化与恢复

### 8.1 表

| 表 | 内容 |
|---|---|
| `policy_snapshots` | 版本、生成时间、摘要、策略 JSON |
| `cluster_objects` | 期望/实际/来源事实和预算 |
| `drift_results` | 每对象漂移结论 |
| `batch_versions` | `staging|complete|aborted` 批次版本与计划载荷 |
| `batch_entries` | 每批次条目 |
| `write_intents` | `pending|committed|rolled_back` 意图 |
| `write_records` | 每次 HTTP 尝试与终态 |
| `intent_log` | 意图事件审计日志 |
| `rounds` | 轮次状态、结果 JSON、错误码 |

SQLite 使用 WAL、`synchronous=FULL`、外键、5 秒 busy timeout。Store 进程内 `writeMu` 串行化写者，读查询可并发使用数据库一致性快照。

### 8.2 两阶段意图

所有持久化写操作遵循：

```text
1. 插入 pending 意图并写 intent_log
2. 在事务中提交实际数据
3. 将意图标记 committed
```

分析提交、批次版本提交、对象写回均使用该顺序。若第 3 步失败，恢复扫描会依据已落盘的实际数据判定应提交还是回滚。

### 8.3 恢复判定

`Recover` 只读取已落盘事实：

| 观察 | 动作 |
|---|---|
| 快照数据存在 | 意图 committed |
| 快照数据不存在 | 意图 rolled_back |
| 批次版本 complete | 意图 committed |
| 批次版本 staging/其他 | 标记 aborted，意图 rolled_back |
| 写回记录已是终态 | 意图 committed |
| 写回记录仍为 prepared/running | 改为 pending_writeback，意图 rolled_back |
| 轮次仍为 running | 改为 aborted，错误码 `E_RECOVERED_INTERRUPTION` |

恢复报告同时给出扫描数、回滚数、待写回数、中止批次数、中止轮次数、最后完整批次版本和逐意图动作。`analyze`、`run`、`demo`、`web` 启动时都会先自动扫描；`recover` 也可单独运行。

## 9. 幂等与并发

轮次 ID 去重分两层：

1. SQLite `rounds.round_id` 是主键；已有终态直接反序列化复用。
2. 进程内 `rounds` 单飞表保存 `done` channel；同 ID 并发调用等待首个执行完成并复用同一报告。

同 ID 重放设置 `reused=true`，不新增写回记录。若发现别的进程留下的 `running` 轮次，执行端短暂等待其终态；恢复扫描负责最终中止并留痕。

## 10. 安全与回环约束

- 模拟器和 Web 服务只允许绑定回环监听地址。
- Web 请求按 `RemoteAddr` 再次验证回环客户端。
- 写回端点必须是绝对 HTTP URL，主机必须是 `localhost` 或回环 IP，禁止凭据、查询和片段。
- 写回客户端禁止代理、重定向和 keep-alive，限制响应体为 4 MiB。
- 样本和输出路径可用于本地验收；程序不主动访问未授权目标。

## 11. 关键代码索引

| 主题 | 文件 |
|---|---|
| 样本/策略模型 | `internal/model/model.go`, `internal/model/load.go` |
| 漂移与双路径 | `internal/detect/detect.go` |
| 环检测与批次 | `internal/batch/batch.go` |
| 定点数值 | `internal/fixed/fixed.go` |
| 写回计划 | `internal/plan/plan.go` |
| 调度/写回 | `internal/engine/scheduler.go`, `internal/engine/writeback.go` |
| 分析入口与单飞 | `internal/engine/engine.go` |
| 持久化/恢复 | `internal/store/store.go`, `internal/store/recovery.go` |
| 本地端点 | `internal/simulator/simulator.go` |
| Web 控制台 | `internal/web/server.go`, `internal/web/static/index.html` |

## 12. 已知取舍

- 验收要求两路径逐对象一致，因此即使选择一条主路径，也会运行另一条影子路径并做对账，判定成本约为单路径的两倍。
- 为保证结果可重建，批次优先按依赖层级而非负载均衡切分；批次大小上限只控制每层内的稳定切块。
- 环上对象及其依赖者不进入批次，避免违反“被依赖先写”的硬约束；环外独立 DAG 仍输出。
- 跨进程并发以 SQLite 事务和 busy timeout 兜底；复杂的跨进程调度租约不在本项目范围内。

