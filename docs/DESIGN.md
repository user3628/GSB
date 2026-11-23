# 技术设计

## 1. 范围与运行边界

组件是离线诊断器，不是 HTTP/2 协议栈。它读取本地确定性 JSON，使用 SQLite 保存诊断状态，并通过仅监听回环地址的 HTTP 服务提供提交与查询接口。运行时不发起外部网络请求，不依赖容器。

诊断状态按“样本版本”推进：样本先完整解析并做内存预演，业务错误不会留下半批状态；预演成功后才按事件批次和调度轮次逐版本提交。

## 2. 样本模型

顶层结构：

```json
{
  "sample_id": "baseline-v1",
  "config": {
    "round_bytes": 16384,
    "scheduling_threshold": 32,
    "approximation": "exact",
    "weight_scale_ppm": 1000000,
    "rounding_mode": "truncate_then_largest_remainder",
    "max_scheduling_rounds": 100000,
    "sampling_fallback_below": 8
  },
  "connections": [],
  "streams": [],
  "frames": []
}
```

连接声明初始窗口、最大窗口、初始流窗口、最大流窗口和优先级层数。流声明权重、优先级，并可覆盖流窗口。帧字段为：

```json
{
  "sequence": 1,
  "timestamp": 0,
  "connection_id": "conn-a",
  "stream_id": 1,
  "type": "DATA",
  "flags": ["END_STREAM"],
  "payload_bytes": 4096,
  "window_increment": 0
}
```

`sequence` 是全局唯一序号，决定样本的声明顺序。校验时按 `sequence` 排序后要求 `timestamp` 非递减；执行时按 `(timestamp, sequence)` 分组，因此按流分批上传不会改变执行次序。

## 3. 帧校验与原子预演

下列情况返回 `E_FRAME_INVALID`：

- JSON 解码失败、未知字段或其他尾部内容；
- 未知帧类型或帧标志；
- 非 `SETTINGS` 使用非零流 ID，或 `HEADERS`/`DATA`/`RST_STREAM` 使用流 ID 0；
- 流 ID 小于 0 或大于 `2^31-1`，或引用未声明流；
- 时间戳为负，或按 `sequence` 排序后出现下降；
- `payload_bytes` 为负；
- `window_increment` 为负，或出现在非 `WINDOW_UPDATE` 帧；
- 窗口、权重、优先级、样本配置不合法。

`Compile` 在内存中完整推进样本。只有预演成功才调用 SQLite 提交。由此：

- 非法帧、非法状态迁移、窗口溢出、最终未解除的流控阻塞都不会留下任何样本状态；
- 中途阻塞但后续 `WINDOW_UPDATE` 解除时，样本可以成功完成；
- 预演与持久化使用同一确定性算法。

## 4. 执行时钟

同一 `timestamp` 的所有帧视为一个事件批次。事件批次内先按 `sequence` 应用全部帧，再依次处理连接（连接 ID 升序）的调度轮次。因此同一批次内先排队的 DATA 可以被后续 RST 清除，窗口更新也会在批次调度前生效。

零长度 DATA 不需要窗口字节；若带 `END_STREAM`，可直接完成本地关闭迁移。

## 5. 流状态机

| 事件 | 当前状态 | 目标/效果 |
|---|---|---|
| `HEADERS` 无 `END_STREAM` | `IDLE`、`OPEN` | `OPEN` |
| `HEADERS` 带 `END_STREAM` | `IDLE`、`OPEN` | `HALF_CLOSED_REMOTE` |
| `DATA` | `OPEN`、`HALF_CLOSED_REMOTE` | 排队发送；最后一个 `END_STREAM` 字节发送后迁移 |
| DATA 的 `END_STREAM` 完成 | `OPEN` | `HALF_CLOSED_LOCAL` |
| DATA 的 `END_STREAM` 完成 | `HALF_CLOSED_REMOTE` | `CLOSED` |
| `RST_STREAM` | `OPEN`、`HALF_CLOSED_LOCAL`、`HALF_CLOSED_REMOTE` | `RESET`，清空待发送数据 |
| `WINDOW_UPDATE` 流 0 | 任意连接状态 | 更新连接窗口 |
| `WINDOW_UPDATE` 非零流 | `OPEN`、`HALF_CLOSED_LOCAL`、`HALF_CLOSED_REMOTE` | 更新流窗口，状态不变 |
| `SETTINGS` | 任意连接状态 | 接受设置；可更新诊断轮次字节数 |

其余迁移返回 `E_INVALID_TRANSITION`。`RESET` 是诊断器中的终结状态，不参与加权分配，也不能再接收 DATA 或流级窗口更新。`CLOSED` 同样不再调度。

DATA 是“本地出站需求”。它先进入流内 FIFO，随后由调度器按字节切分发送。DATA 与 `END_STREAM` 只有实际发送完成后才推动本地关闭状态。

## 6. 流控窗口

每个连接维护：

- `initial_window`、`max_window`、`window_remaining`；
- 累计窗口更新字节数。

每条流维护：

- `initial_window`、`max_window`、`window_remaining`；
- `sent_unacked`、`total_sent`；
- 待发送 DATA 片段。

发送前同时检查：

```text
connection.window_remaining >= send_bytes
stream.window_remaining     >= send_bytes
```

调度分配已把发送量限制在两边余量内；实际扣减前仍再次断言。最终仍有待发送字节时，预演返回 `E_FLOW_CONTROL_BLOCKED`，上下文包含连接余量、流余量、剩余待发送字节和原始请求字节。

流级 `WINDOW_UPDATE` 在更新窗口的同时，以 `min(increment, sent_unacked)` 扣减诊断用的未确认计数。连接级更新只增加连接窗口。

窗口更新先做无溢出检查：

```text
new_window = current + increment
new_window <= declared_max
```

不满足时返回 `E_WINDOW_OVERFLOW`，且不会修改该帧目标窗口，也不会提交整批预演状态。

## 7. 精确加权调度

调度器对每个连接循环执行，直到没有可调度 DATA、窗口耗尽或轮次上限到达。单轮预算为：

```text
budget = min(config.round_bytes, connection.window_remaining)
```

优先级采用严格模式：优先级 0 先使用预算，剩余预算才给优先级 1，以此类推。每一优先级片段都有独立的 `round_no`、提交版本和轨迹。

对同一优先级中的流：

```text
demand_i = min(stream.pending_bytes, stream.window_remaining)
```

精确字节分配使用整数最大余数法：

1. 计算 `floor(remaining_budget * weight_i / total_weight)`；
2. 若结果超过 `demand_i`，先分配 `demand_i`，再从剩余预算重算；
3. 所有需求都可容纳时，将不足一整字节的预算按余数降序分配，余数相同按流 ID 升序；
4. 所有乘法与取模使用 `math/big.Int`，避免 `int64` 中间乘积溢出。

轮次轨迹中的发送顺序固定为：

```text
allocated_bytes DESC, weight DESC, stream_id ASC
```

每条选择记录包含流 ID、发送字节、原始待发送字节、权重、归一化份额、选择依据及扣减前后的连接/流窗口。

## 8. 固定点数值契约

所有窗口、字节、权重和份额均为整数。权重份额使用 `1,000,000 PPM`：

```text
W  = sum(weight_i)
exact_numerator_i = weight_i * 1,000,000
base_i = floor(exact_numerator_i / W)
remainder_i = exact_numerator_i mod W
```

先截断求和，再把 `1,000,000 - sum(base_i)` 个 1 PPM 单位按余数降序分配，余数相同按流 ID 升序。每步轨迹记录：

- `exact_numerator`：精确分子；
- `truncated_share_ppm`：截断份额；
- `remainder_numerator`：余数分子；
- `adjustment_ppm`：0 或 1 的最大余数修正；
- `normalized_share_ppm`：最终整数份额。

`round_rounding_error_ppm` 是本轮施加的 1 PPM 修正数，`cumulative_rounding_error_ppm` 是跨轮累计修正数。它们给出可逐位复算的保守修正量，不依赖任何浮点排序。

样本、批次和查询不进行浮点运算。同一输入在完整提交、按流分批提交、提交过程中查询和提交结束后查询时，只要比较同一提交版本，窗口余量、待发送字节、发送份额和轮次轨迹均逐位一致。

## 9. 分桶估计路径

仅当同时满足以下条件时启用估计路径：

```text
config.approximation = "bucketed"
eligible_streams > scheduling_threshold
eligible_streams > sampling_fallback_below
```

分桶与采样：

1. 按流 ID 排序；
2. 以 `ceil(eligible_streams / scheduling_threshold)` 为步长做确定性等距采样；
3. 按 `floor(log2(weight))` 的位长桶分组；
4. 用桶内样本均值外推桶总权重与总需求；
5. 先在桶间做容量受限整数分配，再在桶内用精确权重分配。

轨迹输出采样率 PPM、可读采样比例、分桶方法、置信说明、`exact_order`、`estimated_order` 和 `difference_reason`。精确全量路径始终作为审计基线输出，便于逐项核对两条路径的差异。

估计路径不会改变 SQLite、固定点格式或批次确定性；它只改变该轮预算在流之间的估计分配。

## 10. 规范排序、幂等与批次

样本规范化包括：

- 连接按 ID 排序；
- 流按连接 ID、流 ID 排序；
- 帧按 `(timestamp, sequence)` 排序；
- 帧标志排序并去重；
- 配置默认值补齐。

对规范化 JSON 做 SHA-256，得到样本内容哈希。相同样本 ID：

- 哈希相同且状态 `DONE`：直接复用结果，不增加提交版本；
- 哈希相同且正在推进：同进程内等待单写者锁，之后复用；
- 哈希不同：返回 `E_CONFLICTING_SAMPLE`。

批次 API 将帧按 `batch_id` 保存，最终合并后仍执行上述规范化。批内顺序、批次边界和上传顺序不影响最终规范样本文本。

## 11. SQLite 持久化与意图

主要表：

- `samples`：规范化哈希、状态、当前版本、状态摘要、结果 JSON；
- `connections` / `streams`：当前完整状态；
- `commits`：每个版本的种类、轨迹、完整状态 JSON 和 SHA-256 摘要；
- `intents`：独立落盘的写意图及状态；
- `sample_drafts` / `sample_batches`：分批提交草稿；
- `recovery_reports`：启动恢复报告。

每个状态写操作流程：

1. 独立事务插入 `intents(status=RUNNING)` 并提交；
2. 在一个事务中替换连接/流状态、插入 `commits` 版本、更新 `samples.version` 和 `state_digest`；
3. 独立事务把意图标记为 `COMMITTED`。

因此进程在任意点终止，SQLite 都只会留下意图记录、旧完整版本或新完整版本之一，不会留下半个轮次。

## 12. 启动恢复

服务启动、监听端口前扫描 `RUNNING` 意图：

- 对应 `commits` 版本存在：意图修复为 `COMMITTED`；
- 对应版本不存在：意图标记 `ABORTED`，样本标记 `ABORTED`，保留最后一个完整快照；
- `FINALIZE` 意图特殊处理：只有 `samples.status=DONE` 才视为已提交；相同版本号的调度提交不能冒充完成提交；
- 发现没有意图但状态仍为 `RUNNING` 的样本时，同样回退为 `ABORTED`。

无意图恢复同样会把当前落盘快照与 `samples.state_digest` 比较；不一致时报告 `E_CHECKPOINT_CORRUPT`，不会把损坏表状态描述为可信检查点。

恢复报告包含：

- 意图版本、类型、状态和是否存在提交；
- 最后完整版本、快照摘要和摘要校验结果；
- 根据落盘连接/流表重建的完整快照；
- `basis = SQLite committed rows only`；
- `exit_report_guaranteed = false`。

强制终止时不保证进程有机会输出退出报告。重启后的报告只基于 SQLite 可见事实，并明确给出可复算边界。

## 13. 并发与查询一致性

- 样本级互斥锁保证同一 `sample_id` 只有一个推进者，其他同内容请求等待并复用。
- 全局写锁串行化状态提交；SQLite 使用 WAL。
- 每个调度轮次在一个事务内更新状态、轨迹和版本。
- 当前快照读取放在一个只读事务中，连接、流和摘要来自同一数据库快照。
- 历史快照读取 `commits.state_json` 并重算摘要。
- SQLite 中出现摘要不匹配时返回 `E_CHECKPOINT_CORRUPT`，不返回混合状态。

## 14. 业务错误体

所有业务失败统一返回：

```json
{
  "code": "E_FLOW_CONTROL_BLOCKED",
  "message": "DATA demand remains blocked by flow-control windows",
  "context": {
    "connection_id": "loopback",
    "stream_id": 1,
    "connection_window_remaining": 0,
    "stream_window_remaining": 64511,
    "pending_bytes": 3072,
    "requested_bytes": 4096
  }
}
```

| 业务码 | HTTP | 触发条件 |
|---|---:|---|
| `E_FRAME_INVALID` | 400 | 帧/样本格式、流 ID、时间戳、负增量、未知字段 |
| `E_INVALID_TRANSITION` | 400 | 当前流状态不允许该操作 |
| `E_FLOW_CONTROL_BLOCKED` | 409 | 最终仍有 DATA 受连接或流窗口阻塞 |
| `E_WINDOW_OVERFLOW` | 409 | 窗口更新超过声明上限 |
| `E_CONFLICTING_SAMPLE` | 409 | 同一样本标识内容冲突 |
| `E_CHECKPOINT_CORRUPT` | 500 | 落盘状态与版本摘要不匹配 |
| `E_SAMPLE_ABORTED` | 409 | 崩溃恢复后的样本不可继续复用 |
| `E_SCHEDULER_LIMIT` | 409 | 超过最大调度轮次 |
| `E_NOT_FOUND` | 404 | 路径、样本或版本不存在 |
| `E_METHOD_NOT_ALLOWED` | 405 | HTTP 方法错误 |
