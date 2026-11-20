# 执行与数值契约

## 1. 计划格式

计划是 JSON 对象，顶层只允许 `version`、`dataset`、`operators`。`version` 缺省为 `1`。`operators` 中最多一个 `filter`，且必须位于第 0 位；随后只能选择 `project` 或 `aggregate` 其中一种输出模式。

```json
{
  "version": 1,
  "dataset": "small",
  "operators": [
    {
      "op": "filter",
      "predicate": {
        "op": "compare", "fn": "ge",
        "left": {"op": "column", "name": "id"},
        "right": {"op": "literal", "dtype": "int64", "value": 0}
      }
    },
    {
      "op": "project", "name": "ratio",
      "expr": {
        "op": "binary", "fn": "div",
        "left": {"op": "column", "name": "f64_a"},
        "right": {"op": "column", "name": "f64_b"}
      }
    }
  ]
}
```

表达式节点：

| `op` | 字段 | 类型规则 |
| --- | --- | --- |
| `column` | `name` | 列必须存在 |
| `literal` | `dtype`, `value` | `dtype` 为 `int64`、`float64` 或 `bool`；`value` 可为 `null` |
| `unary` | `fn`, `arg` | `neg` 仅限数值；`not` 仅限布尔 |
| `binary` | `fn`, `left`, `right` | `add/sub/mul/div`；两侧 dtype 必须完全相同 |
| `compare` | `fn`, `left`, `right` | `eq/ne/lt/le/gt/ge`；两侧 dtype 必须相同；布尔仅允许 `eq/ne` |
| `logical` | `fn`, `left`, `right` | `and/or`；两侧均为布尔 |
| `conditional` | `condition`, `then`, `else` | 条件为布尔；两个分支 dtype 必须相同 |

不做隐式数值提升。非法计划、未知列、类型不匹配、输出名重复等都在创建批次之前返回 `E_PLAN_INVALID`，不会写入会话或批次统计。

## 2. 空值、条件与比较

- 空值位图约定：`true` 表示值有效，`false` 表示 `null`。
- 算术、比较、`and`、`or` 都采用严格传播：任一操作数为 `null`，结果为 `null`。`and/or` 不采用 SQL 三值逻辑的短路真值表。
- `Filter` 谓词只有 `true` 才保留该行；`false` 与 `null` 都过滤掉。
- `conditional` 的显式规则：条件为 `true` 取 `then`；条件为 `false` 或 `null` 取 `else`。被选中分支为 `null` 时结果仍为 `null`。两条路径都遵循同一规则，向量路径不做分支短路。
- 浮点比较使用 IEEE-754 精确比较，不注入 epsilon。NaN 与任何值（包括自身）`eq/lt/le/gt/ge` 为假，`ne` 为真；空值仍由位图单独表示。

## 3. 整数、除零和浮点

- 整数类型固定为有符号 64 位。
- `integer_overflow=wrap` 使用二进制补码回绕；`integer_overflow=saturate` 饱和到 `[-2^63, 2^63-1]`。标量与向量实现逐元素相同。
- 整数除法向零截断，不采用 Python 的向下取整；负数、`INT64_MIN / -1` 均有相同路径规则。
- 除数为 0（整数或浮点）的结果为 `null`，不是异常，也不是 `inf`。
- 浮点运算为 IEEE-754 binary64。除零按上一行定义为 `null`；其他浮点溢出可产生 `±inf`，JSON 显示为 `"Infinity"`。
- 浮点 JSON 输出只做展示舍入：默认固定 6 位小数、Python `format` 的 round-half-even，并输出 `"float_format"` 说明。NPZ 下载保存完整 binary64 值。需要全精度时应下载 NPZ。
- 两路径审计使用 `rtol=1e-12, atol=1e-12`，并报告最大绝对/相对偏差；整数与布尔要求位级相等。

## 4. 聚合

支持 `sum`、`avg`、`min`、`max`、`count`、`count_star`。`count_star` 不接收表达式，统计过滤后行数；`count` 忽略空值。

- 空输入、过滤后为空、全空列：`count/count_star` 为 `0`；`sum/avg/min/max` 为 `null`。
- 整数 `sum` 先精确累加，再按声明的溢出策略回绕或饱和；`avg` 使用精确整数和除以有效计数并输出 `float64`。
- 浮点 `sum` 使用两路径共享、按行顺序累积的 Neumaier 补偿求和。`avg` 使用同一补偿和除以有效计数。
- 聚合结果固定为一行。

## 5. 路径、批大小与内存

- `execution.path` 支持 `auto/scalar/vector`。显式值总是优先。
- `auto` 仅当 `rows <= 50000`、`rows * dataset_columns <= 400000` 且计划复杂度 `<= 48` 时选择逐行路径；否则选择向量路径。阈值可由 `VECENGINE_SCALAR_MAX_ROWS`、`VECENGINE_SCALAR_MAX_CELLS`、`VECENGINE_SCALAR_MAX_NODES` 覆盖。
- 自适应批大小以内存预算的 35% 为工作区，按每行临时槽位估算，取不超过目标值的 2 的幂，再夹到 `[512, 65536]` 且不超过总行数。调用方可显式指定边界内的批大小。
- 预检同时计算最坏情况输出列和批工作区峰值。任何估计或实际分配超过 `memory_budget_bytes` 都返回 `E_MEMORY_BUDGET`，不降低精度或静默缩小语义。
- 预算约束针对每批中间列与结果列；已加载的只读样本缓存不重复计入每批工作区。
- 选择结果始终输出 `path`、`batch_size`、推断依据、阈值、复杂度和内存估算。

## 6. 持久化、幂等与恢复

SQLite 启用 WAL、`synchronous=FULL` 和外键。每个执行写操作先提交意图记录，再在同一事务里提交业务行并完成意图：

- 批次意图先写入；批次统计、`completed_batches` 和 `processed_rows` 检查点同事务提交。
- 结果 NPZ 先写临时文件并原子替换，最后 `READY` 状态、结果哈希、结果路径和 finalize 意图在同一事务提交。
- 只有 `READY` 会话可读取结果；因此读端不会看到“结果已发布但批次统计仍是旧中间状态”。
- 同一 `request_id` 与相同规范化载荷只执行一次。并发重复请求等待首个执行完成并复用同一会话/结果；同 ID 不同载荷返回 `E_DUPLICATE_REQUEST`。
- 不同请求在单写者锁下串行执行；只读 HTTP 查询使用独立 SQLite 连接并发读取 WAL。
- 重启扫描 `queued/running` 会话及遗留 pending 意图。当前策略是标记 `aborted`，报告最后一个已提交批次和检查点，不承诺恢复内存中的结果。若进程在结果文件重命名后、最终事务前退出，未被 DB 引用的文件会清理。

## 7. 业务码

| 业务码 | 含义 |
| --- | --- |
| `E_PLAN_INVALID` | JSON、计划、类型、列名、执行参数非法 |
| `E_MEMORY_BUDGET` | 预计或实际内存超过预算 |
| `E_SESSION_NOT_FOUND` | 会话/路由不存在（路由另有 `E_ROUTE_NOT_FOUND`） |
| `E_DUPLICATE_REQUEST` | 同请求 ID 载荷冲突或等待超时 |
| `E_SESSION_NOT_READY` | 结果尚未发布 |
| `E_SESSION_ABORTED` | 进程中断，无完整结果 |
| `E_RESULT_NOT_FOUND` | 已登记结果文件丢失 |
| `E_EXECUTION_FAILED` | 已归类的执行失败 |

所有 API 错误均返回 `{"ok":false,"error":{"code","message","details"}}`。
