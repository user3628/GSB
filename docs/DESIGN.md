# 设计说明：查询计划与索引选择组件（qopt）

本文档记录组件在数值稳定、误差可界定、计划可解释之间的取舍，
以及代价精度契约、直方图语义、相关性修正与连接基数策略。
原始任务书见 `docs/TASK.md`。

## 模块结构

| 模块 | 职责 |
| --- | --- |
| `qopt/numeric.py` | Kahan-Neumaier 补偿求和、误差上界报告、[0,1] 截断、定点舍入、带容差比较 |
| `qopt/stats.py` | 统计快照模型、全量校验、版本识别与原子替换（`SnapshotRegistry`） |
| `qopt/selectivity.py` | 等值/范围/IN/NULL 谓词选择率、直方图语义、列间相关性修正 |
| `qopt/joincard.py` | 对数域连接基数、左深贪心单遍枚举、笛卡尔积上限策略 |
| `qopt/cost.py` | 代价常量与分解、精度契约、稳定次序的计划比较 |
| `qopt/indexsel.py` | 候选索引前缀匹配、代价评估、逐候选排除原因 |
| `qopt/planner.py` | 串联估算与选择，输出确定性计划 JSON |
| `qopt/server.py` | 本地回环 HTTP API（默认 `127.0.0.1:8787`） |

## 数值稳定与误差上界

- **补偿求和**：直方图桶计数、累计占比、IN 列表求和、计划总代价一律使用
  Kahan-Neumaier 补偿求和，迭代顺序固定。设 `S = Σ|x_i|`，`u = 2^-53`：
  朴素求和误差上界 `(n-1)·u·S`，补偿求和为 `2·u·S`（与 n 无关的领先阶）。
  `summation_error_report()` 同时给出两种上界与实测差值，示例脚本在真实
  直方图与对抗性偏斜序列（`[1e16, 1×1000, -1e16]`，朴素得 0、补偿得 1000）
  上演示该对比。
- **逐位一致**：所有估算路径只用 IEEE-754 binary64 与固定迭代顺序，
  不依赖哈希序、并行或平台数学库；同一快照同一查询重复求值输出逐字节
  一致（演示脚本与测试均断言）。
- **对数域基数**：连接中间基数以自然对数累加（等价于几何平均式累乘），
  避免 `1e200 × 1e200` 这类上溢与选择率连乘下溢；仅在输出阶段转回线性值。

## 直方图语义（等深、右闭）

- 桶 i 覆盖 `(b_i, b_{i+1}]`，首个桶双端闭合；直方图仅统计非 NULL 值，
  NULL 比例单独保存并作为 `(1 - null_frac)` 因子缩放。
- 退化桶（`b_i == b_{i+1}`）视为点质量（重值/hot value）：等值查找优先命中
  点质量桶；范围查询按端点包含性决定是否计入整个桶计数。
- 范围选择率在非退化桶内按均匀分布线性插值；端点包含性只影响点质量桶
  （连续插值下单点测度为零）。点区间 `[v,v]` 仅在双端均包含时计为命中。
- 空表（`row_count = 0`）与零不同值列（`ndv = 0`）一律返回确定性 0.0，
  不触发除零；所有选择率结果截断到 `[0,1]`。

## 列间相关性

- 快照可声明两类相关性（`correlations`）：
  - `functional_dependency`（a→b）：联合选择率取各列选择率的 **max**；
  - `coefficient`（ρ∈[0,1]）：`joint = (1-ρ)·独立估计 + ρ·min(各列)`，
    两个端点都在 `[0,1]` 内，修正结果必然有界。
- 未声明相关性的列组合保持独立假设，并在计划 `assumptions` 中逐条标注
  “independence assumption (no declared correlation)”，已声明的则标注
  “declared …”，保证计划可解释、假设来源可追溯。

## 连接基数与笛卡尔积上限

- 等值连接按包含假设 `|R⋈S| = |R|·|S| / max(NDV_l, NDV_r)`，NDV 缺失报
  `E_STATS_MISSING`，`ndv = 0` 截断为 1 以避免 `log(0)`。
- 连接顺序为**左深贪心单遍枚举**：从最小基数的表开始，每步追加使中间
  对数基数最小的表（表名字典序打破平局），不回溯，候选评估 O(n²)。
- 无连接谓词的笛卡尔积：步骤级 `cartesian: true` 标记 + 显式上限
  `CARTESIAN_CAP = 1e15`，超限时 `cap_applied: true` 并同时报告未截断的
  对数域数值——**不做静默截断**。

## 索引选择与前缀规则

- 前缀匹配：前导列由等值/IN 谓词逐列消费；再允许一个范围谓词列，其后
  列不可用于寻址（转为残余过滤）。前导列无任何谓词的候选直接以
  `prefix_mismatch` 排除。
- 代价常量（契约的一部分，固定）：`SEQ_PAGE=1.0`、`RANDOM_PAGE=4.0`、
  `CPU_TUPLE=0.01`、`CPU_INDEX_TUPLE=0.005`、`CPU_OPERATOR=0.0025`。
- 每个候选输出 CPU/IO/选择率敏感项分解；未中选候选给出具体原因
  （`prefix_mismatch` / 代价超出 / 容差内平局按稳定键落败）。

## 代价精度契约

- JSON 输出的代价分项（`cpu_cost`、`io_cost`、`total_cost` 及信息性子项
  `selectivity_sensitive_cost`）统一保留 **6 位小数**，`ROUND_HALF_EVEN`；
  附 `rounding` 说明：单项舍入误差 ≤ 5e-7，3 个加性分项累计 ≤ 1.5e-6。
- 计划优劣比较使用相对容差 `rel_tol = 1e-9`（加绝对下限 `1e-12`），
  不做裸浮点相等比较；容差内平局按访问路径键字典序（`idx:<名>` /
  `seq_scan`）打破，输入顺序不影响结果，计划选择可复现。

## 快照版本与错误码

- 快照文件含 `format`、`version`、`generated_at`（ISO-8601）；加载时全量
  校验（表/列/直方图形状、索引与相关性引用、取值域），任何失败抛
  `E_SNAPSHOT_INVALID` 且**保留旧快照**（先完整构建再原子替换，绝不混用）。
- 查询可指定 `snapshot_version`，与当前加载版本不符返回
  `E_SNAPSHOT_VERSION_MISMATCH`；缺表/列统计返回 `E_STATS_MISSING`；
  其他业务码：`E_QUERY_INVALID`、`E_BAD_JSON`、`E_SNAPSHOT_NOT_LOADED`、
  `E_NOT_FOUND`。错误响应统一为
  `{"ok": false, "error": {"code", "message", "details"}}`。

## HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `GET /v1/health` | 存活探测 |
| `GET /v1/snapshot` | 当前快照版本、时间戳、指纹、表与索引清单 |
| `POST /v1/snapshot/load` | `{"path": ...}` 校验并原子替换快照 |
| `POST /v1/plan` | `{"query": {...}, "snapshot_version": 可选}` 返回计划与代价分解 |

查询描述：`tables`（表名列表）、`predicates`（`eq`/`range`/`in`/`is_null`/
`is_not_null`/`and`）、`join_predicates`（等值连接，`left`/`right` 两端
`table`+`column`）。
