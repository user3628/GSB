# 优化、并发与恢复设计

## 1. 选择性估算

所有选择率以 `1e9` 定点表示，并强制落在 `(0, 1]`。内部下限为 `0.000001`，
避免定点比例无法表示极小值时变成 0。

| 谓词 | 基础选择率 |
|---|---|
| 等值 | `1 / max(ndv_left, ndv_right)` |
| 范围过滤 | `range_fraction_permille / 1000` |
| 范围连接 | `range_fraction_permille / 1000 / max(ndv_left, ndv_right)` |
| 自连接 | `1 / ndv_relation` |

`ndv = max(1, round(row_count * ndv_ratio))`。相关系数修正为：

```text
selectivity = clamp(round(base_selectivity * correlation), 0.000001, 1)
```

谓词显式声明系数时使用谓词系数；否则跨关系谓词取两端关系统计系数较小值，本地过滤
使用本关系系数。结果中返回基础选择率、修正系数、最终选择率及区间检查。

## 2. 左深搜索

关系按名称排序并映射为位图。每个状态表示已经连接的前缀集合；下一个关系必须在谓词
图中与当前前缀存在边，否则该候选分支被剪枝。搜索没有笛卡尔积分支。

默认关系数上限为 8，硬上限为 9（避免阶乘搜索失控）。超过上限立即返回：

```json
{
  "code": "E_SEARCH_SPACE_TOO_LARGE",
  "details": {
    "relation_count": 9,
    "max_relations": 8,
    "pruned_branches": 362880
  }
}
```

关系数没超限但谓词图不连通时，没有合法完整左深计划，返回
`E_NO_CONNECTED_PLAN` 并附已剪枝候选数。

每个子集应用“关系集合完全包含于该子集”的所有谓词。因此等值/范围连接谓词在两端
关系都进入前缀时只应用一次，自连接与范围过滤在关系首次扫描时应用。

## 3. 中间基数与代价

基数为：

```text
log2(cardinality) = sum(log2(base_rows)) + sum(log2(selectivity))
```

对数域累加后转换为 `1e6` 定点基数。零行关系保持 0；超过 `2^63-1` 行的中间结果
封顶并标记 `overflow_capped=true`。每个步骤输出 `upper_bound_rows`，保证有限。

代价三项：

```text
scan                = sum(base_row_count)
join                = sum(left_input_rows + right_input_rows)
intermediate_write  = sum(joined_rows)
```

每个分项均为 `1e6` 定点整数微单位，输出 6 位小数。基数在转换时半偶舍入；
分项加总不再经过浮点。

## 4. 容差与平局

候选总代价为定点整数。令最小代价为 `minimum`，容差为 `tolerance_ppm`：

```text
threshold = minimum + floor(minimum * tolerance_ppm / 1e6)
```

所有 `cost <= threshold` 的候选视为等价，按关系名序列的稳定字典序选最小者。
响应同时展示绝对最小计划、选中计划、等价候选数以及是否由容差裁决胜出。

## 5. 快照与请求状态机

SQLite 表：

- `snapshot_versions`：不可变的完整历史快照，保留给已绑定构建。
- `current_snapshot`：单行当前版本指针。
- `import_reports`：接受、降级、拒绝报告。
- `intents`：统计提交与计划构建意图。
- `plan_requests`：请求哈希、状态和完整结果。
- `recovery_reports`：每次恢复扫描的结果。

统计提交顺序：

1. 解码与格式/数值校验，全部在内存完成。
2. 写 `snapshot_commit` 意图并提交事务。
3. 单事务插入完整快照、更新当前指针、写导入报告、完成意图。
4. 任一步失败事务回滚，旧指针仍然有效。

计划构建顺序：

1. 读取请求指定版本的完整快照对象。
2. 单事务插入 `RUNNING` 请求与 `plan_build` 意图。
3. 在该不可变快照对象上串行构建，不使用“当前快照”再次取数。
4. 单事务发布结果并完成意图。
5. 若构建期间当前版本被替换，结果仍绑定旧版本，并标记
   `version_retired_during_build=true`；不混用新旧统计。

同一 `request_id` 的请求哈希相同则等待并复用持久化结果；哈希不同返回
`E_CONFLICTING_REQUEST`。

## 6. 并发模型

- 导入写者锁：串行化快照替换和意图写入。
- 构建锁：串行化计划提交，符合“按提交串行执行”。
- SQLite WAL：只读快照/计划查询可与一个写事务并发。
- 每个 HTTP 请求使用独立 SQLite 连接。
- 生产演示是单 HTTP 服务进程；不将同一数据库共享给多个写服务进程作为支持场景。
- 手动 `POST /v1/recovery` 会同时等待构建锁与写者锁，只在服务空闲时扫描意图。

## 7. 强制终止恢复

重启时 `Store.recover()` 在单个写事务中扫描 `PENDING/STARTED` 意图：

- `plan_build`：若计划已完整发布则标记已恢复；否则请求标为 `ABORTED`，错误码
  `E_REQUEST_ABORTED`。
- `snapshot_commit`：若对应版本和 digest 已原子落库则标记已恢复；否则意图中止，
  当前快照回退到最后一个完整版本。
- 额外检查当前指针是否指向存在且 digest 匹配的快照；不匹配则回退到最高完整版本。
- 报告包含恢复前后版本、扫描意图数、已恢复提交、已中止计划和差异列表。

## 8. 业务错误码

| 错误码 | 场景 |
|---|---|
| `E_FORMAT_CORRUPT` | 魔数、CRC、长度、记录数、摘要不一致 |
| `E_FORMAT_UNSUPPORTED` | 高版本、未知必填字段/标志 |
| `E_STAT_INVALID` | 负数/溢出/非法统计比例或版本 |
| `E_SEARCH_SPACE_TOO_LARGE` | 关系数超过搜索上限 |
| `E_VERSION_RETIRED` | 导入旧版本或引用不存在的快照版本 |
| `E_CONFLICTING_REQUEST` | 同一请求标识内容不一致 |
| `E_SNAPSHOT_CONFLICT` | 同版本不同语义摘要 |
| `E_NO_CONNECTED_PLAN` | 谓词图不连通，无法避免笛卡尔积 |
| `E_REQUEST_ABORTED` | 进程终止后恢复为中止，或请求仍在运行 |
| `E_INPUT_INVALID` | 请求 JSON、路径、谓词或参数非法 |
| `E_NOT_FOUND` | 请求或历史版本不存在 |
| `E_INTERNAL` | 本地内部错误 |

## 9. 确定性边界

同一 Python 版本、同一快照、同一输入和同一请求标识下，计划响应从 SQLite 直接复用
或重新构建均通过整数定点路径得到逐位相同结果。格式互操作只要求语义统计相同；
二进制浮点源在归一化时按声明精度舍入，报告会明确记录每个舍入位置与误差。

