# 技术说明

## 1. 数据流

```text
本地 JSON / HTTP 内联 spec
        |
        v
Pydantic 结构校验
        |
        v
normalize_spec: 数量解析、单位检查、signed 64-bit 溢出检查、名称唯一性
        |
        +--- 失败: E_INVALID_QUANTITY / E_INVALID_SPEC，未写批次状态
        |
        v
SQLite begin intent（持久化原始规格与 intent 时间）
        |
        v
schedule_batch: 初始账本 -> 顺序过滤 -> 打分/tie-break -> 放置或拒绝 -> 约束快照
        |
        v
SQLite final conclusion（原子更新 intent -> completed）
```

HTTP 提交和进程启动恢复都复用同一个 `normalize_spec` 与 `schedule_batch`，避免“在线结果”和“恢复结果”出现两套调度语义。

## 2. 数量契约

内部类型只有两类：

- CPU：`int` 毫核，例如 `1.5` -> `1500`，`500m` -> `500`。
- 内存：`int` 字节，例如 `1Gi` -> `1073741824`，`1000M` -> `1000000000`。

解析使用 `Decimal`，不经过二进制浮点。CPU 少于 1 毫核的输入、未知单位和负数均返回 `E_INVALID_QUANTITY`。内存必须恰好落在整字节。所有数量与最终累计值都必须不超过 `2^63 - 1`。

## 3. 账本与拒绝快照

节点状态仅保存 `used_cpu` 和 `used_memory` 两个整数。初始 `assignedPods` 与成功放置的 Pod 都通过同一类扣减逻辑更新：

```text
remaining_before = allocatable - used
if request <= remaining_before:
    used = used + request
    record accepted
else:
    leave used unchanged
    record rejection, shortfall, remaining_before
```

节点结果同时输出：

- `initialLedger`：样本声明已分配 Pod 的逐个回放。
- `placementLedger`：本批次成功放置的逐个扣减。
- `preemptionLedger`：抢占后归还的资源。
- `invariant`：`allocatable - initialAccepted - scheduled + preemptionReclaimed = remaining` 的整数一致性校验。

初始已分配 Pod 若造成超配，不执行负余量扣减；节点标记 `accountingOverflow`，后续候选统一被 `E_NODE_ACCOUNTING_OVERFLOW` 安全拒绝，并保留首个溢出快照。

## 4. 过滤语义

### 节点选择器

`nodeSelector` 是标签等值 AND 条件。任一键值不匹配即返回 `E_UNSATISFIED_AFFINITY`，诊断中给出键、期望值和节点实际值。

### Required 节点亲和性

`nodeSelectorTerms` 之间是 OR，单个 term 中的 `matchExpressions` 是 AND。操作符语义：

- `In`：标签存在且值在集合中。
- `NotIn`：标签缺失，或值不在集合中。
- `Exists` / `DoesNotExist`：只检查键是否存在。
- `Gt` / `Lt`：标签值和期望值都按整数解析后比较；非整数标签不匹配。

节点选择器和 required affinity 是独立插件，因此二者必须同时通过。首个失败 term 的首个失败表达式会写入 `firstViolationExpression`。

### 污点与容忍

- `NoSchedule`：无匹配容忍则硬否决 `E_TAINT_NOT_TOLERATED`。
- `PreferNoSchedule`：只进入打分降权；每个未容忍污点扣 5 分。
- `NoExecute`：无匹配容忍立即否决；有 `tolerationSeconds` 时按逻辑时钟计算 `remaining = toleranceSeconds - max(0, now - timeAdded)`，`remaining <= 0` 视为窗口过期并硬否决。无限期容忍直接通过。

`effect` 为空的容忍匹配所有效果；`Exists` 容忍可省略 value，空 key 可匹配所有键。

### Pod 间亲和与反亲和

required affinity：候选节点必须与至少一个“已存在或本批次已放置”的匹配 Pod 共享同一拓扑值。

required anti-affinity：同拓扑中不得存在匹配 Pod。

拓扑键在候选节点或已放置 Pod 节点上缺失时不视为同拓扑。每放置一个 Pod，后面的 Pod 都重新基于最新世界状态进行过滤，因此结果天然包含顺序传播。

## 5. 打分与精度

内部评分单位是 `1e-6` 分，常量 `SCORE_SCALE = 1_000_000`。

| 分项 | 上限 |
| --- | ---: |
| 节点 preferred affinity | 35 |
| Pod preferred affinity | 20 |
| Pod preferred anti-affinity | 15 |
| 资源平衡度 `1 - abs(cpu_ratio - memory_ratio)` | 30 |
| 每个未容忍 PreferNoSchedule | -5 |

比例分项使用 `Fraction` 计算精确值，转换为 micro-point 时执行一次 half-up：

```text
exact_units = ratio * max_points * 1_000_000
rounded_units = floor(exact_units + 1/2)
residual = exact_units - rounded_units
```

总分是各分项整数单位的和，限制在 `0..100_000_000`，最终固定输出 6 位小数。每个分项输出 `roundingErrorPoints`，单次评分输出 `componentResidualTotalPoints` 与精确分数 `componentResidualTotalExact`。批次级 `scoreContract.roundingAudit` 再将所有候选评分的残差以 `Fraction` 精确汇总，最后才转换为展示用小数；全流程没有二进制浮点参与判定。

节点排序键为 `(-scoreUnits, nodeName)`，因此平分时仍完全确定。

## 6. 抢占

抢占默认关闭，可在样本 `config.preemptionEnabled` 中开启。开启后若 Pod 无直接可行节点：

1. 只考虑其他过滤均通过、失败代码仅属于资源不足或硬反亲和的节点。
2. 选择低优先级且 `preemptable: true` 的已分配 Pod。
3. 将受害者请求归还到节点余量，删除其标签对后续亲和性的影响。
4. 重新执行完整过滤与打分。
5. 只有重算后全部通过的方案才提交；否则不产生部分驱逐。

`protected-job` 这类 `preemptable: false` 的 Pod 永不进入受害者集合。抢占事件保存 victims、归还前后余量和重算说明。

## 7. SQLite 状态机与恢复

表结构：

- `batches`：规格、状态、intent/completed 时间、结果、错误、是否恢复。
- `recovery_runs`：每次启动或 API 扫描的持久化记录。
- `recovery_events`：开始恢复、按规格重算、标记中止等事实。

服务使用进程内 `RLock` 串行化“写 intent -> 计算 -> 写结论”以及恢复扫描。SQLite 事务与 `synchronous=FULL` 保证单行更新原子性。

进程被 `kill -9` 或 Windows 强制终止后：

- 已写 intent 未写结论的批次会出现在 `intent` 或 `recovering` 状态。
- 下次启动 `lifespan` 自动扫描，读取持久化规格并确定性重算。
- 重算成功写 `completed`，`recovered=true`；失败写 `aborted`。
- 报告明确只引用已持久化事实，不声称生成过优雅退出报告。

## 8. 测试矩阵

`tests/` 包含 48 项测试：

- `test_quantity.py`：合法量纲、非法单位、亚毫核、Decimal 精确性、溢出。
- `test_engine.py`：资源账本与不变量、拒绝快照、节点/ Pod 亲和性、全部操作符、NoExecute 时间窗、Prefer 降权、抢占、固定点评分逐位确定性。
- `test_api_persistence.py`：API 端到端、统一错误、数量错误零写入、重复批次、节点查询、启动恢复、恢复报告限制声明。

验证命令：

```bash
.venv/bin/python -m pytest
.venv/bin/python scripts/demo.py
```

## 9. 离线与安全边界

HTTP 默认绑定 `127.0.0.1`，样本名限制为 `[A-Za-z0-9_-]` 且只读取 `samples/*.json`。页面不加载外部字体、脚本、CDN 或图片。运行期没有网络访问、容器调用或集群凭据需求。
