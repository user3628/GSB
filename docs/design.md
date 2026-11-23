# 技术设计

## 1. 一致性目标

工作台把每一次业务变化表示为一个 SQLite 提交序号 `commits.id`。策略、批次、观测、风险裁决、冷却占用和意图状态都引用该序号。读取侧在单个事务内同时读取策略、批次和观测，因此快速视图与完整裁决不会跨越两个提交版本。

三个关键不变量：

1. 一次观测请求要么整体提交并增加一个批次修订，要么完全不落盘。
2. 回滚动作、批次进入 `ROLLING_BACK` 和服务冷却占用在同一事务提交。
3. 所有业务动作先创建 `intents` 行；最终状态与 `intents.status` 一起提交。进程在事务中途退出时 SQLite 原子回滚，启动扫描只会看到未完成意图，而不会看到半份观测或半次回滚。

SQLite 使用 WAL、`synchronous=FULL`、`busy_timeout=5000`，应用侧设置为单连接写队列，从而在本地高并发查看时避免写升级死锁。只读视图短事务执行，不参与写入。

## 2. 表与事实边界

| 表 | 作用 |
|---|---|
| `commits` | 全局追加式提交序号 |
| `policies` / `policies_current` | 不可变策略文档、摘要和当前版本指针 |
| `batches` | 批次、基线、状态、修订号和最后观测时间 |
| `observation_buckets` | 每个上报对应一个版本化时间桶贡献，含 `commit_seq` 与摄入时策略版本 |
| `risk_evaluations` | 每次持久化裁决的完整 JSON、策略版本、观测边界和提交序号 |
| `cooldowns` | 服务级冷却占用与触发批次 |
| `intents` | `PENDING → APPLIED/COMPLETED/FAILED/ABORTED` 的写入意图 |
| `recovery_runs` | 启动恢复报告快照 |

观测桶按 `(batch_id, seq)` 唯一。序号重复时比较规范化 SHA-256：相同内容幂等，不同内容返回 `E_CONFLICTING_REPORT` 且保留第一次内容。时间戳必须不小于该批次最后已提交时间戳；任何非法字段在事务前被校验，任何请求内回退都会整批拒绝。

## 3. 单一读快照

`GET /api/batches/{id}/risk?mode=compare` 在一个 SQLite 读事务中：

1. 读取当前策略版本和摘要。
2. 读取批次状态与观测修订。
3. 读取该批次全部已提交观测。
4. 读取当前全局提交序号。
5. 在同一内存数据上分别计算 `fast` 和 `full`。

响应中同时携带 `policy_version`、`policy_checksum`、`as_of_commit_seq` 和 `observation_revision`。前端不会把一次快速响应与另一次完整响应拼成结论；对照视图由后端同一快照生成。

## 4. 风险数值契约

所有指标先转为 PPM（百万分之一）：

- 错误率：`round_half_up(error_count × 1,000,000 / error_samples)`。
- 成功率：`round_half_up(success_count × 1,000,000 / success_samples)`。
- P90：升序排序后用最近秩 `rank = ceil(0.9 × n)`，取第 `rank` 个值。
- 延迟偏离：`round_half_up(max(0, p90 - baseline_p90) × 1,000,000 / baseline_p90)`。
- 成功率下降：`max(0, baseline_success_ppm - candidate_success_ppm)`。

分项归一化到 `[0, 1,000,000]` 微单位：

```text
component_micro = min(1,000,000, round_half_up(metric_ppm × 1,000,000 / threshold_ppm))
weighted_numerator = Σ(component_micro × weight_bp)
risk_bp = round_half_up(weighted_numerator / 1,000,000)
```

策略权重合计必须为 `10,000bp`。每一步响应都包含精确值、舍入值、方法、单位和余数；最终同时给出精确有理数误差和定点 `μbp` 累计舍入误差。评分只用整数，审计余数用有理数表示，不使用浮点参与判定。

样本量按错误率、延迟、成功率三条流分别检查。少于策略 `min_samples` 时返回 `E_INSUFFICIENT_SAMPLES`，不会生成零风险分。

示例：放行批次错误率为 0，P90 偏离为 `33,333ppm`，成功率下降为 0。延迟分项为 `111,111` 微单位，只有延迟权重生效：`111,111 × 3,500 / 1,000,000 = 388.8885bp`，半入后为 `389bp`。极高风险批次三个分项均被截顶到 1,000,000，加权后为 `10,000bp`。

## 5. 风险等级、裁决与状态机

四档边界由策略提供。边界容差使用相对 PPM：`tolerance_bp = round_half_up(boundary_bp × tolerance_ppm / 1,000,000)`。判断时按 `LOW → MEDIUM → HIGH → EXTREME` 固定顺序取第一个满足 `score ≤ boundary + tolerance` 的档位，因此容差内自动下档，平局稳定。

裁决规则：

- `score ≥ rollback_bp` 且开关开启：自动回滚。
- `score ≥ rollback_bp` 且开关关闭：升级人工。
- `score < pass_bp`：放行。
- 其它：继续观察。
- 快速模式存在未采样报告时，一律禁止放行，即使采样风险低于放行阈值。

批次状态迁移：

```text
OBSERVING → PASSED | ROLLING_BACK | ESCALATED
ROLLING_BACK → ROLLED_BACK | ESCALATED
PASSED / ROLLED_BACK / ESCALATED → 终态
```

进入 `ROLLING_BACK` 必须经过回滚裁决端点，以便同事务占用冷却期；普通状态迁移端点返回 `E_INVALID_TRANSITION` 并保持已持久化状态不变。

## 6. 快速采样

快速模式按 `fast_sample_limit` 对每个指标选择“最近报告”，排序键为 `(timestamp_ms DESC, seq DESC)`。响应输出：

- 采样方法 `latest_reports_per_metric`。
- 每指标上限、纳入/排除报告数、排除序号。
- 按样本数计算的采样率和置信说明。
- 全部快速/完整差异字段。

快速结论和完整结论都从同一观测集合切分，因此差异只来自采样，不来自不同策略或不同提交。

## 7. 意图与恢复

业务写入流程：

1. 独立事务创建 `intents(PENDING)` 并提交。
2. 开启业务事务，执行所有写入，同时把意图状态更新为 `COMPLETED` 或 `APPLIED`。
3. 若事务内异常退出，SQLite 回滚业务写入；意图仍为 `PENDING`。
4. 启动时把所有遗留 `PENDING` 标为 `ABORTED`。
5. 对已提交为 `APPLIED` 且批次仍为 `ROLLING_BACK` 的回滚，重试本地 HTTP 触达，成功则 `ROLLED_BACK`，失败则 `ESCALATED`。
6. 写入并暴露 `recovery_runs`，报告最新提交、批次状态、中止意图和恢复动作。

自动回滚的中间态是合法的：`ROLLING_BACK + 冷却已占用 + intent=APPLIED`。不存在 `ROLLING_BACK` 但未占冷却，或 `ROLLED_BACK` 但批次仍 `OBSERVING` 的可见状态。

## 8. 本地边界

- 工作台与独立 webhook 都拒绝非回环监听地址。
- 回滚目标默认为同进程 `/api/local-rollback-target`，也可显式指定另一个 `127.0.0.1` HTTP 端点。
- 无容器、无外部账号、无公共网络依赖。
