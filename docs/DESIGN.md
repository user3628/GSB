# ChangeGuard 技术设计

## 1. 架构

```text
Browser / demo.py
       │ local HTTP (127.0.0.1)
       ▼
FastAPI routes ── unified error envelope / request id
       │
       ▼
WorkbenchService ── policy validation, risk engine, state machine
       │                    │
       │                    └── deterministic FAST sampling + bootstrap
       ▼
SQLite (WAL, foreign keys, BEGIN IMMEDIATE writes)
```

服务层不依赖 FastAPI，测试可以直接调用。风险算法也不依赖数据库，可独立测试。

## 2. 持久化模型

| 表 | 关键字段 | 作用 |
|---|---|---|
| `policies` | `version`, `digest`, `canonical_json` | 不可变策略快照 |
| `batches` | `service`, `change_type`, `baseline_json`, `status`, `policy_version`, `window_epoch` | 批次与当前业务版本 |
| `windows` | `window_index`, `version`, `state`, `sample_count`, `success_count`, `watermark_ms` | 带版本时间桶 |
| `observations` | `(batch_id, sequence)` 唯一、`latency_micros`, `success` | 原始候选样本 |
| `decisions` | `mode`, `policy_version`, `window_version`, `window_epoch`, `details_json` | 风险/裁决审计 |
| `batch_transitions` | from/to status, action, policy/window versions | 状态轨迹 |
| `cooldowns` | `service` 主键、`cooldown_until_ms` | 按服务占用冷却期 |
| `intents` | `PENDING/COMMITTED/ABORTED`, payload, recovery run | 崩溃恢复边界 |
| `recovery_reports` | 扫描统计与 JSON 报告 | 只基于持久化事实 |

SQLite 连接逐次创建，启用 WAL、foreign keys、10 秒 busy timeout。读操作使用 deferred snapshot；写操作使用 `BEGIN IMMEDIATE` 串行化，避免“读后改”的竞态。

## 3. 策略

策略提交先完整规范化与校验，再计算 canonical JSON（排序键、紧凑分隔符）和 SHA-256 摘要。客户端可传 `digest` 做预校验；不匹配即 `E_POLICY_INVALID`。

版本检查在 `BEGIN IMMEDIATE` 内读取 `MAX(version)`。`submitted_version <= current_version` 时返回 `E_STALE_POLICY`，事务整体回滚，策略数量与最新摘要不变。

策略版本可以对既有批次重算，但有窗口几何兼容约束：`window_ms` 与 `late_tolerance_ms` 必须等于批次创建时固化的值。这样策略切换可改变阈值/权重/风险阈值/FAST 配置和裁决，但不会让历史窗口边界漂移。否则返回 `E_POLICY_WINDOW_INCOMPATIBLE`。

## 4. 观测与窗口

示例观测是一条请求样本：

```json
{"sequence": 1, "timestamp_ms": 1763900000000, "metric_type": "CANDIDATE", "latency_ms": 181, "success": true}
```

延迟先按 `ROUND_HALF_UP` 转为微秒整数；窗口统计由 SQLite 原始样本确定性地重建。

窗口规则：

1. `window_index = floor((timestamp_ms - started_at_ms) / window_ms)`。
2. 提交的 `watermark_ms` 与观测时间取最大值，得到 proposed watermark。
3. 若目标窗口已经关闭，返回 `E_LATE_SAMPLE`。
4. 若 `end_ms + late_tolerance_ms <= proposed_watermark`，目标窗口在此次请求中本应关闭，同样拒绝迟到样本。
5. 接受新样本后，将满足关闭条件的 OPEN 窗口置 CLOSED；关闭动作增加窗口 `version`。
6. 一次提交中任一冲突/迟到/非法样本导致整批事务回滚，不会产生部分摄入。

幂等规则：

- `(batch_id, sequence)` 唯一。
- 完全相同内容返回 `duplicate_count`，不增加窗口版本或批次 epoch。
- 同序号不同内容返回 `E_CONFLICTING_REPORT`，保留首次持久化内容。
- 重复检查先于关闭窗口检查，因此关闭后重放原样样本仍是幂等操作。

## 5. 风险算法

### 5.1 最近秩 P90

`rank = max(1, ceil(0.90 * n))`，排序后取 `rank - 1` 索引。响应同时给出方法、样本规模和秩，避免分位算法含糊。

### 5.2 分项与定点

```text
error_pressure   = min(1, max(0, candidate_error - baseline_error) / error_threshold)
latency_pressure = min(1, max(0, candidate_p90 / baseline_p90 - 1) / latency_threshold)
success_pressure = min(1, max(0, baseline_success - candidate_success) / success_threshold)

risk = 100 * Σ(weight_i * pressure_i)
```

所有比率、压力与分项 points 均量化到 6 位小数，使用 `ROUND_HALF_UP`。每步账本包含：

- 精确值（12 位小数）；
- 舍入值；
- 定点整数（乘 `1,000,000` 后四舍五入）；
- 绝对误差；
- 舍入规则。

累计误差是每步绝对量化误差之和，同时输出十进制和 micro 定点表示。最终风险分为各分项已量化值之和，再次量化为总分。

### 5.3 风险等级

```text
CRITICAL  score >= 90
HIGH      score >= risk_threshold
MEDIUM    score >= risk_threshold / 2
LOW       otherwise
```

## 6. FAST 与 FULL

FULL 使用目标窗口所有观测，置信区间退化为点值。

FAST：

- 按 `sequence` 排序；
- 若总体不超过 `sample_cap`，使用全体；
- 否则选择索引 `floor(i * n / cap)`，方法为 `systematic_even_by_sorted_sequence`；
- 输出总体数、样本数、抽样率、步长、被选序号；
- 用固定种子 bootstrap（默认 400 次）重算风险，取 nearest-rank 百分位作为置信区间；
- 种子由策略摘要、批次、窗口索引/版本、epoch 和样本序号确定。

快速复核条件：

- 点估计 `>= risk_threshold`；
- 或置信区间跨越 `risk_threshold`；
- 置信上界高于点估计的间距超过 `fast.confidence_upper_bound` 时显式标记 `confidence_bound_exceeded`。

FAST 的 `can_auto_rollback` 固定为 `false`。即使 POST 到 decision 接口，也只持久化 `applied=0` 的审计裁决；批次状态、策略绑定和窗口 epoch 不变。

`GET /compare` 在同一个只读 SQLite 快照中读取批次、策略、窗口和观测，再计算两次；响应输出 `same_policy_version`、`same_policy_digest`、`same_window_version`、`same_window_epoch` 等断言。

## 7. 状态机与裁决事务

```text
                 ┌───────────► PASSED
                 │
OBSERVING ───────┼───────────► ESCALATED
                 │
                 └─ ROLLING_BACK ─► ROLLED_BACK
```

自动回滚时，`ROLLING_BACK` 与 `ROLLED_BACK` 两条轨迹在同一事务中写入，批次最终状态一次提交为 `ROLLED_BACK`。因此不会暴露中间状态。

裁决写事务顺序：

1. 读取并校验批次窗口 epoch、目标窗口版本和策略。
2. 计算风险与动作。
3. 若需自动回滚，校验策略开关（动作已由风险决定）、服务冷却期和状态。
4. 插入 decision。
5. 插入 transition 轨迹。
6. 用 `WHERE batch_id=? AND window_epoch=? AND status='OBSERVING'` 条件更新批次。
7. 占用冷却期。
8. 将 intent 标记 `COMMITTED`。
9. 提交。

任一步失败都会回滚 3–8 的全部业务写入。冷却期检查先于状态检查，所以已回滚批次在冷却期内再次触发会稳定返回 `E_ROLLBACK_COOLDOWN`，且剩余时间基于请求的 `decision_time_ms` 计算。

## 8. 意图与恢复

每个写请求：

```text
独立事务：INSERT intent(status=PENDING, pre_state=...)
业务事务：BEGIN IMMEDIATE
              ...领域写入...
              UPDATE intent SET status=COMMITTED
          COMMIT
```

若进程在业务事务中途被 `os._exit` 或外部 kill：

- `PENDING` 意图已经持久化；
- 业务事务没有 COMMIT，由 SQLite 丢弃；
- 重启后扫描 PENDING，读取意图中的 `pre_state` 与当前持久化批次/冷却状态对比；
- 意图更新为 ABORTED，写入 recovery run id、错误上下文和报告；
- 报告输出 `boundary_match`、最后完整边界和当前观测状态。

恢复不尝试重放未提交业务，也不声称能生成强杀前的完整退出报告。演示通过环境变量 `RISKLAB_FAULT_INJECTION=1` 和显式 arm 接口，在全部回滚写操作入事务后、COMMIT 前执行 `os._exit(137)`，可重复验证真实进程级终止。

## 9. 错误码

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_POLICY_INVALID` | 422 | 策略字段/摘要非法 |
| `E_STALE_POLICY` | 409 | 版本不大于当前版本 |
| `E_POLICY_WINDOW_INCOMPATIBLE` | 409 | 策略窗口几何与批次不兼容 |
| `E_BATCH_INVALID` | 422 | 批次字段非法 |
| `E_BATCH_EXISTS` | 409 | 批次 ID 重复 |
| `E_BATCH_NOT_FOUND` | 404 | 批次不存在 |
| `E_CONFLICTING_REPORT` | 409 | 同序号内容冲突 |
| `E_LATE_SAMPLE` | 409 | 已关闭窗口的迟到样本 |
| `E_INSUFFICIENT_SAMPLES` | 422 | 样本不足 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移 |
| `E_ROLLBACK_COOLDOWN` | 409 | 冷却期内重复回滚 |
| `E_VALIDATION` | 422 | HTTP 结构校验失败 |
| `E_NOT_FOUND` | 404 | 路由或资源不存在 |
| `E_METHOD_NOT_ALLOWED` | 405 | HTTP 方法不允许 |

## 10. 固定样本与预期

样本配置：

- 窗口 `60,000 ms`，迟到容忍 `5,000 ms`，最小样本 `10`；
- 基线：40 样本、38 成功、P90 `180 ms`；
- FAST 上限 `12`、置信水平 `0.95`、上界间距配置 `6`；
- 策略 v1 风险阈值 `50`；v2 阈值 `30` 并收紧指标阈值。

实际结果：

| 批次/场景 | 实际风险 | 实际裁决 |
|---|---:|---|
| `B-PASS` FULL | `2.33338` | `RELEASE / PASSED` |
| `B-ROLLBACK` FULL/FAST | `100 / 100` | `AUTO_ROLLBACK / ROLLED_BACK` |
| `B-COMPARE` FULL | `14` | `RELEASE` |
| `B-COMPARE` FAST | `65.6664` | `REQUIRES_FULL_REVIEW`，CI `[10.88892, 80.55554]` |
| `B-COMPARE` 策略 v2 | `35`，阈值 `30` | `AUTO_ROLLBACK` |
| `B-INSUFFICIENT` | — | `E_INSUFFICIENT_SAMPLES`，3 < 10 |
| `B-CRASH` 强杀后 | `100` 可重算 | 状态保持 `OBSERVING`，PENDING→ABORTED |

## 11. 验证矩阵

| 要求 | 验证 |
|---|---|
| 策略单调/摘要 | `tests/test_policy.py`，演示 stale 场景 |
| 幂等/冲突/迟到 | `tests/test_ingest.py`，固定故障用例 |
| P90/定点误差 | `tests/test_risk.py` |
| FAST/FULL 同版本 | `compare` 断言 + `tests/test_risk.py` |
| FAST 不写状态 | 显式 v2 FAST 裁决前后批次策略/状态不变 |
| 回滚+冷却原子 | `tests/test_decisions.py`，并发双请求一胜一冷却 |
| 策略切换 | 同窗口 v1/v2 风险与动作变化 |
| 强杀恢复 | `scripts/run_demo.py` 真实退出码 137 + 重启报告 |
| HTTP 统一错误 | `tests/test_api.py` |

## 12. 设计边界

- 本地演示不实现认证、TLS、生产限流或多节点写入协调。
- SQLite 单文件是本地一致性边界；并发写由 `BEGIN IMMEDIATE` 串行化。
- 策略可切换，但已固化窗口几何不能漂移。
- FAST 结果是条件于抽中样本的统计预判，不能替代完整窗口复核，也永不触发自动回滚。
- 恢复报告不做未提交业务重放；目标是给出可审计的最后完整边界，而非伪造一次“完整退出”。
