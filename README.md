# ChangeGuard 本地变更风险裁决台

面向本地微服务测试环境的变更风险分级、自动回滚与崩溃恢复工作台。项目从零实现，使用 FastAPI + SQLite（标准库 `sqlite3`）+ 原生 Web 控制台；不依赖 Docker、真实集群或外部服务。

核心目标不是“算一个分数”，而是让同一批次、同一策略版本、同一窗口版本下的 **快速裁决** 与 **完整裁决** 可逐项核对，并把风险结论、状态迁移、冷却期与审计事实放入明确的 SQLite 事务边界。

## 已交付能力

- 带版本、不可变内容摘要的 JSON 风险策略快照，版本必须严格递增。
- 变更批次、基线汇总、观察窗口、原始候选观测和状态迁移审计。
- 确定性最近秩 P90、定点风险分、逐步 ROUND_HALF_UP 账本与累计舍入误差。
- `OBSERVING / PASSED / ROLLING_BACK / ROLLED_BACK / ESCALATED` 五状态模型。
- 完整模式释放全部窗口观测；快速模式按上限确定性抽样，并输出抽样率、方法、bootstrap 置信区间和“需完整复核”判定。
- 自动回滚与按服务维度的冷却期；重复触发返回 `E_ROLLBACK_COOLDOWN` 和剩余时长。
- 同序号幂等上报、内容冲突拒绝、关闭窗口后的迟到样本拒绝，均不静默改写统计。
- 先持久化意图、再执行原子业务事务；硬终止后启动扫描 PENDING 意图并生成只基于数据库事实的恢复报告。
- 可交互控制台：批次选择、策略版本切换、FAST/FULL 对照、风险分解、舍入账本、窗口/轨迹/冷却/恢复报告。
- 固定样本、23 项自动化测试、自包含 HTTP 演示脚本和实际运行报告。

## 快速开始

### Windows（推荐 Python 3.11）

```powershell
cd <项目目录>
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
scripts\run_server.ps1
```

打开 <http://127.0.0.1:18080>，点击“装载固定样本”。

### Linux / macOS

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 18080
```

同样打开 <http://127.0.0.1:18080>。服务只监听 `127.0.0.1`。

### 一键固定样本演示

```bash
.venv/bin/python scripts/run_demo.py --port 18080
# Windows: .venv\Scripts\python.exe scripts\run_demo.py --port 18080
```

脚本会使用独立 SQLite 文件启动真实 `uvicorn` 子进程，通过 HTTP 完成：

1. 策略提交与 `E_STALE_POLICY` 验证；
2. `B-PASS` 完整放行；
3. `B-ROLLBACK` 自动回滚；
4. 冷却期重触发与剩余时长；
5. `B-COMPARE` 的 FAST/FULL 逐项对照；
6. 同一窗口切换策略 v1/v2 的结论变化；
7. 样本不足、幂等重复、冲突重复、关闭窗口迟到样本；
8. 回滚事务提交前 `os._exit(137)` 强制终止，重启后恢复扫描。

报告输出到：

- `artifacts/demo-report.json`：完整机读结果；
- `artifacts/demo-summary.md`：实际验证摘要；
- `artifacts/demo-runtime/server.log`：两次 uvicorn 进程日志。

## 测试

```bash
.venv/bin/python -m pytest -q
```

本次环境实际结果：`23 passed`。覆盖策略单调性/非法字段、幂等和冲突、迟到关闭规则、最近秩 P90、定点账本、FAST/FULL 版本对齐、FAST 只读、回滚与冷却原子性、并发回滚竞争、策略切换和恢复意图扫描。

## HTTP API

所有成功响应为：

```json
{"ok": true, "data": {}, "request_id": "req_..."}
```

所有业务失败统一为：

```json
{
  "ok": false,
  "error": {
    "code": "E_ROLLBACK_COOLDOWN",
    "message": "服务处于自动回滚冷却期，拒绝重复触发",
    "context": {"remaining_ms": 270000}
  },
  "request_id": "req_..."
}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/policies` | 提交策略快照 |
| `GET` | `/api/policies` | 列出策略版本 |
| `GET` | `/api/policies/latest` | 最新策略 |
| `POST` | `/api/batches` | 提交变更批次 |
| `GET` | `/api/batches` | 批次列表 |
| `GET` | `/api/batches/{id}` | 批次、窗口、轨迹、冷却、最近裁决 |
| `POST` | `/api/observations` | 批量摄入观测（body 含 `batch_id`） |
| `POST` | `/api/batches/{id}/observations` | 同上，批次来自路径 |
| `POST` | `/api/batches/{id}/windows/close` | 以 watermark 关闭窗口 |
| `GET` | `/api/batches/{id}/risk?mode=FULL\|FAST` | 计算风险与裁决；只读 |
| `GET` | `/api/batches/{id}/compare` | 同一读快照内对照 FAST/FULL |
| `POST` | `/api/batches/{id}/decision` | 提交裁决；FAST 不迁移状态 |
| `GET` | `/api/batches/{id}/decisions` | 最近裁决 |
| `GET` | `/api/batches/{id}/trajectory` | 状态轨迹 |
| `GET` | `/api/cooldowns/{service}?at_ms=` | 服务冷却状态/剩余时长 |
| `GET` | `/api/recovery/report` | 最新启动恢复报告 |
| `POST` | `/api/demo/seed` | 幂等装载固定样本 |
| `POST` | `/api/_demo/faults/arm` | 仅故障注入模式可用的硬终止注入 |

交互式 OpenAPI 文档：<http://127.0.0.1:18080/docs>。

### 示例

```bash
curl -s -X POST http://127.0.0.1:18080/api/demo/seed
curl -s http://127.0.0.1:18080/api/batches/B-COMPARE/compare
curl -s -X POST http://127.0.0.1:18080/api/batches/B-PASS/decision \
  -H 'Content-Type: application/json' \
  -d '{"mode":"FULL","decision_time_ms":1763900070000}'
```

## 风险算法契约

候选窗口统计：

- `error_rate = (n - successes) / n`
- `success_rate = successes / n`
- `P90 = sorted_latency[max(1, ceil(0.90 * n)) - 1]`，即最近秩法

分项压力：

```text
error_pressure   = min(1, max(0, candidate_error - baseline_error) / error_threshold)
latency_pressure = min(1, max(0, candidate_p90 / baseline_p90 - 1) / latency_threshold)
success_pressure = min(1, max(0, baseline_success - candidate_success) / success_drop_threshold)

risk = 100 * (0.40 * error_pressure + 0.35 * latency_pressure + 0.25 * success_pressure)
```

权重来自策略快照。比例与分项均以 `1,000,000` 为定点尺度，每步按 `ROUND_HALF_UP` 量化到 6 位小数；响应 `rounding.steps` 给出每步精确值、舍入值、定点整数、绝对误差、舍入规则，`rounding.cumulative_rounding_error*` 给出累计误差。样本数小于策略 `min_samples` 时返回 `E_INSUFFICIENT_SAMPLES`，不返回零风险。

风险等级：`CRITICAL >= 90`；`HIGH >= risk_threshold`；`MEDIUM >= risk_threshold/2`；其余 `LOW`。

## 时间窗口与迟到规则

- 窗口索引由 `floor((event_timestamp_ms - batch.started_at_ms) / window_ms)` 确定。
- 窗口在 committed watermark 满足 `end_ms + late_tolerance_ms <= watermark_ms` 时关闭。
- 关闭前且在容忍范围边界内到达的样本可被接受；关闭后的新样本返回 `E_LATE_SAMPLE`。
- 关闭窗口不会重新打开；完全相同的重复上报仍幂等，不改变窗口统计。
- 每个窗口有单调递增 `version`，批次有 `window_epoch`。风险结果、裁决、轨迹都记录两者与策略版本；状态更新带 `window_epoch + status` 条件守卫。

## FAST 与 FULL

- FULL：使用目标窗口全部观测，无抽样区间。
- FAST：按序号排序后做确定性等距抽样，最多 `fast.sample_cap` 条；输出总体数、样本数、抽样率和被选序号。
- FAST 额外用固定种子 bootstrap 计算风险区间；种子由策略摘要、批次、窗口版本、epoch 和样本序号确定。
- FAST 点估计越阈值、或置信区间跨越阈值、或置信区间上界与点估计间距超过 `fast.confidence_upper_bound` 时，结果为 `REQUIRES_FULL_REVIEW`。
- FAST 永远 `can_auto_rollback=false`；`POST decision` 的 FAST 请求只写审计记录，不迁移批次状态或策略绑定。

## 状态机与自动回滚

合法迁移：

```text
(new) -> OBSERVING
OBSERVING -> PASSED
OBSERVING -> ROLLING_BACK -> ROLLED_BACK
OBSERVING -> ESCALATED
```

自动回滚事务在同一个 `BEGIN IMMEDIATE ... COMMIT` 内写入裁决、两条轨迹、最终 `ROLLED_BACK` 状态和按服务冷却期。查询只能看到提交后状态，不能看到“已回滚但冷却未占用”。失败事务整体回滚，非法迁移返回 `E_INVALID_TRANSITION`。

## 意图与强制终止恢复

每个写操作先用独立事务提交一条 `PENDING` 意图，再开启业务事务；业务事务内完成所有领域写入并把同一意图改为 `COMMITTED`。若进程在业务事务中途被强制终止，业务事务不会提交，但先前 `PENDING` 意图仍可见。重启时扫描并把意图标为 `ABORTED`，记录当时持久化的批次/窗口/冷却状态和此前完整提交边界。

恢复报告明确声明：

- 事实来源是 SQLite 已提交快照与重启扫描；
- 不承诺强制终止前的进程内完整退出报告；
- 不把内存中未落盘的中间状态当作事实。

## 固定样本

| 批次 | 用途 |
|---|---|
| `B-PASS` | 完整风险 `2.33338`，完整裁决放行 `PASSED` |
| `B-ROLLBACK` | 完整/快速风险 `100`，自动回滚与冷却期 |
| `B-COMPARE` | 完整 `14` 放行；快速 `65.6664`，CI `[10.88892, 80.55554]`，需完整复核 |
| `B-INSUFFICIENT` | 3 条观测 < 10，返回 `E_INSUFFICIENT_SAMPLES` |
| `B-CRASH` | 回滚提交前硬终止与重启恢复 |

`B-COMPARE` 同一窗口：策略 v1 风险 `14`、阈值 `50`、动作 `RELEASE`；策略 v2 风险 `35`、阈值 `30`、动作 `AUTO_ROLLBACK`。这正是策略版本切换改变结论但窗口版本保持一致的演示。

## 本次实际验证

验证环境：Linux 回环环境，Python `3.14.4`（代码要求 `>=3.11`），SQLite 标准库，端口 `127.0.0.1:18080` 可用。Windows 使用 `scripts/setup.ps1`、`scripts/run_server.ps1` 和 `scripts/run_demo.bat`。

实际结果：

- `python -m pytest -q`：`23 passed`。
- 演示脚本返回码：`0`。
- 强制终止退出码：`137`；重启后 `B-CRASH` 状态仍为 `OBSERVING`。
- 恢复扫描：`pending_found=1`、`aborted_count=1`、`boundary_mismatch_count=0`。
- 冷却期重触发：`E_ROLLBACK_COOLDOWN`，`remaining_ms=270000`。
- 停止后无外部服务、容器或集群依赖。

详见 `artifacts/demo-summary.md`、`artifacts/demo-report.json` 和 `docs/DESIGN.md`。

## 目录

```text
app/                  FastAPI、SQLite 服务层、风险算法、静态控制台
samples/              固定策略、批次、观测和演示时间线
scripts/              样本构建、跨平台演示、Windows 安装/启动
tests/                23 项 pytest
artifacts/            实际演示报告与摘要
docs/DESIGN.md        模型、事务、恢复与一致性技术说明
```

## 边界说明

本工作台面向本地确定性演示，不包含身份认证、TLS、真实集群执行器或生产级限流。策略快照版本可切换，但同一批次的窗口几何（`window_ms`、`late_tolerance_ms`）在创建时固化；不兼容几何版本会返回 `E_POLICY_WINDOW_INCOMPATIBLE`。演示中的裁决时间使用固定毫秒值，便于重复核对；交互控制台默认使用当前时间。
