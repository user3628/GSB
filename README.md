# 本地对象存储网关配额与限流控制面

这是一个从零构建、仅依赖本地确定性数据的 FastAPI + SQLite 应用。它维护带版本号的租户策略快照，对固定事件批次执行容量配额、分层令牌桶、三种计量模式、父子配额级联和租户状态机判定，并通过“意图先行”事务协议支持进程强制终止后的恢复。

不依赖 Docker、真实对象存储、外部账号或网络服务。所有 HTTP 测试和本地运行均使用回环地址。

## 快速开始

环境已按任务要求确认：Python 3.14（要求 3.11+）、FastAPI、uvicorn、NumPy、pytest、SQLite 可用。

```bash
python3 -m pip install -r requirements.txt
python3 scripts/check_port.py --host 127.0.0.1 --port 18081
python3 -m app.cli serve --host 127.0.0.1 --port 18081
```

`8000` 端口在本次环境中已被占用，因此默认本地端口使用 `18081`；`app.cli serve` 在启动 uvicorn 前会再次检查端口。数据库默认位于 `data/gateway.db`，首次启动会自动导入 `samples/policy_snapshot.json`。

常用本地命令：

```bash
python3 -m app.cli seed --database data/gateway.db --samples samples
python3 -m app.cli status --database data/gateway.db
python3 -m app.cli recover --database data/gateway.db
pytest -q
python3 scripts/acceptance_demo.py
```

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `data/gateway.db` | SQLite 文件；父目录自动创建 |
| `SAMPLES_DIR` | `samples` | 自动导入的样本目录 |
| `AUTO_SEED` | `1` | 数据库无策略时自动导入版本 1 |
| `AUTO_RECOVER` | `1` | 启动时扫描并恢复 `PENDING` 意图 |
| `THROTTLE_REJECTION_THRESHOLD` | `3` | 连续限流拒绝进入 `THROTTLED` 的阈值 |
| `ALLOW_CHAOS` | `0` | 仅本地故障演示使用；允许 HTTP 请求触发强制退出 |

`crash-batch` 与 `crash-rebuild` CLI 会在自身进程内启用 chaos 标志，不要求全局环境变量。

## 固定样本

- `samples/policy_snapshot.json`：版本 1，`root -> child -> grand` 与 `root -> sibling` 层级，另有独立租户 `isolated`。
- `samples/events_batch.json`：固定 7 条读写事件，覆盖 BYTES、REQUESTS、COMPOSITE、子配额拒绝、限流拒绝和父级容量拒绝。
- `samples/fault_injection.json`：六类异常的注入方式与固定校验次序。
- `samples/incremental_patch.json`：基于版本 1 的增量编辑片段。
- `samples/crash_batch.json`：意图提交后强制 `os._exit(91)` 的批次。
- `samples/incremental_patch.json` 也可被 `crash-rebuild` 使用，以在重建事务中途强制 `os._exit(92)`。

## 核心语义

### 定点单位与时间

- 所有字节、令牌、请求权重和速率都转换为 `1e-6` 整数单位。
- 输入超过 6 位小数时按 `ROUND_FLOOR` 截断。
- 对外固定输出 6 位小数，例如 `"20000.000000"`。
- 事件时间和补充时间都使用整数毫秒。输入时间若带亚毫秒部分，会在 API 入口使用 `ROUND_FLOOR` 截断为整数毫秒；令牌公式不会消费小数毫秒。
- 补充公式为：

```text
numerator = refill_remainder + elapsed_ms * rate_units_per_second
added     = floor(numerator / 1000)
remainder = numerator % 1000
tokens    = min(burst, tokens + added)
```

令牌补充不足一个微令牌的部分保留在 `refill_remainder`；桶达到突发上限时余数清零。`next_available_at_ms` 用整数上取整计算。
- 批量累计和重建聚合使用 Neumaier/Kahan 补偿累加器 `CompensatedSum`；底层事实本身是整数定点，因此不会产生二进制浮点漂移。

### 计量模式

计量模式决定扣减资源，读写标志仅作为事件事实持久化和回显；租户权重作为策略元数据持久化，实际准入令牌量使用事件 `request_weight`：

| 模式 | 容量扣减 | 请求令牌扣减 |
| --- | --- | --- |
| `BYTES` | `payload_bytes` | 不扣 |
| `REQUESTS` | 不扣 | `request_weight` |
| `COMPOSITE` | `payload_bytes` | `request_weight` |

复合模式先检查整条祖先链的容量，再检查整条祖先链的令牌，全部满足后才一次性扣减；任一失败都不会发生部分扣减。

### 父子级联

- 事件租户的容量与请求令牌会同时记入自身、父租户和全部祖先。
- 容量检查按“自身 -> 父 -> 祖先”顺序，父级不足返回 `E_PARENT_QUOTA_EXCEEDED`，上下文包含：
  - `ancestor_chain`
  - `insufficient_tenant_id`（第一个不足的祖先）
  - `resource`
  - `difference_bytes` / `difference_tokens`
- 令牌桶同样按整条祖先链扣减，因此父级请求速率是子孙合计速率。
- 子租户扣减、祖先聚合更新、事件明细和状态迁移都在同一个 SQLite `BEGIN IMMEDIATE` 事务中提交，不存在父级仍为旧值的中间可见状态。

### 状态机

```text
ACTIVE        -> THROTTLED | QUOTA_BLOCKED | SUSPENDED
THROTTLED     -> ACTIVE | QUOTA_BLOCKED | SUSPENDED
QUOTA_BLOCKED -> ACTIVE | THROTTLED | SUSPENDED
SUSPENDED     -> ACTIVE
```

- 连续限流拒绝达到阈值（默认 3，可用 `THROTTLE_REJECTION_THRESHOLD` 配置）后进入 `THROTTLED`。
- `THROTTLED` 在后续成功准入后回到 `ACTIVE`。
- 容量拒绝进入 `QUOTA_BLOCKED`；后续成功扣减容量或策略重建使聚合用量低于容量后回到 `ACTIVE`。
- 人工挂起从任何非 `SUSPENDED` 状态进入 `SUSPENDED`；重复挂起返回 `E_INVALID_TRANSITION`。
- 恢复必须显式调用恢复接口，且满足：祖先链无挂起、剩余容量大于 0、令牌数不少于 `min(1 token, burst)`；否则返回 `E_RECOVERY_CONDITION_NOT_MET`。
- 状态迁移与事件扣减在同一事务内生效。

### 增量编辑与重建

`PATCH /api/v1/policy` 接受：

```json
{
  "base_version": 1,
  "generated_at_ms": 1002600,
  "tenant_patches": [
    {"tenant_id": "child", "capacity_bytes": 50000}
  ],
  "upsert_tenants": [],
  "remove_tenant_ids": []
}
```

- `base_version` 不等于当前版本时返回 `E_STALE_BASE`，状态不变。
- 补丁只修改受影响字段；新增租户必须提交完整 `TenantSpec`。
- 变更后：
  - 根据直接用量事实重建所有受影响租户、祖先和子孙的聚合用量视图；
  - 重新计算状态；
  - 令牌参数变化的租户按新突发容量重建令牌状态；
  - 之前的仿真结果在同一事务中标记为 `STALE`，查询返回 `E_SIMULATION_STALE`。

仿真默认从策略生成时刻的 `POLICY` 基线开始，因此已执行的固定批次仍可原样回放；`"baseline": "CURRENT"` 则从当前持久化令牌/用量视图开始，并要求事件时间不早于持久化时钟。
- 注入 `faults.rebuild_failure=true` 会在事务中途失败并回滚到编辑前版本；`PENDING` 意图保留给恢复扫描。
- 强制终止测试使用 `crash-rebuild`，在重建事务已写入但未提交时退出；重启后 SQLite 回滚事务，恢复器将意图标为 `ABORTED` 并恢复旧策略/状态。

## 异常顺序与业务码

批量校验固定顺序：

1. 解析、空事件、负载荷/负权重、非法时间：`E_SPEC_INVALID`
2. 未知租户：`E_UNKNOWN_TENANT`
3. 批次内时钟回拨或低于持久化时钟：`E_CLOCK_REWIND`
4. 中途中断标记：`E_BATCH_ABORTED`
5. 容量：`E_QUOTA_EXCEEDED` 或 `E_PARENT_QUOTA_EXCEEDED`
6. 令牌：`E_RATE_LIMITED` 或 `E_PARENT_QUOTA_EXCEEDED`

策略快照提交顺序为：`E_SPEC_INVALID` -> `E_STALE_SNAPSHOT` -> `E_TENANT_CYCLE`。增量编辑顺序为：`E_SPEC_INVALID` -> `E_STALE_BASE` -> `E_UNKNOWN_TENANT` -> `E_TENANT_CYCLE`。

异常响应结构统一为：

```json
{
  "code": "E_PARENT_QUOTA_EXCEEDED",
  "message": "an ancestor capacity quota is insufficient",
  "context": {
    "ancestor_chain": ["sibling", "root"],
    "insufficient_tenant_id": "root",
    "difference_bytes": "5000.000000"
  }
}
```

## HTTP API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康、策略版本、持久化时钟、未完成意图数 |
| GET | `/api/v1/meta` | 定点规则、计量模式、状态机、固定校验顺序 |
| GET/POST | `/api/v1/policy` | 读取/提交完整策略快照 |
| PATCH | `/api/v1/policy` | 带基础版本的增量编辑与原子重建 |
| GET | `/api/v1/state` | 全部租户策略、用量、令牌与状态 |
| GET | `/api/v1/tenants/{id}` | 单租户视图 |
| PATCH | `/api/v1/tenants/{id}` | 单租户增量编辑 |
| POST | `/api/v1/tenants/{id}/suspend` | 人工挂起 |
| POST | `/api/v1/tenants/{id}/resume` | 条件恢复 |
| POST | `/api/v1/batches` | 执行固定事件批次 |
| GET | `/api/v1/batches/{id}` | 读取已提交批次结果 |
| POST | `/api/v1/admissions` | 单事件便捷准入 |
| POST | `/api/v1/simulations` | 确定性回放，不修改真实状态；默认 `POLICY` 基线，可选 `CURRENT` |
| GET | `/api/v1/simulations/{id}` | 读取仿真；旧策略结果返回 `E_SIMULATION_STALE` |
| POST | `/api/v1/recover` | 扫描并恢复 `PENDING` 意图 |
| GET | `/api/v1/recovery/report` | 最近一次恢复报告 |
| GET | `/api/v1/intents` | 查询意图日志 |
| GET | `/api/v1/deductions` | 查询扣减/祖先联动台账 |
| GET | `/api/v1/faults/scenarios` | 六类异常注入说明 |

示例：

```bash
curl -s http://127.0.0.1:18081/health
curl -s -X POST http://127.0.0.1:18081/api/v1/batches \
  -H 'content-type: application/json' \
  --data-binary @samples/events_batch.json
curl -s -X PATCH http://127.0.0.1:18081/api/v1/policy \
  -H 'content-type: application/json' \
  --data-binary @samples/incremental_patch.json
curl -s http://127.0.0.1:18081/api/v1/recovery/report
```

## 持久化与强制终止恢复

SQLite 表包括：

- `policy_snapshots`：完整版本、生成时间、SHA-256 校验摘要与规范化 JSON。
- `tenants`、`tenant_states`：当前租户定义、令牌、直接/聚合用量、拒绝计数与状态。
- `batches`、`events`、`usage_ledger`：批次结果、逐事件结果与逐账户（含祖先）扣减事实。
- `intents`：`PENDING/COMPLETE/ABORTED` 意图及事务前状态快照。
- `simulation_runs`：策略版本绑定的仿真结果。
- `recovery_reports`：只由已落盘事实生成的恢复报告。
- `clock_state`：持久化单调事件时钟。

写入协议：

1. 先计算批次/重建并提交一条 `PENDING` 意图，其中保存受影响租户的事务前状态。
2. 在单独事务中执行扣减、状态迁移、祖先聚合更新和策略重建，并在同一事务末尾将意图标记为 `COMPLETE`。
3. 若进程在步骤 2 中途被强制终止，状态事务由 SQLite 回滚，`PENDING` 意图仍存在。
4. 重启时（或下一次写操作前）扫描 `PENDING`：
   - 批次意图：删除任何未完成批次行，恢复意图前租户状态和时钟，并标记批次 `ABORTED`；
   - 策略意图：删除更高版本，恢复旧策略、租户、状态、时钟与仿真状态；
   - 意图统一标为 `ABORTED`，写入恢复报告。
5. 报告列出回滚批次、中止批次、恢复的旧策略版本和未完成意图清单。

实际强制终止点：

- `python3 -m app.cli crash-batch ...`：意图提交后、扣减事务前退出码 91。
- `python3 -m app.cli crash-rebuild ...`：重建事务中途退出码 92。

## 验证结果

```text
pytest -q
18 passed
```

`python3 scripts/acceptance_demo.py` 实际输出保存在 `artifacts/acceptance_report.json`，pytest 输出保存在 `artifacts/pytest.txt`，回环 HTTP 服务及重启实跑记录保存在 `artifacts/live_http_verification.json`。本次结果：

- 固定批次 7 条事件：4 接受、3 拒绝。
- 业务码覆盖：`E_SPEC_INVALID`、`E_UNKNOWN_TENANT`、`E_STALE_SNAPSHOT`、`E_TENANT_CYCLE`、`E_CLOCK_REWIND`、`E_BATCH_ABORTED`、`E_STALE_BASE`、`E_QUOTA_EXCEEDED`、`E_RATE_LIMITED`、`E_PARENT_QUOTA_EXCEEDED`、`E_SIMULATION_STALE`。
- 中断批次未污染任何已提交状态，恢复报告列出 `crash-recovery-batch-001`。
- 批次判定中途退出码 91；重建中途退出码 92；两次重启均清空 `PENDING` 意图并恢复最后完整版本。
- 增量编辑生成版本 2，旧仿真返回 `E_SIMULATION_STALE`。
- 本地 uvicorn 在 `127.0.0.1:18081` 完成一次真实 HTTP 批次提交；停止并使用同一 `data/gateway.db` 重启后，`last_event_ms=1002500`、各租户用量/令牌/状态保持一致。

快速查看：

```bash
python3 - <<'PY'
import json
r = json.load(open('artifacts/acceptance_report.json'))
print(json.dumps({
  'acceptance': r['acceptance'],
  'codes': r['observed_business_codes'],
  'batch_summary': r['fixed_batch']['summary'],
  'recovery': r['forced_termination']['recovery_report'],
  'rebuild_recovery': r['forced_termination_during_rebuild'],
}, indent=2, ensure_ascii=False))
PY
```
