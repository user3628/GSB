# 本地多服务集群容量与成本账本

这是一个不依赖容器、外部服务或云端附件的本地容量/成本权衡组件。它通过 FastAPI 暴露 HTTP 接口，以 SQLite WAL 保存带版本的容量账本，并用纯整数/显式定点完成成本、占比、预算裁决和加权成本指数计算。

核心交付包括：

- 版本化快照与增量片段校验、SHA-256 摘要和 `E_STALE_BASE` 原子拒绝；
- 每服务成本、总成本、成本占比、预算余量、加权成本指数；
- 69 个显式产物节点及依赖图，支持级联失效、局部重建和全量重建；
- 带相对容差的精确整数比较，不使用裸浮点预算裁决；
- 跨进程单写者、并发读、提交幂等和冲突检测；
- PREPARED 意图先行、单 SQLite 事务原子提交、强制终止后的恢复扫描；
- 固定样本、端到端 HTTP 演示、Pytest 测试和持久化事实恢复报告。

## Windows 快速开始

要求 Python 3.11 及以上。以下命令在 PowerShell 中执行：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
python -m pip --version
python -m pip install -r requirements.txt
python scripts\init_environment.py
python scripts\build_samples.py
python -m pytest -q
python scripts\run_demo.py
```

也可以直接运行一键验证脚本：

```powershell
.\scripts\verify.ps1
```

`requirements.txt` 固定为本工作区实际验证版本：

```text
fastapi==0.165.0
uvicorn==0.54.0
numpy==2.5.5
pytest==9.2.0
httpx==0.28.1
```

NumPy 按任务环境要求安装；金额和裁决链路刻意不使用 NumPy 浮点，避免不同计算路径产生判定翻转。`httpx` 是 FastAPI `TestClient` 的测试依赖，固定为当前环境版本。

默认本地路径由脚本创建：

```text
runtime/capacity_ledger.sqlite3
runtime/intents/intents.jsonl
runtime/capacity-ledger.writer.lock
```

启动 HTTP 服务：

```powershell
python -m uvicorn capacity_ledger.api:app --host 127.0.0.1 --port 8000
```

如需服务启动时自动扫描未完成意图，可在启动前设置
`$env:CAPACITY_RECOVER_ON_STARTUP = "1"`；演示脚本为了显式展示恢复过程使用 `0`。

## 业务模型

快照位于 `samples/snapshot_v1.json`，包含 8 个服务。每个服务记录：

- 副本数、每副本整数核数/内存 GB；
- 最小和最大核数/内存规格；
- 核时单价、GB 时单价，单位为整数分；
- 服务预算上限、最大成本占比（百万分位）和 SLO 目标（百万分位）。

快照根还包含单调版本号、生成时间、总预算、相对容差和整体校验摘要。摘要计算方式为：移除顶层 `checksum` 后，对规范 JSON（键排序、紧凑分隔符、UTF-8）做 SHA-256。

增量片段只包含受影响服务和完整的新服务记录，并携带 `base_version`；目标版本必须等于 `base_version + 1`。增量不能新增或删除服务。摘要不匹配、规格越界、字段缺失或未知字段都返回 `E_SNAPSHOT_INVALID`。

## 精确成本模型

全部展示与裁决来自整数：

```text
per_replica = cores_per_replica * price_core + memory_gb_per_replica * price_memory
service_cost = replicas * per_replica
total_cost = sum(service_cost)
share_ppm = round_half_up(service_cost * 1_000_000 / total_cost)
budget_remaining = budget - service_cost
weight_denominator = sum(slo_target_ppm)
weighted_numerator = sum(service_cost * slo_target_ppm)
weighted_index_micro_cents =
    round_half_up(weighted_numerator * 1_000_000 / weight_denominator)
```

舍入只用于展示值。预算和占比裁决始终使用交叉相乘后的精确整数比较：

```text
actual > limit * (1 + tolerance_ppm / 1_000_000)
```

实现将两边同时乘以正分母和 `1_000_000`，不产生浮点。恰好落在容差边界时不算违例；超过边界才算违例。多个违例按 `constraint` 的稳定字典序排序，`primary_constraint` 取排序后的第一项。

`metrics.rounding.steps` 逐项输出：

- 服务成本：`EXACT_INTEGER_MULTIPLY_ADD`，舍入单位为 0；
- 总成本：`EXACT_INTEGER_SUM`，舍入单位为 0；
- 占比：`ROUND_HALF_UP`、百万分位、每服务误差分子/分母及累计 `ppm` 误差；
- 加权指数：`ROUND_HALF_UP`、百万分位和精确误差分数；
- 预算裁决：`EXACT_CROSS_MULTIPLY_RELATIVE_TOLERANCE`，舍入单位为 0。

所有金额和比例同时提供整数原值和六位小数字符串，例如 `"1866.000000"`、`"0.234647"`。

## 依赖失效与重建范围

每个服务维护 8 个产物：规格、价格、预算策略、SLO 权重、成本、占比、预算余量和预算裁决。另有 5 个聚合产物：总成本、总预算余量、权重分母、加权指数和聚合预算裁决，共 69 个节点。

依赖图持久化在 SQLite 的 `artifact_catalog`，当前物化值在 `artifact_current`，每次重建审计在 `artifact_versions`，失效波次在 `invalidations`。每次提交只更新实际失效的产物，其余产物复用当前物化值。

选择逻辑只依据被改字段是否改变加权指数归一化基数：

- 价格、副本数、核/内存规格或服务预算字段变化：局部路径；重建受影响叶子及其下游，例如总成本、所有占比、预算裁决和加权指数分子。
- `slo_target_ppm` 变化或提交完整快照：全量路径；原因明确记录为 SLO 归一化分母变化或完整快照提交。

固定样本的 v2 单价增量重建 23 个产物、复用 46 个；v3 规格增量同样重建 23 个、复用 46 个。局部和全量路径的最终 `metrics` 与 `verdict` 使用规范 JSON 逐位比较，测试断言完全一致。

## 事务、并发与恢复

提交协议如下：

1. 在写者锁之外完成 JSON、摘要和模型校验；
2. 获取进程内 `RLock` 和跨进程文件锁；Windows 使用 `msvcrt.locking`，POSIX 使用 `fcntl.flock`；
3. 在锁内检查提交标识、内容摘要和基础版本；
4. 计算候选账本；预算违例只记录确定性拒绝结果，不触碰已提交账本；
5. 在独立事务中写入 PREPARED 意图，并追加、`flush`、`fsync` 意图 JSONL；
6. 用一个 `BEGIN IMMEDIATE` SQLite 事务写失效记录、产物重建、账本版本、提交结果，并把意图标记为 COMMITTED；
7. 提交后返回完整结果。

SQLite 使用 WAL、`synchronous=FULL`、`foreign_keys=ON` 和 `busy_timeout=30000`。所有成功写入在一个事务中提交，因此并发查询只能看到旧完整版本或新完整版本，不会看到部分产物。

提交幂等性按 `commit_id + 规范请求摘要` 处理：

- 同一标识、同一内容重试直接复用原结果；
- 同一标识、不同内容返回 `E_CONFLICTING_COMMIT`；
- 同一基础版本的不同并发增量由单写者串行化，只有第一个成功，其余返回 `E_STALE_BASE`；
- 预算拒绝结果可被同一提交标识重放，但不创建账本版本。

崩溃恢复只依赖已持久化事实。若进程在重建事务中被 `os._exit` 强制终止：

- 未提交的 SQLite 事务自动回滚；
- 已 fsync 的 PREPARED 意图仍在；
- 重启后的恢复扫描将它标记为 `ABORTED / E_PROCESS_TERMINATED`；
- 当前账本保持最后一个完整版本，所有摘要不变；
- 报告明确标记 `forced_termination_exit_report_available=false`，不承诺被终止进程能生成退出报告。

恢复前会验证快照摘要、逐版本指标重算、结果摘要、账本校验和链、提交记录、产物目录、物化产物和失效日志。任一项不一致均返回 `E_LEDGER_CORRUPT`。

## HTTP 接口

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/v1/snapshots` | 提交完整快照 |
| `POST` | `/v1/increments` | 提交增量片段 |
| `GET` | `/v1/ledger/current` | 查询当前完整版本及指标 |
| `GET` | `/v1/ledger/versions/{version}` | 查询指定完整版本 |
| `GET` | `/v1/dependencies` | 查询产物依赖图 |
| `GET` | `/v1/intents` | 查询意图状态 |
| `POST` | `/v1/admin/recover` | 扫描并协调未完成意图 |
| `GET` | `/v1/admin/verify` | 验证账本与产物完整性 |
| `GET` | `/health` | 健康检查 |

所有失败均返回同一 JSON 信封：

```json
{
  "ok": false,
  "error": {
    "code": "E_BUDGET_EXCEEDED",
    "message": "...",
    "details": {
      "primary_constraint": "service_budget:payments-service",
      "violations": [],
      "persisted_state_changed": false
    }
  }
}
```

业务错误码至少覆盖：

| 错误码 | HTTP | 含义 |
| --- | ---: | --- |
| `E_STALE_BASE` | 409 | 增量基础版本已过期，账本不变 |
| `E_SNAPSHOT_INVALID` | 422 | 摘要、字段或规格校验失败 |
| `E_BUDGET_EXCEEDED` | 409 | 总预算、服务预算或占比约束违例 |
| `E_CONFLICTING_COMMIT` | 409 | 相同提交标识对应不同内容 |
| `E_LEDGER_CORRUPT` | 500 | 持久化事实无法通过完整性验证 |

## 固定样本与演示

```text
samples/snapshot_v1.json
samples/increments/01_auth_price_increment.json
samples/increments/02_catalog_cores_increment.json
samples/increments/03_payments_budget_violation.json
samples/increments/04_cart_force_terminate.json
samples/increments/05_notifications_slo_full_rebuild.json
samples/expected.json
```

`scripts/run_demo.py` 会启动真实本地 Uvicorn 进程，通过 HTTP 完成：

1. 加载 v1 全量快照；
2. 提交仅改 auth 单价的 v2 增量并展示局部重建范围；
3. 重放同一 v2 提交，验证幂等结果复用；
4. 提交仅改 catalog 核数的 v3 增量；
5. 提交 payments 扩容 v4，得到预算、占比和总预算三个违例；
6. 子进程写入意图后在大约一半产物重建处强制退出，再通过 HTTP 恢复，验证 v3 摘要不变；
7. 提交 notifications SLO 变化 v4，展示强制全量重建；
8. 验证账本完整性并写出 `artifacts/demo_report.json`。

## 实际验证结果

验证时间：2026-11-21。实际执行环境为当前 WSL/Ubuntu 工作区、Python 3.14.4，未使用 Docker；Windows 兼容路径（`msvcrt` 文件锁、PowerShell 脚本、Windows 路径处理）已实现，但本次未在原生 Windows 进程上实跑。

测试命令：

```text
python -m pytest -q
```

结果：

```text
18 passed, 1 warning
```

唯一警告是当前 Starlette 对 `TestClient` 所依赖 `httpx` 的弃用提示，不影响测试结果或业务代码。

端到端演示结果：

```text
phase 1 full snapshot: version=1 total=1866.000000 cents/h verdict=PASS rebuilt=69
phase 2 auth price increment: version=2 mode=incremental total=1896.000000 rebuilt=23 reused=46
phase 3 idempotent retry: reused version=2 result
phase 4 catalog spec increment: version=3 total=1926.000000 cents/h mode=incremental
phase 5 budget violation: E_BUDGET_EXCEEDED primary=service_budget:payments-service persisted_state_changed=false
phase 6 forced termination recovery: aborted=1 version_before=3 version_after=3 state_unchanged=True
phase 7 SLO global rebuild: version=4 mode=full
final: version=4 total=1926.000000 cents/h weighted_index=242.068117 verdict=PASS integrity=VALID
```

预算违例实测：

```text
service_budget:payments-service     observed 512.000000  limit 320.000000
service_cost_share:payments-service observed 0.234647    limit 0.200000
total_budget                        observed 2182.000000 limit 2100.000000
```

版本账本校验和：

```text
v1 sha256:9162298c381069d403b42124fa4f991f28231abafa5c4f55546aa5acde5cce7a
v2 sha256:cbf62330ebeaeb526a7efaa604b3cb0b935ee7d1ecc285eaf515a117142cab71
v3 sha256:41fe3644779c266aee72ed6587750def5d1f2e3b60bd2247702a25c9e1f5fec7
v4 sha256:02b715d0a1ab6c24a9d142c2d5e328980f0d7b77b345ff46d3bbc2e037680f5b
```

强制终止前后的 v3 快照、指标和账本校验和完全相同；恢复报告 `state_unchanged=true`、`integrity=VALID`、`aborted_count=1`。完整机器可读报告位于 `artifacts/demo_report.json`。

## 目录结构

```text
capacity_ledger/
  api.py          FastAPI 路由和统一错误信封
  models.py       快照/增量校验与摘要
  domain.py       成本、定点指标和预算裁决
  artifacts.py    69 个产物、依赖图和失效传播
  storage.py      SQLite 模式、WAL、跨进程写锁
  service.py      提交协议、幂等、恢复和完整性验证
  crash_worker.py 测试用强制终止子进程
scripts/
  build_samples.py
  init_environment.py
  run_demo.py
  verify.ps1
samples/
tests/
```

本实现不使用 Docker 或其他容器，不访问外部服务；演示和测试只读取项目内固定 JSON 样本并在本地回环地址启动 HTTP 服务。

