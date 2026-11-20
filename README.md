# 本地回环容量评估与压测编排工作台

这是一个面向 `127.0.0.1` / `localhost` 的确定性容量评估工作台。它按计划档位依次发起真实 HTTP 压测，采集固定种子生成的延迟样本，计算吞吐与最近秩分位延迟，做出通过/否决与升降档结论，并用 SQLite 的事务、意图记录和不可变结论保证中断恢复一致性。

项目不依赖 Docker、外部站点或真实账号。目标服务、样本分布和工作台都在本机回环端口运行。

## 快速开始（Windows）

要求 Python 3.11 或更高版本，并确保 `python` 或 `py` 命令可用。

```powershell
cd <项目目录>
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 推荐直接运行完整演示（会自己启动两个服务）
.\.venv\Scripts\python.exe .\scripts\demo.py
```

也可以使用附带的脚本：

```powershell
.\scripts\setup_windows.ps1
.\scripts\run_demo.ps1
```

手工启动服务：

```powershell
$env:CAPACITY_DB = "data\workbench.db"
$env:CAPACITY_WORKLOADS = "config\workloads.json"
.\.venv\Scripts\python.exe -m uvicorn capacity_workbench.test_service:app --host 127.0.0.1 --port 18091

# 另一个 PowerShell 窗口
$env:CAPACITY_DB = "data\workbench.db"
$env:CAPACITY_WORKLOADS = "config\workloads.json"
.\.venv\Scripts\python.exe -m uvicorn capacity_workbench.api:app --host 127.0.0.1 --port 18090
```

Linux/WSL 下只需把虚拟环境路径换成 `.venv/bin/python`。本次实际验证在 WSL2、Python 3.14.4 上完成；代码使用的语言特性下限为 Python 3.11。

## 验证命令

```bash
python -m pytest
python scripts/demo.py
```

当前结果：`37 passed`。演示脚本会生成：

- `artifacts/demo-output.json`：完整结论、档位指标、恢复报告与前后状态。
- `artifacts/demo-workbench.db`：本次演示 SQLite 数据库。
- `artifacts/demo-target.log`、`artifacts/demo-workbench.log`：服务日志。

## 本次实测输出

以下来自 `python scripts/demo.py` 的实际运行。固定种子和样本配置下，延迟、错误率与判定可重放。吞吐按“固定样本预算 / 计划档位时长”计算，实际墙钟采样窗口保存在 `observed_duration_ms`。

```text
=== 1. Complete sequence progression ===
stage attempt conc samples throughput_rps   p50_ms   p95_ms   p99_ms error_rate verdict triggers
    0       1    1     100         100.00    11.59    16.52    17.25     0.0000    PASS -
    1       1    2     200         200.00    11.67    18.12    22.27     0.0000    PASS -
    2       1    4     400         400.00    11.95    18.95    21.96     0.0000    PASS -
conclusion: SEQUENCE_EXHAUSTED, capacity=concurrency 4

=== 2. Mid-sequence veto and fallback ===
stage attempt conc samples throughput_rps   p50_ms   p95_ms   p99_ms error_rate verdict triggers
    0       1    2     200         200.00    14.23    23.66    31.47     0.0000    PASS -
    1       1    4     400         400.00    14.13    24.44    29.75     0.0025    PASS -
    2       1    8     800         800.00    36.61    65.92    84.36     0.0437  REJECT p99_ms
conclusion: KNEE_FOUND, rejected=concurrency 8, capacity=last passed concurrency 4

=== 3. Forced termination after WAL/intent commit ===
before kill:
  plan_status=RUNNING, round_status=SAMPLING, open_intents=1,
  persisted sample_count=0, aggregate_present=false, judgment_present=false

after restart:
  plan_status=READY, round_status=ABORTED, open_intents=0,
  persisted sample_count=0, aggregate_present=false, judgment_present=false

recovery report action: aborted_incomplete_round
recovery report limitation: No complete load-test exit report is fabricated after a forced termination.
```

完整 JSON 结构和决策依据见 `artifacts/demo-output.json`。

## 项目结构

```text
capacity_workbench/
  api.py             FastAPI 工作台 HTTP 接口与启动恢复
  models.py          Pydantic 计划模型、语义校验、记录类型
  state_machine.py   PENDING/SAMPLING/CONVERGED/ABORTED 状态机
  db.py              SQLite 模式、事务、意图、不可变触发器、恢复
  sampling.py        真实回环 HTTP 采样、固定样本、聚合与阈值判定
  service.py         档位推进、幂等、升降档与拐点结论
  workloads.py       固定种子 PRNG 与多种延迟/错误分布
  test_service.py    本地确定性测试服务
  randomness.py      SplitMix64 与样本 nonce 派生
  percentiles.py     最近秩分位算法与样本量校验
config/
  workloads.json             lognormal/normal/uniform 分布与错误、超时注入参数
  sample_plan_complete.json  完整序列计划
  sample_plan_fallback.json  中途否决回退计划
  sample_plan_recovery.json  强制终止恢复计划
scripts/
  demo.py                    端到端演示
  setup_windows.ps1          Windows 安装脚本
  run_demo.ps1               Windows 演示入口
tests/                       37 个自动化测试
```

## 计划与校验

计划核心字段：

```json
{
  "target_url": "http://127.0.0.1:18091/sample",
  "stages": [
    {"concurrency": 1, "duration_seconds": 1},
    {"concurrency": 2, "duration_seconds": 1}
  ],
  "thresholds": {
    "p99_ms": 100.0,
    "error_rate": 0.05,
    "min_throughput_rps": 1.0
  },
  "decision_rule": {
    "on_reject": "fallback",
    "max_retries": 1,
    "step_down_factor": 0.5
  },
  "seed": 12345,
  "profile": "stable",
  "request_timeout_ms": 200,
  "sampling": {
    "requests_per_second_per_worker": 100,
    "min_samples_for_percentiles": 100
  }
}
```

提交时执行以下校验，失败统一返回 `E_PLAN_INVALID`，且不会写入计划或档位表：

- 档位非空，并发数为严格递增的正整数。
- 每档持续时长为正整数。
- `p99_ms > 0`、`0 <= error_rate <= 1`、`min_throughput_rps >= 0`。
- 超时、采样率等参数为正数。
- 目标必须是带显式端口的 HTTP(S) 回环地址，拒绝外部主机和 URL 凭据。

## 状态机与不可变性

```text
PENDING --start--> SAMPLING --commit result--> CONVERGED
   |                    |
   +------abort---------+----------------------> ABORTED

CONVERGED: terminal, no outgoing transition
ABORTED:   terminal, no outgoing transition
```

`Store.transition_round()` 在事务内先校验再写入。非法迁移返回 `E_INVALID_TRANSITION`，事务回滚，不写状态、结束时间或迁移审计记录。

另外由 SQLite 触发器保证：

- `CONVERGED`/`ABORTED` 轮次不能再更新。
- 判定结论 `judgments` 不可更新或删除。
- 样本聚合 `aggregates` 不可更新。
- `COMPLETED` 计划的结论不可覆盖。

已收敛档位再次触发时直接复用原轮次；不会重新采样，也不会重算判定。

## 采样与判定

每个档位生成固定样本预算：

```text
每 worker 样本数 = duration_seconds * requests_per_second_per_worker
总样本数 = 每 worker 样本数 * concurrency
```

请求被均匀铺排到计划持续时间内，`asyncio.Semaphore(concurrency)` 与 HTTP 连接池共同限制在途请求数。每个请求都有独立的 `httpx` 超时；请求显式带 `Connection: close`，超时时传输层会关闭对应连接。测试服务通过 `request.is_disconnected()` 统计客户端断连，自动化测试验证超时后断连确实发生并计入错误。

延迟样本由 `SplitMix64` 和样本 nonce 确定生成。nonce 是计划种子、档位索引、attempt、请求序号的确定性混合，因此相同配置可重放同样延迟。测试服务支持：

- `lognormal`、`normal`、`uniform` 三种分布。
- 固定错误率注入（HTTP 503）。
- 固定超时率注入（服务端延迟响应，客户端超时后关闭连接）。
- 随并发数变化的延迟/错误放大，用于产生容量拐点。

分位计算采用确定性最近秩法：

```text
rank = ceil(percentile * sample_size)
value = sorted_latencies[rank - 1]
```

P99 至少要求 100 个样本；不足时返回 `E_INSUFFICIENT_SAMPLES`，轮次转为 `ABORTED`，不会写入聚合或判定。判定结果同时记录 `sample_size`、`percentile_method=nearest_rank` 以及每个触发阈值的名称、上限、实测值和比较符号。

## 升降档与拐点

`decision_rule.on_reject` 支持：

- `stop`：停止序列，记录失败档与最后一个通过档。
- `fallback`：回退到上一通过档的并发作为容量结论。
- `step_down_retry`：在失败并发与上一通过并发之间选择一个更低并发重测。

降档重测轮次使用同一逻辑档位，但具有不同的：

- `attempt`（如 `2`）
- `round_key`（如 `stage-1-r2`）
- `origin=step_down_retry`
- `retry_of_round_id`

重测通过时停止原序列，并把重测通过的并发作为容量点；重测仍失败则继续按 `step_down_factor` 降档，直至达到重试上限，再回退到最后一个通过档。

最终结论二选一：

- `SEQUENCE_EXHAUSTED`：所有计划档位通过，记录序列末端容量上界。
- `KNEE_FOUND`：记录拐点档、被否决并发、触发阈值证据，以及最后通过档或降档重测容量点。

## 并发一致性与幂等

- SQLite 部分唯一索引 `one_sampling_round_per_plan` 保证每个计划最多一个 `SAMPLING` 轮次，跨进程同样成立。
- 相同 `plan_id` 启动另一个轮次返回 `E_ROUND_ACTIVE`。
- 同档位重复触发由进程内 per-stage 锁串行化；后到请求等待并复用已提交轮次。
- 只读接口在一个读事务中查询轮次、聚合与判定；只有 `CONVERGED` 且聚合、判定都已存在时才返回性能数据，不会暴露半写状态。
- 采样样本、聚合、判定与 `CONVERGED` 状态在同一个 SQLite 事务中提交。

## 持久化、意图与恢复

所有表位于 SQLite，使用 WAL、外键和 `synchronous=FULL`。轮次进入采样前，先在事务中写入 `ROUND_EXECUTION` 意图，再提交 `PENDING -> SAMPLING`；结果提交时同时关闭意图。进程若在采样中被强制终止，会留下：

- 已提交的 `SAMPLING` 轮次；
- `OPEN` 执行意图；
- 可能已经提交的部分事实（当前实现通常样本仍只在内存中）。

工作台启动时扫描 `SAMPLING` 轮次和 `OPEN` 意图：

- 若轮次已有完整聚合与判定，则只根据已持久化判定恢复为 `CONVERGED`，绝不重算。
- 否则标记为 `ABORTED`，关闭意图，把 `RUNNING` 计划重置为 `READY`。
- 即使进程在档位之间终止、已经没有活动轮次，也会把残留的 `RUNNING` 计划重置为 `READY`。
- 写入 `recovery_reports`，只包含轮次 ID、迁移前后状态、意图数量、已持久化样本数、聚合/判定是否存在等事实。
- 不承诺为被强制终止的运行补造完整退出报告。

恢复后将同一档位再次触发会创建新的 `attempt` 轮次，旧 `ABORTED` 轮次保留用于审计。

## HTTP 接口摘要

```text
POST /plans                                     创建并校验计划
GET  /plans                                     列出计划
GET  /plans/{plan_id}                           计划报告
GET  /plans/{plan_id}/rounds                    轮次与已提交结论
GET  /plans/{plan_id}/snapshot                  一致性的轮次事实快照
GET  /plans/{plan_id}/rounds/{stage_index}      某档最新轮次
POST /plans/{plan_id}/rounds/{stage_index}/execute  执行/复用某档
POST /plans/{plan_id}/run                       推进整个序列
GET  /rounds/{round_id}                         单轮详情
POST /admin/recover                             手动扫描未完成事实
GET  /recovery-reports                          恢复报告
GET  /health                                    健康信息
```

错误响应统一为：

```json
{"error": {"code": "E_ROUND_ACTIVE", "message": "...", "details": {}}}
```

主要错误码：`E_PLAN_INVALID`、`E_INVALID_TRANSITION`、`E_ROUND_ACTIVE`、`E_INSUFFICIENT_SAMPLES`、`E_STAGE_ORDER`、`E_NOT_FOUND`。

## 测试覆盖

自动化测试覆盖：

- 计划字段、递增档位、阈值、回环地址校验与非法提交不落库。
- 四种轮次状态及全部关键非法迁移的无副作用回滚。
- 意图先写、终态不可变、已收敛档位复用。
- 完整序列、拐点回退、降档重测标识与容量结论。
- 同档位并发触发只执行一次、不同档位冲突返回 `E_ROUND_ACTIVE`。
- 采样并发上限、超时断连、超时计错、样本不足。
- 多种分布与固定种子可重放。
- 强制终止事实上的 `SAMPLING/OPEN` 恢复、完整事实恢复及恢复报告。
