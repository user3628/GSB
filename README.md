# Inference Scaling Arbiter

面向模型推理服务的本地弹性伸缩与并行放置裁决组件。系统读取带版本的策略快照、逻辑 GPU 节点和负载观测，执行确定性副本数推导、预算内放置、故障域降级、缩容回收、节点故障迁移，并以 SQLite WAL 保存完整版本化状态。

不依赖 GPU、Docker、容器运行时或外部集群。所有场景来自固定种子本地样本。

## 快速启动（Windows，离线）

前提：Windows 上已安装 Python 3.11+；`wheelhouse/` 中已放入 FastAPI、uvicorn、NumPy、httpx、pytest 的本地 wheel。

```powershell
# 1. 创建 .venv、仅从 wheelhouse 安装、检查 8000 端口、初始化 sample v1、跑测试
powershell -ExecutionPolicy Bypass -File scripts\setup_windows.ps1

# 2. 启动本地 HTTP 服务与交互控制台
powershell -ExecutionPolicy Bypass -File scripts\run_windows.ps1
```

打开 `http://127.0.0.1:8000`，OpenAPI 位于 `http://127.0.0.1:8000/docs`。

常用选项：

```powershell
# 清空数据库并重新装载 sample v1
powershell -ExecutionPolicy Bypass -File scripts\run_windows.ps1 -ResetData

# 允许仅用于崩溃恢复演示的显式 fault injection
powershell -ExecutionPolicy Bypass -File scripts\run_windows.ps1 -EnableFaultInjection

# 单元/集成测试 + 真实 HTTP + 强制终止恢复验收
powershell -ExecutionPolicy Bypass -File scripts\verify_windows.ps1
```

当前执行环境已确认 `127.0.0.1:8000` 可绑定：

```text
PORT_AVAILABLE 127.0.0.1:8000
```

## 交付结构

```text
app/
  main.py             FastAPI 路由、统一 JSON 错误封套、并发只读端点
  service.py          策略/轮次门面、幂等、状态迁移、恢复编排
  domain.py           策略/观测归一化与校验
  units.py            Gi/Mi/m 量纲、溢出、定点整数工具
  engine.py           定点目标副本推导
  scheduler.py        缩容回收、迁移、扩容、预算与故障域裁决
  state_machine.py    五状态合法迁移
  db.py               SQLite WAL、版本快照、意图、恢复扫描
  static/             离线交互控制台（无 CDN）
samples/              固定种子策略、节点、五组观测
scripts/              Windows 离线安装/启动/验收、样本生成、CLI 客户端
tests/                17 项自动化测试
artifacts/            实际验收报告
docs/ARCHITECTURE.md  技术设计与事务不变量
```

## 本地 HTTP 接口

所有失败均返回：

```json
{
  "error": {
    "code": "E_SPEC_INVALID",
    "message": "human readable reason",
    "details": {}
  }
}
```

核心端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/api/v1/health` | 版本、revision、未完成意图数量 |
| `POST` | `/api/v1/policy` | 提交完整策略快照（含服务、节点、运行预算） |
| `GET` | `/api/v1/policy/current` | 当前原始与归一化策略 |
| `GET` | `/api/v1/state?revision=N` | 读取一个完整提交版本 |
| `POST` | `/api/v1/rounds` | 提交负载观测并执行单写者伸缩轮次 |
| `GET` | `/api/v1/rounds/{round_id}` | 查询持久化轮次结果 |
| `POST` | `/api/v1/derive` | 只计算副本数推导，不修改状态 |
| `POST` | `/api/v1/services/{id}/transition` | 运维显式状态迁移校验 |
| `GET` | `/api/v1/recovery/report` | 基于 SQLite 事实的重启恢复报告 |
| `GET` | `/api/v1/samples` | 列出提交内确定性样本 |

### 提交策略

策略支持 `2Gi`、`512Mi`、`250m` 等量纲。版本必须严格大于当前版本；字段、负数、溢出或容量矛盾返回 `E_SPEC_INVALID`。提交失败不增加 revision。

```powershell
$policy = Get-Content samples\policy_v1.json -Raw
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/policy `
  -ContentType application/json -Body $policy
```

### 执行轮次

```powershell
$obs = Get-Content samples\observations_01.json -Raw | ConvertFrom-Json
$round = @{
  round_id = "round-001"
  policy_version = 1
  observations = $obs
} | ConvertTo-Json -Depth 20
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/rounds `
  -ContentType application/json -Body $round
```

也可使用标准库客户端查询：

```powershell
python scripts\demo_client.py state
python scripts\demo_client.py derive --observation observations_01.json
python scripts\demo_client.py round --round-id demo-001 --observation observations_01.json
```

直接查询示例（Windows 自带 `curl.exe`）：

```powershell
curl.exe http://127.0.0.1:8000/api/v1/health
curl.exe http://127.0.0.1:8000/api/v1/state
curl.exe -X POST http://127.0.0.1:8000/api/v1/rounds `
  -H "Content-Type: application/json" `
  --data-binary "@samples/round_request_01.json"
```

`samples/round_request_01.json` 是可直接 POST 的完整轮次请求样例。

### 错误码

| 业务码 | 触发场景 |
|---|---|
| `E_SPEC_INVALID` | 量纲、字段、负数/溢出、结构错误 |
| `E_STALE_SNAPSHOT` | 策略版本不大于当前版本；轮次引用旧策略 |
| `E_OBSERVATION_INVALID` | 观测缺失、未知服务、目标并发为零等无法推导 |
| `E_INVALID_TRANSITION` | 非法/同状态显式迁移，状态不变 |
| `E_SCALE_QUEUE_FULL` | 超出授予槽与有界等待队列，整轮不提交 |
| `E_INSUFFICIENT_CAPACITY` | 资源不足；失败任务附各节点当前余量 |
| `E_CONFLICTING_ROUND` | 同一 round_id 的内容哈希不同 |
| `E_ROUND_NOT_FOUND` | 轮次不存在 |

## 确定性样本

`scripts/generate_samples.py` 使用固定种子 `20261121` 与 NumPy 整数 RNG 生成样本：

| 样本 | 用途 |
|---|---|
| `policy_v1.json` | 3 服务、4 节点、3 故障域、25 ms/3 并行槽预算 |
| `policy_v2_budget.json` | 版本更新且保留既有放置；预算变为 2 并行槽 |
| `policy_invalid_unit.json` | 验证 `-2Gi` 的 `E_SPEC_INVALID` |
| `observations_01.json` | 6 副本需求、3 槽授予、3 个排队 |
| `observations_02.json` | 补齐上一轮排队扩容 |
| `observations_03.json` | 缩容回收 2 副本后扩容；chat 触发故障域降级 |
| `observations_04_failover.json` | `gpu-b1` 故障；首轮迁移 3 个、排队 1 个，下一轮完成 |
| `observations_05_queue_full.json` | 8 个排队超过上限 4，返回 `E_SCALE_QUEUE_FULL` |
| `round_request_01.json` | 可直接 POST 的完整轮次请求 |

## 核心规则

### 定点副本推导

全部使用整数，不依赖浮点：

```text
arrival_rate_milli_rps = floor(arrivals * 1_000_000 / window_ms)
concurrency_replicas  = ceil(arrival_rate_milli_rps / (target_concurrency * 1000))
effective_latency_ms  = max(p99_latency_ms, queue_duration_ms)
latency_ratio_milli   = floor(effective_latency_ms * 1000 / latency_budget_ms)
latency_adjusted      = ceil(concurrency_replicas * latency_ratio_milli / 1000)
current_floor         = current_replicas if latency_breached else 0
desired_raw           = max(concurrency_replicas, latency_adjusted, current_floor)
desired               = min(max(desired_raw, min_replicas), max_replicas)
```

延迟预算被突破时，当前副本数构成确定性下限，避免在已过载状态下继续缩容；预算恢复后允许按负载缩容。响应包含每个输入、所有中间整数、当前下限是否生效、夹取标记和可读推导串；同观测重复计算逐位一致（测试与验收均比较完整 JSON）。

### 单轮预算与并行放置

```text
available_waves = floor(round_time_budget_ms / placement_cost_ms)
grant_slots     = max_parallel_scales * available_waves
```

本轮先完成全部缩容回收，再按“服务优先级降序、观测到达顺序升序、同服务迁移优先于扩容”生成准入任务。每波最多 `max_parallel_scales` 个准入，单次准入推进 `placement_cost_ms`；结果动作带 `parallel_wave`。超出时间预算可容纳槽数的副本进入长度受限的等待队列；队列溢出则整轮返回 `E_SCALE_QUEUE_FULL`，不产生新 revision。

本地模拟使用确定性虚拟时钟：每次准入推进 `placement_cost_ms`，不使用不可复现的 wall-clock 等待。真实耗时另由日志/外部观测评估，不影响裁决结果。

### 原子资源与故障域

- 节点已用量 = 外部保留量 + 受管副本需求；显存与算力必须同时满足。
- 缩容先在规划工作区释放资源，同一 SQLite 事务内再接纳扩容，查询不会看到旧余量。
- 同服务优先放入未超过 `max_replicas_per_fault_domain` 的域；无法满足时保留已放置副本，放入剩余可行域并输出 `DEGRADED` 与冲突域明细。
- 不健康节点上的副本作为迁移任务进入同一优先级/预算序列；无目标容量时明确返回阻塞和节点余量，不静默跳过。

### 状态机

支持 `STABLE`、`SCALING_UP`、`SCALING_DOWN`、`DEGRADED`、`BLOCKED`。显式同状态迁移和未知状态返回 `E_INVALID_TRANSITION`。若扩容有进展但未达期望副本数，最终只能停在 `SCALING_UP` 或 `DEGRADED`，不得标为 `STABLE`。

### SQLite、意图与恢复

- 一个 revision 包含 `placements + node_states + service_states`，在同一事务写入。
- 轮次先独立提交 `RUNNING` 记录与 `PENDING` 意图，再开始状态事务。
- 正常提交时，新 revision 与轮次 `COMMITTED`、意图 `COMMITTED` 同事务完成。
- 强制终止可能发生在意图之后、状态事务之前；重启扫描 `RUNNING/PENDING`，将轮次标记 `ABORTED`，revision 保持最后一次完整版本。
- 恢复报告来源固定为 `sqlite_persisted_facts`，不会宣称强杀进程产生了完整退出报告。

## 实际验证结果

本工作区使用 Python 3.14.4（满足 3.11+）执行：

```text
$ pytest -q
17 passed
```

```text
$ python scripts/verify_acceptance.py --port 8000
PASS service_start
PASS policy_commit
PASS stale_snapshot
PASS invalid_unit
PASS fixed_point_derivation
PASS invalid_observation
PASS queue_full
PASS forced_termination          process_exit_code=42
PASS startup_recovery            ABORTED, latest_committed_revision=1
PASS bounded_parallel_placement  granted=3 queued=3
PASS round_idempotency
PASS fault_domain_degradation    chat=DEGRADED replicas=6
PASS unhealthy_node_migration    migrated 3 then 1
PASS invalid_transition
PASS legal_state_transition      DEGRADED -> SCALING_DOWN
PASS policy_version_update
PASS insufficient_capacity
PASS round_not_found
ACCEPTANCE_OK
```

完整机器可读结果：`artifacts/acceptance_report.json`。

## 运维边界

- 启动单个 uvicorn worker。进程内写锁负责单写者串行；SQLite WAL 负责多只读连接快照隔离。不要使用 `--workers > 1`。
- `current_replicas` 只在首次或新服务引导时用于生成初始放置；已提交放置是后续策略更新的权威事实。
- 新的策略节点快照会将节点健康状态重置为快照值；运行期健康变化通过轮次 `observations.node_health` 提交。
- Fault injection 默认关闭，仅用于验收；生产运行不要设置 `SCALE_ENABLE_FAULT_INJECTION=1`。
