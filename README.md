# PDB 节点驱逐裁决仿真引擎

本项目是一个本地、确定性的 Kubernetes 风格 PDB（PodDisruptionBudget）与节点腾空裁决引擎。它不连接真实集群，也不做通用装箱最优解；核心目标是让驱逐裁决逐条符合声明式 PDB 语义，并给出可复核的阻塞原因、放置建议、步骤持久化与强制终止恢复事实。

## 交付内容

- `pdb_engine/`：量纲/规格解析、PDB 语义、健康副本统计、单个 Pod 与整节点裁决、放置评分、SQLite 持久化、FastAPI 服务。
- `samples/scenario.json`：固定本地样本，含 3 个节点、2 个工作负载、3 份 PDB（其中两份分别使用 `minAvailable: "50%"` 与 `maxUnavailable: "50%"`）。
- `tests/`：25 个自动化测试，覆盖量纲、舍入方向、健康副本、选择器为空、单 Pod 多 PDB、整节点部分放行、放置、并发幂等与多节点共享 PDB 提交，以及强杀恢复。
- `scripts/demo.py`：通过 FastAPI TestClient 完成端到端演示，并启动子进程在真实提交一步后 `os._exit(97)`，再重启恢复。
- `scripts/setup_windows.ps1`：Windows 本地虚拟环境、离线/在线依赖安装与测试入口。
- `docs/VERIFICATION.md`：本次实际运行输出与验收事实。

## Windows 环境准备

要求 Python 3.11 或更高版本。

```powershell
cd <项目目录>
python --version
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

如需可复制的离线安装：

```powershell
# 在可联网的 Windows 机器上预先下载与目标 Python/OS 匹配的轮子
python -m pip download -r requirements.txt -d wheelhouse

# 在离线机器上
python -m pip install --no-index --find-links wheelhouse -r requirements.txt
```

也可直接运行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows.ps1
```

确认 SQLite 标准库可用并创建输出目录：

```powershell
python -c "import sqlite3; print(sqlite3.sqlite_version)"
New-Item -ItemType Directory -Force data, demo_output
```

默认数据库路径为 `data/pdb_engine.sqlite3`，可通过环境变量覆盖：

```powershell
$env:PDB_ENGINE_DB = "C:\work\pdb-engine\data\local.sqlite3"
```

## 运行

```powershell
# 自动化测试
python -m pytest

# 固定样本端到端演示（输出 demo_output/demo_result.json）
python scripts/demo.py

# 本地 HTTP 服务
python -m uvicorn pdb_engine.api:app --host 127.0.0.1 --port 8000
```

API 文档：`http://127.0.0.1:8000/docs`。

## HTTP API

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | 健康状态、数据库路径、可见性规则 |
| `POST` | `/api/v1/samples/load` | 加载本地 JSON 样本，body 可为 `{"sample_path":"samples/scenario.json"}` 或空对象 |
| `GET` | `/api/v1/scenario` | 当前样本、PDB 百分号换算、初始/当前可用数 |
| `POST` | `/api/v1/evictions/check` | 单个 Pod 驱逐裁决，body：`{"pod":"ledger-0","request_id":"..."}` |
| `POST` | `/api/v1/drains` | 整节点腾空裁决，body：`{"node":"worker-a","request_id":"..."}` |
| `GET` | `/api/v1/drains/{request_id}` | 已提交步骤与最终裁决结论 |
| `GET` | `/api/v1/drains` | 批次摘要 |
| `GET` | `/api/v1/recovery/report` | 最近一次强杀恢复报告 |

失败统一为：

```json
{
  "error": {
    "code": "E_PDB_BLOCKED",
    "message": "eviction is blocked by one or more PDB constraints",
    "details": {}
  }
}
```

业务码至少覆盖 `E_SPEC_INVALID`、`E_SELECTOR_EMPTY`、`E_PDB_BLOCKED`、`E_NO_FEASIBLE_NODE`、`E_DRAIN_NOT_FOUND`。请求体校验错误也统一映射为 `E_SPEC_INVALID`。

PowerShell 示例：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/samples/load `
  -ContentType application/json -Body '{"sample_path":"samples/scenario.json"}'

try {
  Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/evictions/check `
    -ContentType application/json -Body '{"pod":"ledger-0","request_id":"single-1"}'
} catch {
  $_.ErrorDetails.Message
}

Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/drains `
  -ContentType application/json -Body '{"node":"worker-a","request_id":"drain-1"}'
```

## 核心语义

### 量纲与规格

- CPU 解析为毫核整数：`2 -> 2000m`，`500m -> 500`，`0.25 -> 250`。
- 内存解析为字节整数：`512Mi`、`1Gi`、纯字节整数均支持。
- 所有结果必须精确落为整数；负数、非有限数、无法精确转换的小数、超过有符号 64 位上限的输入返回 `E_SPEC_INVALID`。
- PDB 必须且只能声明 `minAvailable` 或 `maxUnavailable` 之一，值可为整数或 `0%..100%` 字符串。
- PDB 使用 `controller.type` 与可选 `controller.name` 限定适用控制器；百分比的分母是适用控制器的 `desired_replicas` 总和。

`minAvailable` 百分比向上取整：

```text
required_available = ceil(desired_replicas * percent / 100)
```

`maxUnavailable` 百分比先向下取整为允许中断数，再转为健康下限：

```text
max_unavailable    = floor(desired_replicas * percent / 100)
required_available = desired_replicas - max_unavailable
```

整数预算不缩放。API 的 `budget` 输出显式包含 `kind`、`input`、`unit`、`rounding`、`calculated_integer`、`required_available` 与公式字符串。

### 匹配与健康副本

- 选择器支持普通标签映射，也兼容 `matchLabels` / `matchExpressions`（`In`、`NotIn`、`Exists`、`DoesNotExist`）。
- Pod 必须同时满足 PDB 选择器和适用控制器。
- 健康副本必须 `phase/status = Running`、未处于 `evicting/terminated`，且 `terminating != true`。
- PDB 匹配集合为空时，样本加载直接返回 `E_SELECTOR_EMPTY`，不会把零副本当作约束已满足。

### 单 Pod 与多 PDB

- 找出候选 Pod 命中的全部 PDB。
- 每个受影响 PDB 都计算 `after_available = current_available - 1`（仅健康 Pod 消耗健康预算）。
- 任一 PDB 满足 `after_available < required_available` 即返回 `E_PDB_BLOCKED`；阻塞项包含 PDB 名称、当前可用数、要求下限和缺口。
- 所有 PDB 都通过后才检查放置；无可行节点则返回 `E_NO_FEASIBLE_NODE`，不会先放行、事后才发现无处安置。

### 整节点腾空

- 先按输入顺序模拟朴素序列，并输出放行/阻塞清单。
- 再模拟四种确定性顺序：最少 PDB 隶属优先、稀缺预算优先、自适应最少约束优先；Pod 数不超过 20 时额外精确搜索最大 PDB 可行子集。所有顺序都使用完全相同的声明式约束检查。
- 选择放行数最高的序列；并强制保证候选集合包含朴素顺序，因此 `optimized_allowed >= naive_allowed`。平局按 Pod 名序列稳定决胜。
- 实际落库前，每个步骤都会基于当时已提交状态重新裁决；若其他节点并发消耗了预算，当前步骤会按真实原因降级为阻塞，绝不越过约束。

样本中：

```text
朴素顺序：web-a-shared -> web-a-alpha -> web-a-beta       放行 1
约束感知：web-a-alpha -> web-a-beta -> web-a-shared       放行 2
```

共享 Pod `web-a-shared` 同时命中两份 PDB，因此必须被阻塞，且两个原因都被列出。

### 放置评分

候选节点必须同时满足：

- 不是源节点，且未标记 `drain_requested`；
- CPU 与内存余量足够；
- 满足 Pod `node_selector`；
- 容忍节点 `NoSchedule` / `NoExecute` 污点（`PreferNoSchedule` 不硬阻塞）。

候选节点确定性评分：

```text
headroom = 1 - max(cpu_after, memory_after)
spread   = 1 / (1 + 目标节点上同 workload 副本数)
balance  = 1 - abs(cpu_after - memory_after)
score    = 50*headroom + 30*spread + 20*balance
```

同分按节点名升序。输出包含候选列表、评分组成、放置后利用率和剩余资源。

## 并发、一致性与幂等

- 同一节点只允许一个写者，按 `request_id` 的并发重复请求会等待，并复用同一份持久化结果。
- 不同节点可以同时分析；真正提交 PDB 扣减时使用短暂进程级提交门与 SQLite 条件更新，避免两个批次把同一 PDB 扣到下限以下。
- 每个读取使用 SQLite WAL 一致快照。
- 步骤先写 `STEP_INTENT` 并提交；放行步骤只有在同一事务内完成 `PDB current_available -= 1` 与 `status=COMMITTED` 后才可见。查询不会看到“已放行但 PDB 未扣减”的中间态。
- 运行中批次可暴露已完整提交的前缀步骤；最终 `allowed/blocked` 只在批次 `COMPLETED` 时发布。
- 当前实现以单个本地 API 进程（单个 uvicorn worker）为支持边界；SQLite 条件更新提供额外的跨进程 PDB 下限保护，但节点锁和等待复用是进程内机制。

## SQLite 持久化与恢复

主要表：

- `scenario_meta`、`nodes`、`workloads`、`pods`、`pdbs`：样本与 PDB 初始/当前可用数。
- `drains`：批次意图与状态 `INTENT -> RUNNING -> COMPLETED | ABORTED`。
- `drain_steps`：每步意图、结论、受影响 PDB、放置目标及前后可用数快照。
- `decisions`：单 Pod 与步骤级裁决结论。
- `recovery_events`：恢复报告。

启动时扫描 `INTENT/RUNNING` 批次：

1. 未提交的 `STEP_INTENT` 标为 `ABORTED`；
2. 批次标为 `ABORTED`；
3. 按 `initial_available - 所有已提交 ALLOWED 步骤` 重建每个 PDB 当前可用数；
4. 生成只包含持久化事实的报告，明确 `facts_scope=PERSISTED_ONLY` 与 `complete_exit_report=false`。

强制终止后不存在完整退出报告；报告只陈述最后一个完整步骤、已提交步骤数和恢复后的可用数。

## 实际验证

本次执行环境与完整命令输出见 `docs/VERIFICATION.md`。核心结果：

- `python -m pytest -q`：`25 passed`。
- `python scripts/demo.py`：`DEMO PASSED`，所有断言为 `true`。
- 单 Pod `ledger-0`：`E_PDB_BLOCKED`，`current_available=2`、`required_available=2`、`gap=1`。
- 整节点 `worker-a`：朴素放行 1，约束感知放行 2，实际放行 2、阻塞 1。
- 子进程在提交 `web-a-alpha` 后以退出码 97 终止；重启报告把批次标为 `ABORTED`，`web-a-budget` 恢复到 2，并明确没有完整退出报告。
